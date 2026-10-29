// Package packcache is the cache directory of narrenhaus: the packs of the
// references that were made local, a record per reference of the packs it
// needs, and the files of uploads and downloads on their way in.
//
// Under the directory given to Open:
//
//	lock                     held with flock by the process that has the directory open
//	origin                   JSON: the server's endpoint ID and the prefix
//	packs/<root hex>.data    the pack's objects, uncompressed, back to back
//	packs/<root hex>.idx     where each object lies, in jaccard-store's index encoding
//	packs/<root hex>.meta    JSON: complete or patch, a patch's parent, the two sizes
//	refs/<sha256 hex>.json   the record of the reference whose name has that SHA-256
//	staging/                 uploads, from their first byte until their push is over
//	tmp/                     downloads, and every file on its way into packs/ or refs/
//
// A pack is cached under the root key of the reference it was fetched or
// staged for. It is complete when it holds the whole key set of its root,
// and a patch when it holds only what its parent, a complete pack, lacks.
//
// A file is written under staging/ or tmp/ and renamed into place. A pack's
// .meta goes last and marks the pack valid. It carries the sizes of the
// other two files, so that a file the system cut short when it went down
// is not taken for a pack: Open removes packs without a .meta that fits,
// everything under tmp/ and staging/, and records whose pack is gone.
//
// Nothing of a pack is kept in memory but its sizes and its stamp. An
// object is read by finding its key in the index where it lies, a few
// entries at a time, and then reading its bytes from the data file by
// position. Files are opened when first read and at most maxOpen of them
// are open at a time, however many packs are held and uploads staged.
//
// What is read is not checked against its key. Nothing is synced to disk
// either, and Open takes a pack for whole by the sizes of its files: after
// the system went down, a pack may have the right sizes and other bytes. A
// caller that gives out what it reads holds each object to its key itself,
// as package refs does.
//
// The size of the cache is the sum of the .data and .idx files under
// packs/. Each use of a pack stamps it: the modification time of its data
// file is set to now. Whenever a pack is added or a hold is released, packs
// that nobody holds are deleted, the one stamped longest ago first, until
// the size is within the limit.
package packcache

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/packfile"
)

// Origin is what a cache directory belongs to: one server and one prefix.
type Origin struct{ Server, Prefix string }

var (
	// ErrOrigin is returned, wrapped, by Open for a directory that holds
	// the packs of another server or prefix.
	ErrOrigin = errors.New("packcache: directory belongs to another server or prefix")
	// ErrForeign is returned, wrapped, by Open for a directory that has no
	// origin and holds what no cache directory holds before it has one.
	ErrForeign = errors.New("packcache: directory is not a cache directory")
	// ErrInUse is returned, wrapped, by Open for a directory another Cache
	// has open, in this process or another.
	ErrInUse = errors.New("packcache: directory is in use by another process")
	// ErrAbsent is returned, wrapped, by Get for a key that is in none of
	// the files asked.
	ErrAbsent = errors.New("packcache: no such object")
	// ErrLimit is returned, wrapped, by Writer.Add for an object the file
	// does not take because of what a pack may hold: a key that is not
	// canonical, an object above amberpack.MaxPayload, and one object more
	// than packfile.MaxEntries. Unlike a failed write, no repeat cures it.
	ErrLimit = errors.New("packcache: beyond the limits of a pack")

	errClosed = errors.New("packcache: the cache is closed")
)

const (
	lockName    = "lock"
	originName  = "origin"
	packsName   = "packs"
	refsName    = "refs"
	stagingName = "staging"
	tmpName     = "tmp"
	probeName   = ".probe"
	newPrefix   = "new-" // of the files writeTemp makes under tmp/

	dataExt = ".data"
	idxExt  = ".idx"
	metaExt = ".meta"
)

// Cache is an open cache directory. It is safe for concurrent use.
type Cache struct {
	dir   string
	limit uint64
	lock  *os.File

	// mu guards the pack table, the holds, the sizes and the record
	// table. Files are renamed and deleted under it, never read.
	mu       sync.Mutex
	closed   bool
	packs    map[key.Key]*pack
	reserved map[key.Key]int // holds per root, whether a pack is cached under it or not
	// following are the slots of the reservations, by root: a pack placed
	// under a root becomes what they read.
	following map[key.Key]map[*slot]*Hold
	size      uint64    // of the packs in the table
	last      time.Time // the newest stamp given
	parked    uint64    // packs dropped while held, for the names of their files
	records   map[recordID]key.Key
	byRoot    map[key.Key]map[recordID]struct{}

	// openMu guards open, the files that are open, oldest first.
	openMu sync.Mutex
	open   []*flat
}

// Meta says what kind of pack is cached under a root.
type Meta struct {
	Complete bool
	Parent   key.Key // valid when !Complete
}

// pack is a cached pack. It is in the table until it is evicted or
// dropped, and lives on outside it while holds read it.
type pack struct {
	root key.Key
	meta Meta
	size uint64 // of its data and its index
	flat *flat

	// Guarded by Cache.mu.
	stamp time.Time
	holds int // holds that read it

	// parked is set, under Cache.mu, when the pack is dropped while holds
	// read it: its files wait under tmp/ for the last of them.
	parked atomic.Bool
}

// Open opens or creates the cache directory dir for origin and takes an
// exclusive lock on it, which Close gives up. It creates the
// subdirectories that are missing, removes what an earlier process left
// unfinished, and evicts down to limit, the size limit in bytes.
//
// It fails with ErrInUse for a directory that is open elsewhere, with
// ErrOrigin for one that belongs to another server or prefix, and with
// ErrForeign for one that is no cache directory and not empty either; all
// three leave the directory as it was. Any other failure names the path
// that could not be read or written.
func Open(dir string, limit uint64, origin Origin) (*Cache, error) {
	if dir == "" {
		return nil, errors.New("packcache: no cache directory given")
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	// Before anything is created in it, the lock included.
	if err := checkNew(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		dir:       dir,
		limit:     limit,
		lock:      lock,
		packs:     make(map[key.Key]*pack),
		reserved:  make(map[key.Key]int),
		following: make(map[key.Key]map[*slot]*Hold),
		records:   make(map[recordID]key.Key),
		byRoot:    make(map[key.Key]map[recordID]struct{}),
	}
	if err := c.load(origin); err != nil {
		lock.Close()
		return nil, err
	}
	return c, nil
}

// checkNew sees to it that a directory without an origin file is one a
// cache may be made in. Open removes everything under tmp/ and staging/,
// and the origin file is all that tells a cache directory from a home or
// a project that has directories of those names: without it, dir must
// hold nothing but what an Open leaves that ended before it wrote the
// origin. That is the lock, the four directories, and in them the probe
// of makeDir and, under tmp/, the file the origin is written to.
//
// An origin is there when checkOrigin will find one to read, so a link is
// followed here as it is there: a link of that name that leads nowhere is
// no origin, and one more name the directory may not hold.
func checkNew(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, originName)); err == nil {
		return nil
	}
	foreign := func(name string) error {
		return fmt.Errorf("%w: %s has no file %q and is not empty, it holds %q; a cache is made in an empty directory",
			ErrForeign, dir, originName, name)
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	for _, e := range entries {
		switch name := e.Name(); name {
		case lockName:
			if !e.Type().IsRegular() {
				return foreign(name)
			}
		case packsName, refsName, stagingName, tmpName:
			if !e.IsDir() {
				return foreign(name)
			}
			inside, err := os.ReadDir(filepath.Join(dir, name))
			if err != nil {
				return fmt.Errorf("packcache: %w", err)
			}
			for _, in := range inside {
				ours := in.Name() == probeName || (name == tmpName && strings.HasPrefix(in.Name(), newPrefix))
				if !ours || !in.Type().IsRegular() {
					return foreign(filepath.Join(name, in.Name()))
				}
			}
		default:
			return foreign(name)
		}
	}
	return nil
}

// lockDir takes the lock of dir without waiting for it. The lock belongs
// to the open file: closing it gives the lock up, as does the end of the
// process.
func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err != unix.EINTR {
			break
		}
	}
	switch {
	case err == nil:
		return f, nil
	case errors.Is(err, unix.EWOULDBLOCK):
		f.Close()
		return nil, fmt.Errorf("%w: %s", ErrInUse, dir)
	default:
		f.Close()
		return nil, fmt.Errorf("packcache: lock %s: %w", f.Name(), err)
	}
}

// load does the work of Open once the lock is held.
func (c *Cache) load(origin Origin) error {
	// The origin is checked before anything is created or removed: a
	// directory that belongs elsewhere is left exactly as it was. One
	// without an origin was found new by checkNew.
	known, err := c.checkOrigin(origin)
	if err != nil {
		return err
	}
	for _, name := range []string{packsName, refsName, stagingName, tmpName} {
		if err := c.makeDir(name); err != nil {
			return err
		}
	}
	for _, name := range []string{stagingName, tmpName} {
		if err := c.empty(name); err != nil {
			return err
		}
	}
	if !known {
		b, err := json.Marshal(originFile{Server: origin.Server, Prefix: origin.Prefix})
		if err != nil {
			return fmt.Errorf("packcache: %w", err)
		}
		if err := c.writeFile(filepath.Join(c.dir, originName), append(b, '\n')); err != nil {
			return err
		}
	}
	if err := c.loadPacks(); err != nil {
		return err
	}
	if err := c.loadRecords(); err != nil {
		return err
	}
	c.mu.Lock()
	c.evict()
	c.mu.Unlock()
	return nil
}

// originFile is the content of the origin file.
type originFile struct {
	Server string `json:"server"`
	Prefix string `json:"prefix"`
}

// checkOrigin compares the directory's origin with the one asked for.
// known is false when the directory has none yet.
func (c *Cache) checkOrigin(want Origin) (known bool, err error) {
	path := filepath.Join(c.dir, originName)
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("packcache: %w", err)
	}
	var have originFile
	if err := json.Unmarshal(b, &have); err != nil {
		return false, fmt.Errorf("packcache: %s does not say which server and prefix the directory belongs to: %v", path, err)
	}
	if have.Server != want.Server || have.Prefix != want.Prefix {
		return false, fmt.Errorf("%w: %s holds the packs of server %s with prefix %q, not of server %s with prefix %q",
			ErrOrigin, c.dir, have.Server, have.Prefix, want.Server, want.Prefix)
	}
	return true, nil
}

// makeDir creates the subdirectory name if it is missing and makes sure a
// file can be written in it, so that a directory that cannot be used is
// refused at start and not at the first upload.
func (c *Cache) makeDir(name string) error {
	dir := filepath.Join(c.dir, name)
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("packcache: %w", err)
	}
	probe := filepath.Join(dir, probeName)
	f, err := os.OpenFile(probe, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("packcache: %s cannot be written: %w", dir, err)
	}
	f.Close()
	if err := os.Remove(probe); err != nil {
		return fmt.Errorf("packcache: %s cannot be written: %w", dir, err)
	}
	return nil
}

// empty removes everything in the subdirectory name.
func (c *Cache) empty(name string) error {
	dir := filepath.Join(c.dir, name)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("packcache: %w", err)
		}
	}
	return nil
}

// writeFile writes b under tmp/ and renames it to path, so that path never
// holds half of it.
func (c *Cache) writeFile(path string, b []byte) error {
	tmp, err := c.writeTemp(b)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("packcache: %w", err)
	}
	return nil
}

// writeTemp writes b to a new file under tmp/ and returns its path.
func (c *Cache) writeTemp(b []byte) (string, error) {
	f, err := os.CreateTemp(c.TempDir(), newPrefix+"*")
	if err != nil {
		return "", fmt.Errorf("packcache: %w", err)
	}
	_, err = f.Write(b)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("packcache: %w", err)
	}
	return f.Name(), nil
}

// metaFile is the content of a pack's .meta.
type metaFile struct {
	Kind    string `json:"kind"`             // metaComplete or metaPatch
	Parent  string `json:"parent,omitempty"` // a patch's parent, its root in hex
	Objects uint64 `json:"objects"`          // entries in the index
	Bytes   uint64 `json:"bytes"`            // size of the data
}

const (
	metaComplete = "complete"
	metaPatch    = "patch"
)

func encodeMeta(m Meta, objects, bytes uint64) ([]byte, error) {
	f := metaFile{Kind: metaComplete, Objects: objects, Bytes: bytes}
	if !m.Complete {
		f.Kind, f.Parent = metaPatch, m.Parent.String()
	}
	b, err := json.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	return append(b, '\n'), nil
}

func decodeMeta(b []byte) (m Meta, objects, bytes uint64, err error) {
	var f metaFile
	if err := json.Unmarshal(b, &f); err != nil {
		return Meta{}, 0, 0, err
	}
	switch f.Kind {
	case metaComplete:
		m.Complete = true
	case metaPatch:
		if m.Parent, err = parseKey(f.Parent); err != nil {
			return Meta{}, 0, 0, err
		}
	default:
		return Meta{}, 0, 0, fmt.Errorf("unknown kind %q", f.Kind)
	}
	return m, f.Objects, f.Bytes, nil
}

// parseKey reads a key as Key.String writes one.
func parseKey(s string) (key.Key, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return key.Key{}, fmt.Errorf("%q is not a key: %w", s, err)
	}
	k, err := key.Parse(b)
	if err != nil {
		return key.Key{}, fmt.Errorf("%q is not a key: %w", s, err)
	}
	if k.String() != s {
		return key.Key{}, fmt.Errorf("%q is not a key in lower case", s)
	}
	return k, nil
}

func (c *Cache) packPath(root key.Key, ext string) string {
	return filepath.Join(c.dir, packsName, root.String()+ext)
}

// loadPacks fills the table from packs/ and removes the files of what is
// not a whole pack. Files that are not named as a pack's are left alone.
func (c *Cache) loadPacks() error {
	dir := filepath.Join(c.dir, packsName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	stems := make(map[string]struct{})
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		stem := strings.TrimSuffix(e.Name(), ext)
		if e.IsDir() || len(stem) != 2*key.Size || (ext != dataExt && ext != idxExt && ext != metaExt) {
			continue
		}
		stems[stem] = struct{}{}
	}
	for stem := range stems {
		p, err := c.loadPack(stem)
		if err != nil {
			// Not a pack: placed halfway, cut short, or not ours to read.
			for _, ext := range []string{metaExt, idxExt, dataExt} {
				if err := os.Remove(filepath.Join(dir, stem+ext)); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("packcache: %w", err)
				}
			}
			continue
		}
		c.packs[p.root] = p
		c.size += p.size
		if p.stamp.After(c.last) {
			c.last = p.stamp
		}
	}
	return nil
}

// loadPack returns the pack whose files are named stem, or an error if
// they are not a whole pack.
func (c *Cache) loadPack(stem string) (*pack, error) {
	root, err := parseKey(stem)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(c.packPath(root, metaExt))
	if err != nil {
		return nil, err
	}
	meta, objects, bytes, err := decodeMeta(b)
	if err != nil {
		return nil, err
	}
	data, err := os.Stat(c.packPath(root, dataExt))
	if err != nil {
		return nil, err
	}
	idx, err := os.Stat(c.packPath(root, idxExt))
	if err != nil {
		return nil, err
	}
	if !data.Mode().IsRegular() || !idx.Mode().IsRegular() ||
		uint64(data.Size()) != bytes || uint64(idx.Size()) != packfile.IndexSize(objects) {
		return nil, errors.New("the files are not of the sizes the pack was placed with")
	}
	return &pack{
		root:  root,
		meta:  meta,
		size:  bytes + packfile.IndexSize(objects),
		stamp: data.ModTime(),
		flat:  c.newFlat(c.packPath(root, dataExt), c.packPath(root, idxExt)),
	}, nil
}

// Close gives up the lock on the directory. Holds and sealed files that
// are still out can be read and let go of, but from here on the cache
// changes nothing in the directory, which may be another process's now:
// nothing is placed, recorded, dropped or evicted, and a file that is let
// go of, discarded or refused by Place stays where it lies. The next Open
// removes what is left under staging/ and tmp/.
func (c *Cache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.closeFiles()
	if err := c.lock.Close(); err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	return nil
}

func (c *Cache) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// TempDir returns <dir>/tmp, where files on their way into the cache are
// built. Open empties it.
func (c *Cache) TempDir() string { return filepath.Join(c.dir, tmpName) }

// Size returns the size of the cache: the sum of the .data and .idx files
// under packs/.
func (c *Cache) Size() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// Meta returns what is cached under root, and whether anything is.
func (c *Cache) Meta(root key.Key) (Meta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.packs[root]
	if p == nil {
		return Meta{}, false
	}
	return p.meta, true
}

// Place moves s into packs/ under root: as a complete pack when parent is
// nil, as a patch of *parent otherwise. The pack's .meta is written last.
// If a pack is already cached under root it is kept as it is, complete or
// patch, and s is discarded. Either way the pack is stamped, with its
// parent if it is a patch, and eviction runs: a pack placed over the limit
// is gone again at once unless its root is held (Reserve). A pack that is
// placed is read by the reservations of its root from then on.
//
// Place takes s whether it succeeds or not: s reads nothing afterwards and
// its Discard does nothing.
func (c *Cache) Place(s *Sealed, root key.Key, parent *key.Key) error {
	if s == nil || s.c != c {
		return errors.New("packcache: place of a file this cache did not seal")
	}
	if !s.done.CompareAndSwap(false, true) {
		return errors.New("packcache: place of a sealed file that was placed or discarded")
	}
	if c.isClosed() {
		s.flat.abandon()
		return errClosed
	}
	meta := Meta{Complete: parent == nil}
	if parent != nil {
		meta.Parent = *parent
	}
	b, err := encodeMeta(meta, s.objects, s.bytes)
	if err != nil {
		s.flat.remove()
		return err
	}
	tmpMeta, err := c.writeTemp(b)
	if err != nil {
		s.flat.remove()
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		s.flat.abandon()
		os.Remove(tmpMeta)
		return errClosed
	}
	p := c.packs[root]
	if p == nil {
		if err := s.flat.move(c.packPath(root, dataExt), c.packPath(root, idxExt)); err != nil {
			s.flat.remove()
			os.Remove(tmpMeta)
			return fmt.Errorf("packcache: %w", err)
		}
		if err := os.Rename(tmpMeta, c.packPath(root, metaExt)); err != nil {
			s.flat.remove()
			os.Remove(tmpMeta)
			return fmt.Errorf("packcache: %w", err)
		}
		p = &pack{root: root, meta: meta, size: s.bytes + packfile.IndexSize(s.objects), flat: s.flat}
		c.packs[root] = p
		c.size += p.size
		c.follow(p)
	} else {
		s.flat.remove()
		os.Remove(tmpMeta)
	}
	c.stamp(p)
	if !p.meta.Complete {
		if parent := c.packs[p.meta.Parent]; parent != nil {
			c.stamp(parent)
		}
	}
	c.evict()
	return nil
}
