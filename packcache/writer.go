package packcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/packfile"
)

// Writer appends objects, each key once, to a flat file under staging/ or
// tmp/. It keeps the table of what it wrote in memory until Seal writes it
// beside the data as the index. A Writer is not safe for concurrent use.
type Writer struct {
	c       *Cache
	f       *os.File
	buf     *bufio.Writer
	seen    map[key.Key]struct{}
	entries []packfile.Entry
	size    uint64
	err     error // the first failed write; nothing can follow it
	done    bool  // sealed or discarded
}

// NewStaged returns a writer for an upload, under staging/.
func (c *Cache) NewStaged() (*Writer, error) { return c.newWriter(stagingName) }

// NewDownload returns a writer for a download, under tmp/.
func (c *Cache) NewDownload() (*Writer, error) { return c.newWriter(tmpName) }

func (c *Cache) newWriter(sub string) (*Writer, error) {
	if c.isClosed() {
		return nil, errClosed
	}
	f, err := os.CreateTemp(filepath.Join(c.dir, sub), "*"+dataExt)
	if err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	return &Writer{
		c:    c,
		f:    f,
		buf:  bufio.NewWriterSize(f, 256<<10),
		seen: make(map[key.Key]struct{}),
	}, nil
}

// Add appends the object data under k. A key already added is skipped: its
// bytes are in the file once.
func (w *Writer) Add(k key.Key, data []byte) error {
	if w.done {
		return errors.New("packcache: add to a writer that was sealed or discarded")
	}
	if w.err != nil {
		return w.err
	}
	if _, ok := w.seen[k]; ok {
		return nil
	}
	if err := k.Validate(); err != nil {
		return fmt.Errorf("packcache: key %s is not canonical: %w", k, err)
	}
	if len(data) > amberpack.MaxPayload {
		return fmt.Errorf("packcache: object %s of %d bytes is above the limit of %d", k, len(data), amberpack.MaxPayload)
	}
	if len(w.entries) >= packfile.MaxEntries {
		return fmt.Errorf("packcache: more than %d objects in one file", packfile.MaxEntries)
	}
	if _, err := w.buf.Write(data); err != nil {
		w.err = fmt.Errorf("packcache: %w", err)
		return w.err
	}
	w.seen[k] = struct{}{}
	w.entries = append(w.entries, packfile.Entry{Key: k, Offset: w.size, Length: uint32(len(data))})
	w.size += uint64(len(data))
	return nil
}

// Discard ends writing and removes the file. After Seal, and when called
// again, it does nothing. A closed cache deletes nothing: the file is left
// for the next Open, which empties staging/ and tmp/.
func (w *Writer) Discard() {
	if w.done {
		return
	}
	w.done = true
	w.seen, w.entries = nil, nil
	w.f.Close()
	if w.c.isClosed() {
		return
	}
	os.Remove(w.f.Name())
	os.Remove(idxPathOf(w.f.Name()))
}

// Seal ends writing, writes the index beside the data and returns the
// file as a readable set of objects. A failed Seal leaves no file.
func (w *Writer) Seal() (*Sealed, error) {
	if w.done {
		return nil, errors.New("packcache: seal of a writer that was sealed or discarded")
	}
	s, err := w.seal()
	if err != nil {
		w.Discard()
		return nil, err
	}
	w.done = true
	w.seen, w.entries = nil, nil
	return s, nil
}

func (w *Writer) seal() (*Sealed, error) {
	if w.err != nil {
		return nil, w.err
	}
	if err := w.buf.Flush(); err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	index, err := packfile.NewIndex(w.entries)
	if err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	if err := os.WriteFile(idxPathOf(w.f.Name()), index.Encode(), 0o600); err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	return &Sealed{
		c:       w.c,
		flat:    w.c.newFlat(w.f.Name(), idxPathOf(w.f.Name())),
		bytes:   index.DataSize(),
		objects: uint64(index.Len()),
	}, nil
}

// idxPathOf returns the path of the index that belongs to a data file.
func idxPathOf(dataPath string) string {
	return strings.TrimSuffix(dataPath, dataExt) + idxExt
}

// Sealed is a finished file that is not in packs/ yet. At rest it is two
// files and a few words: its index stays on disk and is read by position,
// so any number of uploads can wait staged. Get and Has are safe for
// concurrent calls.
type Sealed struct {
	c       *Cache
	flat    *flat
	bytes   uint64
	objects uint64
	done    atomic.Bool // placed or discarded
}

var errSealedDone = errors.New("packcache: read of a sealed file that was placed or discarded")

// Get returns the object k in a slice of the caller's own. The error wraps
// ErrAbsent if the file does not hold k.
func (s *Sealed) Get(k key.Key) ([]byte, error) {
	if s.done.Load() {
		return nil, errSealedDone
	}
	data, ok, err := s.flat.get(k)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAbsent, k)
	}
	return data, nil
}

// Has reports whether the file holds k. A file that cannot be read holds
// nothing: Get says why.
func (s *Sealed) Has(k key.Key) bool {
	if s.done.Load() {
		return false
	}
	ok, err := s.flat.has(k)
	return err == nil && ok
}

// Bytes returns the size of the objects in the file, uncompressed.
func (s *Sealed) Bytes() uint64 { return s.bytes }

// Discard removes the file. After Place, and when called again, it does
// nothing. A closed cache deletes nothing: the file is read no more and
// left for the next Open.
func (s *Sealed) Discard() {
	if !s.done.CompareAndSwap(false, true) {
		return
	}
	if s.c.isClosed() {
		s.flat.abandon()
		return
	}
	s.flat.remove()
}

// maxOpen is the number of flat files, each a data file and an index, that
// are open at a time. Opening one more closes the one opened longest ago,
// which is opened again if it is still being read.
const maxOpen = 128

// flat is a data file with its index beside it, read by position. It is
// what a Sealed and a cached pack both are on disk, and stays the same
// flat when the one becomes the other. It is safe for concurrent use.
type flat struct {
	c *Cache

	// mu is held shared for the length of a read, and exclusively to
	// open, close, move and remove.
	mu        sync.RWMutex
	data, idx string // paths
	f         *files // nil while closed
	gone      bool   // removed
}

var errGone = errors.New("packcache: the file was removed")

func (c *Cache) newFlat(data, idx string) *flat {
	return &flat{c: c, data: data, idx: idx}
}

// acquire returns the open files with fl.mu held shared; the caller
// unlocks it when it has read.
func (fl *flat) acquire() (*files, error) {
	for {
		fl.mu.RLock()
		if fl.gone {
			fl.mu.RUnlock()
			return nil, errGone
		}
		if fl.f != nil {
			return fl.f, nil
		}
		fl.mu.RUnlock()
		if err := fl.open(); err != nil {
			return nil, err
		}
	}
}

// open opens the files if they are closed, and closes those of another
// flat if that makes one too many.
func (fl *flat) open() error {
	fl.mu.Lock()
	if fl.gone || fl.f != nil {
		fl.mu.Unlock()
		return nil
	}
	f, err := openFiles(fl.data, fl.idx)
	if err != nil {
		fl.mu.Unlock()
		return err
	}
	fl.f = f
	// The files enter the list while fl.mu is held, so that a removal or
	// a close finds them there: entered after it, they would stay in the
	// list as files that are open when they are gone.
	oldest := fl.c.opened(fl)
	fl.mu.Unlock()
	// No lock of fl is held here: the flat to close may be waiting for
	// this one in the same way.
	if oldest != nil {
		oldest.close()
	}
	return nil
}

// close closes the files; the next read opens them again.
func (fl *flat) close() {
	fl.mu.Lock()
	f := fl.f
	fl.f = nil
	fl.mu.Unlock()
	if f != nil {
		f.close()
	}
}

// move renames the two files. If it fails they are wherever fl says.
func (fl *flat) move(data, idx string) error {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.gone {
		return errGone
	}
	if err := os.Rename(fl.data, data); err != nil {
		return err
	}
	fl.data = data
	if err := os.Rename(fl.idx, idx); err != nil {
		return err
	}
	fl.idx = idx
	return nil
}

// remove deletes the two files, after the reads that are under way.
func (fl *flat) remove() { fl.end(true) }

// abandon is what a closed cache, which deletes nothing, does in place of
// remove: the two files are read no more and stay where they are.
func (fl *flat) abandon() { fl.end(false) }

func (fl *flat) end(unlink bool) {
	fl.mu.Lock()
	f := fl.f
	fl.f, fl.gone = nil, true
	if unlink {
		os.Remove(fl.idx)
		os.Remove(fl.data)
	}
	fl.mu.Unlock()
	if f != nil {
		f.close()
	}
	fl.c.forget(fl)
}

func (fl *flat) get(k key.Key) (data []byte, ok bool, err error) {
	f, err := fl.acquire()
	if err != nil {
		return nil, false, err
	}
	defer fl.mu.RUnlock()
	e, ok, err := f.find(k)
	if err != nil || !ok {
		return nil, false, err
	}
	data, err = f.object(e)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (fl *flat) has(k key.Key) (bool, error) {
	f, err := fl.acquire()
	if err != nil {
		return false, err
	}
	defer fl.mu.RUnlock()
	_, ok, err := f.find(k)
	return ok, err
}

// opened notes that fl has opened its files and returns the flat that has
// to close its own for it, if any. The caller has fl.mu; openMu is taken
// inside it and never the other way round.
func (c *Cache) opened(fl *flat) (oldest *flat) {
	c.openMu.Lock()
	defer c.openMu.Unlock()
	c.open = append(c.open, fl)
	if len(c.open) > maxOpen {
		oldest = c.open[0]
		c.open = append(c.open[:0], c.open[1:]...)
	}
	return oldest
}

// forget takes fl out of the list of open files.
func (c *Cache) forget(fl *flat) {
	c.openMu.Lock()
	defer c.openMu.Unlock()
	for i, o := range c.open {
		if o == fl {
			c.open = append(c.open[:i], c.open[i+1:]...)
			return
		}
	}
}

// closeFiles closes every open file. Reads open them again.
func (c *Cache) closeFiles() {
	c.openMu.Lock()
	open := c.open
	c.open = nil
	c.openMu.Unlock()
	for _, fl := range open {
		fl.close()
	}
}

// files is an open flat file.
type files struct {
	data *os.File
	idx  *os.File
	n    int    // entries in the index
	size uint64 // of the data
}

func openFiles(dataPath, idxPath string) (*files, error) {
	data, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("packcache: %w", err)
	}
	idx, err := os.Open(idxPath)
	if err != nil {
		data.Close()
		return nil, fmt.Errorf("packcache: %w", err)
	}
	f := &files{data: data, idx: idx}
	if err := f.measure(); err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}

// measure reads the sizes of the two files and the header of the index.
func (f *files) measure() error {
	info, err := f.data.Stat()
	if err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	f.size = uint64(info.Size())
	if info, err = f.idx.Stat(); err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	var header [packfile.IndexHeaderSize]byte
	if _, err := f.idx.ReadAt(header[:], 0); err != nil {
		return fmt.Errorf("packcache: %s is no index: %w", f.idx.Name(), noEOF(err))
	}
	count := binary.BigEndian.Uint64(header[len(packfile.IndexMagic):])
	if string(header[:len(packfile.IndexMagic)]) != packfile.IndexMagic ||
		count > packfile.MaxEntries || packfile.IndexSize(count) != uint64(info.Size()) {
		return fmt.Errorf("packcache: %s is no index of %d bytes", f.idx.Name(), info.Size())
	}
	f.n = int(count)
	return nil
}

func (f *files) close() {
	f.data.Close()
	f.idx.Close()
}

// noEOF turns the end of a file into the error it is when more was due.
func noEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

const (
	// window is the number of index entries read at once: what fits in
	// 4096 bytes.
	window = 4096 / packfile.EntrySize
	// estimates is the number of reads placed by estimate before find
	// falls back to halving.
	estimates = 3
)

// find looks k up in the index on disk. The entries are sorted by key, and
// a key begins with the hash of its object, so keys are spread evenly and
// the place of k can be estimated from its leading bytes: a window of
// entries is read around the estimate, and if k is not within it, the
// estimate is made again in the part that is left. That takes one read for
// an index of a thousand entries and two, on average, for one of a million.
// For keys that are not spread evenly, which no hash gives, find goes on by
// halving after a few estimates.
func (f *files) find(k key.Key) (packfile.Entry, bool, error) {
	target := binary.BigEndian.Uint64(k[:])
	// k, if the index has it, is at a position in [lo, hi), and the
	// leading eight bytes of the keys there lie within [loV, hiV].
	lo, hi := 0, f.n
	loV, hiV := uint64(0), uint64(math.MaxUint64)
	var buf [window * packfile.EntrySize]byte
	for step := 0; lo < hi; step++ {
		at := lo + (hi-lo)/2
		if step < estimates && loV <= target && target <= hiV && loV < hiV {
			at = lo + int(float64(target-loV)/float64(hiV-loV)*float64(hi-lo))
		}
		// The window lies around at, and within [lo, hi).
		start := max(lo, min(at-window/2, hi-window))
		n := min(window, hi-start)
		b := buf[:n*packfile.EntrySize]
		if _, err := f.idx.ReadAt(b, int64(packfile.IndexHeaderSize+start*packfile.EntrySize)); err != nil {
			return packfile.Entry{}, false, fmt.Errorf("packcache: read %s: %w", f.idx.Name(), noEOF(err))
		}
		entry := func(i int) []byte { return b[i*packfile.EntrySize:][:packfile.EntrySize] }
		switch {
		case bytes.Compare(k[:], entry(0)[:key.Size]) < 0:
			hi, hiV = start, binary.BigEndian.Uint64(entry(0))
		case bytes.Compare(k[:], entry(n - 1)[:key.Size]) > 0:
			lo, loV = start+n, binary.BigEndian.Uint64(entry(n-1))
		default:
			i := sort.Search(n, func(i int) bool { return bytes.Compare(entry(i)[:key.Size], k[:]) >= 0 })
			raw := entry(i)
			if !bytes.Equal(raw[:key.Size], k[:]) {
				return packfile.Entry{}, false, nil
			}
			return packfile.Entry{
				Key:    k,
				Offset: binary.BigEndian.Uint64(raw[key.Size:]),
				Length: binary.BigEndian.Uint32(raw[key.Size+8:]),
			}, true, nil
		}
	}
	return packfile.Entry{}, false, nil
}

// object reads the bytes e names from the data file into a new slice.
func (f *files) object(e packfile.Entry) ([]byte, error) {
	if e.Offset > f.size || uint64(e.Length) > f.size-e.Offset {
		return nil, fmt.Errorf("packcache: object %s lies at %d+%d, beyond the %d bytes of %s", e.Key, e.Offset, e.Length, f.size, f.data.Name())
	}
	data := make([]byte, e.Length)
	if len(data) == 0 {
		return data, nil
	}
	if _, err := f.data.ReadAt(data, int64(e.Offset)); err != nil {
		return nil, fmt.Errorf("packcache: read %s: %w", f.data.Name(), noEOF(err))
	}
	return data, nil
}
