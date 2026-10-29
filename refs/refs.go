// Package refs makes the references of a jaccard-store server local and
// reads them: the narinfo of a store path and the content its NAR is
// rebuilt from. Nothing of a reference is given out before it was checked.
//
// A reference is asked for by the hash in its name. The server says which
// root the name points at and which packs hold it, one or two. The packs
// that are not in the cache are downloaded, every object checked against
// its key on the way, and then the tree is walked from the root to see
// that all of it is there. A narinfo must be the narinfo of the store path
// it is named for, as narrenhaus stores one. A content reference has its
// NAR rebuilt once and compared with the hash and length its recipe states,
// and the recipe's hash with the name. What passes is recorded in the
// cache, so that it is done once.
//
// The work of making a reference local belongs to the store and not to the
// request that began it: it runs under the store's context, goes on when
// the request gives up, and is joined by every later request for the same
// reference. One download runs per pack, whichever references need it.
//
// A reference that cannot be what its name says is broken: it is refused,
// the packs that were fetched for it are dropped from the cache, and it is
// remembered, so that nothing is downloaded for it again while the name
// points at the same root. That verdict is passed on packs as they came
// from the server; packs that were cached and do not read as they should
// are dropped and fetched again first. A transfer that fails is no broken
// reference either, and is tried again by the next request. Whatever the
// reason, a caller learns only that there is nothing to serve
// (ErrNotFound); the log says why.
package refs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"

	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/remote"
)

const (
	defaultContentDownloads = 8
	defaultNarinfoDownloads = 16

	// maxNarinfoPacks is the most bytes of objects the packs of a narinfo
	// reference may announce. A narinfo is at most narinfo.MaxStored; the
	// rest is room for a parent pack, should a server ever name one.
	maxNarinfoPacks = 4 << 20

	// The lengths of the two hashes in Nix's base 32.
	storeHashLen = 32
	narHashLen   = 52
)

// Options configure a Store.
type Options struct {
	// Prefix is put before the path Nix asks for to make a reference's
	// name, as it is: no separator is added.
	Prefix string
	// ContentDownloads is how many packs of content references are
	// downloaded at once; 0 means 8.
	ContentDownloads int
	// NarinfoDownloads is how many packs of narinfo references are
	// downloaded at once; 0 means 16. They do not wait for content.
	NarinfoDownloads int
	// Log receives why a reference was not served. Nil means
	// slog.Default().
	Log *slog.Logger
}

var (
	// ErrNotFound: no such reference, or one that must not be served
	// (broken, unknown recipe, transfer failure, server unreachable and
	// nothing local). The HTTP answer is 404; the log says which.
	ErrNotFound = errors.New("refs: not found")

	// errBroken marks, inside the package, a reference that cannot be
	// what its name says: see settle.
	errBroken = errors.New("broken reference")
	// errDamaged marks a cached pack that could not be read as it was
	// when it was placed. The reference is not at fault.
	errDamaged = errors.New("the cached packs cannot be read")
)

// kind is which of the two references of a store path one is.
type kind int

const (
	kindNarinfo kind = iota + 1
	kindContent
)

func (k kind) String() string {
	if k == kindNarinfo {
		return "narinfo"
	}
	return "content"
}

// ref is a reference as a lookup found it: a name and the root it points
// at.
type ref struct {
	kind kind
	name string
	hash string // the hash in the name, in Nix's base 32
	root key.Key
}

// refKey is what the work on a reference goes by: the same name pointing
// at another root is another matter.
type refKey struct {
	name string
	root key.Key
}

func (r ref) key() refKey { return refKey{name: r.name, root: r.root} }

// refusal is why a reference is not served, as it was found out once.
type refusal struct {
	broken bool // false: a recipe this narrenhaus does not know
	reason string
}

// Store reads the references of one server under one prefix through one
// cache. It is safe for concurrent use.
type Store struct {
	ctx    context.Context
	cancel context.CancelFunc
	cache  *packcache.Cache
	rem    *remote.Remote
	prefix string
	log    *slog.Logger

	// The places of the downloads that may run at once, of content packs
	// and of narinfo packs, and of the checks of content references.
	contentPlaces chan struct{}
	narinfoPlaces chan struct{}
	checkPlaces   chan struct{}

	mu     sync.Mutex
	closed bool
	// jobs are the references being made local.
	jobs map[refKey]*job
	// fetches are the packs being downloaded, by their roots.
	fetches map[key.Key]*flight
	// refused are the references found broken or of an unknown recipe,
	// by name and then by root.
	refused map[string]map[key.Key]refusal
	// work counts the jobs that run.
	work sync.WaitGroup
}

// New returns a store whose downloads and checks run under ctx, not
// under the contexts of the calls that start them.
func New(ctx context.Context, cache *packcache.Cache, rem *remote.Remote, opts Options) *Store {
	s := &Store{
		cache:   cache,
		rem:     rem,
		prefix:  opts.Prefix,
		log:     opts.Log,
		jobs:    make(map[refKey]*job),
		fetches: make(map[key.Key]*flight),
		refused: make(map[string]map[key.Key]refusal),
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	if s.log == nil {
		s.log = slog.Default()
	}
	content, narinfos := opts.ContentDownloads, opts.NarinfoDownloads
	if content <= 0 {
		content = defaultContentDownloads
	}
	if narinfos <= 0 {
		narinfos = defaultNarinfoDownloads
	}
	s.contentPlaces = make(chan struct{}, content)
	s.narinfoPlaces = make(chan struct{}, narinfos)
	s.checkPlaces = make(chan struct{}, content)
	return s
}

// Close ends the store's work: the downloads and checks under way are
// given up, as when the context given to New ends, and Close returns when
// they have stopped. No work begins after it. What is cached can still be
// read, and a Content that is out can be written and released. Close
// belongs before the cache's.
func (s *Store) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.work.Wait()
}

// NarinfoName returns the name of the narinfo reference of the store path
// with hash part storeHash (spec 4.1): <prefix><storeHash>.narinfo.
func (s *Store) NarinfoName(storeHash string) string {
	return s.prefix + storeHash + ".narinfo"
}

// ContentName returns the name of the content reference of the NAR with
// the SHA-256 narHash, in Nix's base 32 (spec 4.1):
// <prefix>nar/<narHash>.nar.
func (s *Store) ContentName(narHash string) string {
	return s.prefix + "nar/" + narHash + ".nar"
}

// PlaceUploaded records a pack an upload has just placed in the cache as
// the local, checked copy of the reference name with root. If the pack
// has left the cache again there is nothing to record, and that is no
// error. Whatever was remembered against the reference is forgotten: the
// upload has checked what it placed.
func (s *Store) PlaceUploaded(name string, root key.Key) error {
	s.mu.Lock()
	if byRoot := s.refused[name]; byRoot != nil {
		if delete(byRoot, root); len(byRoot) == 0 {
			delete(s.refused, name)
		}
	}
	s.mu.Unlock()

	packs, ok := s.packsOf(root)
	if !ok {
		return nil
	}
	if err := s.cache.PutRecord(name, packcache.Record{Root: root, Packs: packs, Checked: true}); err != nil {
		return fmt.Errorf("refs: recording %s: %w", name, err)
	}
	return nil
}

// packsOf returns the roots of the packs that hold root as it is cached:
// its own pack and, when that is a patch, its parent. ok is false when
// nothing is cached under root.
func (s *Store) packsOf(root key.Key) (packs []key.Key, ok bool) {
	meta, ok := s.cache.Meta(root)
	switch {
	case !ok:
		return nil, false
	case meta.Complete:
		return []key.Key{root}, true
	}
	return []key.Key{root, meta.Parent}, true
}

// isStoreHash reports whether s is the hash part of a store path: 32
// characters of Nix's base 32.
func isStoreHash(s string) bool {
	if len(s) != storeHashLen {
		return false
	}
	_, err := nix32.Decode(s)
	return err == nil
}

// narSum returns the SHA-256 that s writes in Nix's base 32, 52
// characters. ok is false when s is none.
func narSum(s string) (sum [32]byte, ok bool) {
	if len(s) != narHashLen {
		return sum, false
	}
	b, err := nix32.Decode(s)
	if err != nil || len(b) != len(sum) {
		return sum, false
	}
	copy(sum[:], b)
	return sum, true
}

// show quotes what a caller gave for an error, cut so that the error
// stays short.
func show(s string) string {
	const most = 64
	if len(s) > most {
		return strconv.Quote(s[:most]) + "..."
	}
	return strconv.Quote(s)
}

// notFoundf returns an error that wraps ErrNotFound.
func notFoundf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, args...))
}

// brokenf returns an error that wraps errBroken.
func brokenf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBroken, fmt.Sprintf(format, args...))
}

// gaveUp is the error of a caller whose context ended while it waited.
func gaveUp(ctx context.Context, name string) error {
	return fmt.Errorf("%w: the wait for %s ended: %w", ErrNotFound, name, ctx.Err())
}

// lookup asks the server where name points. found is false, with no
// error, when the server knows no such name. The record of the name is
// removed then, and what was remembered against the name is forgotten: a
// reference written anew after it was deleted is looked at afresh, also
// when it has the root it had. An error says that the server could not be
// asked.
func (s *Store) lookup(ctx context.Context, name string) (root key.Key, packs []client.Pack, found bool, err error) {
	root, packs, err = s.rem.Lookup(ctx, name)
	switch {
	case err == nil:
		return root, packs, true, nil
	case errors.Is(err, client.ErrNotFound):
		s.cache.DeleteRecord(name)
		s.mu.Lock()
		delete(s.refused, name)
		s.mu.Unlock()
		return key.Key{}, nil, false, nil
	}
	return key.Key{}, nil, false, err
}

// admit holds the server's answer for a reference of kind k to what such
// a reference can be, before anything is downloaded (spec 6.2 step 3, 6.3
// step 1). Its error wraps errBroken.
func admit(k kind, root key.Key, packs []client.Pack) error {
	switch {
	case len(packs) == 0 || len(packs) > 2:
		return brokenf("the server names %d packs for it", len(packs))
	case packs[0].Root != root:
		return brokenf("the server names the pack of %s for the root %s", packs[0].Root, root)
	case len(packs) == 2 && packs[1].Root == root:
		return brokenf("the server names the pack of %s as its own parent", root)
	}
	switch k {
	case kindNarinfo:
		if t := root.Type(); t != key.Blob && t != key.FileNode {
			return brokenf("the root %s is a %s, a narinfo is a Blob or a FileNode", root, t)
		}
		if root.Length() > narinfo.MaxStored {
			return brokenf("the root %s is a text of %d bytes, a narinfo has at most %d", root, root.Length(), narinfo.MaxStored)
		}
		var announced uint64
		for _, p := range packs {
			if p.Bytes > maxNarinfoPacks-announced {
				return brokenf("the packs of the root %s announce more than %d bytes of objects", root, maxNarinfoPacks)
			}
			announced += p.Bytes
		}
	case kindContent:
		if t := root.Type(); t != key.DirLeaf && t != key.DirNode {
			return brokenf("the root %s is a %s, the content of a store path is a DirLeaf or a DirNode", root, t)
		}
	}
	return nil
}

// refusedFor returns why r is not served, if that was found out before.
func (s *Store) refusedFor(r ref) (refusal, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refusedLocked(r)
}

// refusedLocked is refusedFor for a caller that has s.mu.
func (s *Store) refusedLocked(r ref) (refusal, bool) {
	why, ok := s.refused[r.name][r.root]
	return why, ok
}

// refuse remembers that r is not to be served, and says so in the log:
// as an error for a broken reference, as a warning for a recipe of
// another version.
func (s *Store) refuse(r ref, broken bool, cause error) {
	s.mu.Lock()
	byRoot := s.refused[r.name]
	if byRoot == nil {
		byRoot = make(map[key.Key]refusal)
		s.refused[r.name] = byRoot
	}
	byRoot[r.root] = refusal{broken: broken, reason: cause.Error()}
	s.mu.Unlock()
	if broken {
		s.log.Error("a broken reference is not served, and nothing is fetched for it again while its name points at this root",
			"name", r.name, "root", r.root.String(), "reason", cause.Error())
		return
	}
	s.log.Warn("a reference with a recipe this narrenhaus does not know is not served",
		"name", r.name, "root", r.root.String(), "reason", cause.Error())
}

// again is the error of a reference that was refused before.
func (r ref) again(why refusal) error {
	if why.broken {
		return notFoundf("%s was found broken: %s", r.name, why.reason)
	}
	return notFoundf("%s: %s", r.name, why.reason)
}

// settle is the end of every attempt to make r local. It turns err, the
// outcome, into what the callers get, and acts on it:
//
//   - A broken reference is remembered, and packs, those that were made
//     local for it, are dropped from the cache.
//   - A recipe of another format or version is remembered; the packs stay.
//   - Cached packs that cannot be read are dropped, to be fetched again.
//   - Anything else is a failure for now, and only logged.
func (s *Store) settle(r ref, packs []client.Pack, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errBroken):
		s.refuse(r, true, err)
		for _, p := range packs {
			s.cache.Drop(p.Root)
		}
	case errors.Is(err, nartree.ErrUnknownRecipe):
		s.refuse(r, false, err)
	case errors.Is(err, errDamaged):
		s.log.Warn("cached packs cannot be read and are dropped from the cache",
			"name", r.name, "root", r.root.String(), "error", err.Error())
		for _, p := range packs {
			s.cache.Drop(p.Root)
		}
	case s.ctx.Err() != nil:
		s.log.Debug("making a reference local was given up", "name", r.name, "root", r.root.String(), "error", err.Error())
	default:
		s.log.Warn("a reference could not be made local", "name", r.name, "root", r.root.String(), "error", err.Error())
	}
	return fmt.Errorf("%w: %s: %w", ErrNotFound, r.name, err)
}

// obtain makes r local as the server names its packs, waiting for that
// no longer than ctx allows, and returns a hold on its packs to read it
// through. Every error wraps ErrNotFound.
func (s *Store) obtain(ctx context.Context, r ref, packs []client.Pack) (*packcache.Hold, error) {
	if why, ok := s.refusedFor(r); ok {
		s.log.Debug("a reference that was refused before is asked for again", "name", r.name, "reason", why.reason)
		return nil, r.again(why)
	}
	if err := admit(r.kind, r.root, packs); err != nil {
		// Nothing was fetched for it, so there is nothing of it to drop:
		// a pack cached under that root is another reference's.
		return nil, s.settle(r, nil, err)
	}
	// The packs are held from the moment they are known, so that what is
	// placed for this request is there when the request reads it.
	reserved := s.cache.Reserve(roots(packs)...)
	defer reserved.Release()

	if !s.isLocal(r, packs) {
		j, err := s.join(r, packs)
		if err != nil {
			return nil, err
		}
		select {
		case <-j.done:
			if j.err != nil {
				return nil, j.err
			}
		case <-ctx.Done():
			// The work goes on: the next request finds it done.
			return nil, gaveUp(ctx, r.name)
		}
	}
	hold, ok := s.cache.Acquire(r.root)
	if !ok {
		return nil, notFoundf("the packs of %s left the cache", r.name)
	}
	return hold, nil
}

// fromRecord returns a hold on the packs the record of name asks for,
// with the root the name pointed at when it was last made local. ok is
// false without a record or when one of its packs is not cached.
func (s *Store) fromRecord(name string, checked bool) (root key.Key, hold *packcache.Hold, ok bool) {
	rec, ok := s.cache.Record(name)
	if !ok || (checked && !rec.Checked) {
		return key.Key{}, nil, false
	}
	hold, ok = s.cache.Acquire(rec.Packs...)
	if !ok {
		return key.Key{}, nil, false
	}
	return rec.Root, hold, true
}

// unreadable is the end of a read of r from packs that were taken for
// sound. The packs are dropped from the cache, so that the next request
// fetches them, and nothing is remembered against the reference: whether
// it is broken shows when it has been fetched anew.
func (s *Store) unreadable(r ref, err error) error {
	s.log.Warn("a cached reference cannot be read as it was recorded; its pack is dropped from the cache",
		"name", r.name, "root", r.root.String(), "error", err.Error())
	s.cache.Drop(r.root)
	return fmt.Errorf("%w: %s: %w", ErrNotFound, r.name, err)
}

// roots returns the roots of packs, in their order.
func roots(packs []client.Pack) []key.Key {
	out := make([]key.Key, len(packs))
	for i, p := range packs {
		out[i] = p.Root
	}
	return out
}
