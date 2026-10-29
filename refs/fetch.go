package refs

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/verify"

	"github.com/amber-store/narrenhaus/packcache"
)

// maxRounds is how often one piece of work goes through the packs of its
// reference, and how often a request looks for them: once as they are,
// once more after cached packs that did not read as they should were
// dropped, and once for a pack that another reference's work dropped
// meanwhile.
const maxRounds = 3

// job is one reference being made local. err is set before done is
// closed, and wraps ErrNotFound.
type job struct {
	done chan struct{}
	err  error
}

// flight is one pack being downloaded. err is set before done is closed.
type flight struct {
	done chan struct{}
	err  error
}

// progress is what a job has found out about its reference so far.
type progress struct {
	// fetched are the packs that came from the server for this job: those
	// it downloaded, and whatever is cached under a root after the job
	// dropped the pack it found there.
	fetched map[key.Key]bool
	// rebuilt says that the NAR was rebuilt and is what the recipe states.
	// That holds for the root, whichever copies of its packs are read.
	rebuilt bool
}

// isLocal reports whether r needs no work: it is recorded as pointing at
// this root, and as checked if it is content, and its packs are present
// as the server names them now.
func (s *Store) isLocal(r ref, packs []client.Pack) bool {
	rec, ok := s.cache.Record(r.name)
	if !ok || rec.Root != r.root || (r.kind == kindContent && !rec.Checked) {
		return false
	}
	own, ok := s.cache.Meta(r.root)
	switch {
	case !ok:
		return false
	case own.Complete:
		return true
	case len(packs) < 2 || own.Parent != packs[1].Root:
		// A patch of another parent than the server names, or of a root
		// the server names as a base pack (spec 7.2).
		return false
	}
	parent, ok := s.cache.Meta(own.Parent)
	return ok && parent.Complete
}

// join returns the job that makes r local, beginning it unless one runs.
// The job runs under the store's context whoever waits for it.
func (s *Store) join(r ref, packs []client.Pack) (*job, error) {
	k := r.key()
	s.mu.Lock()
	defer s.mu.Unlock()
	if why, ok := s.refusedLocked(r); ok {
		return nil, r.again(why)
	}
	if j := s.jobs[k]; j != nil {
		return j, nil
	}
	if s.closed || s.ctx.Err() != nil {
		return nil, notFoundf("%s is not local, and narrenhaus is stopping", r.name)
	}
	j := &job{done: make(chan struct{})}
	s.jobs[k] = j
	s.work.Add(1)
	go func() {
		defer s.work.Done()
		hold, drop, err := s.make(r, packs)
		// What came of it is settled before the job leaves the table: a
		// request finds the job or what it left.
		j.err = s.settle(r, drop, err)
		s.mu.Lock()
		delete(s.jobs, k)
		if j.err == nil {
			// The job's hold stays until a request has taken what was
			// made local: the one that began it may have given up.
			hold = s.keepLocked(k, hold)
		}
		s.mu.Unlock()
		hold.Release()
		close(j.done)
	}()
	return j, nil
}

// make makes r local: its packs are fetched, the tree is seen to be
// whole, and the reference is checked and recorded. It returns the hold it
// kept the packs with, which the caller releases, and, with an error, the
// packs to drop if the error is that the reference is broken: its own
// pack, if it was fetched for this.
//
// A reference is called broken on the strength of packs that came from
// the server for this work. One that was in the cache may have suffered
// there: when the reference looks broken or cannot be read, the packs
// that came from the cache are dropped and fetched, and the verdict is
// passed on what comes then. A pack that the work of another reference
// drops meanwhile is fetched again in the same way.
func (s *Store) make(r ref, packs []client.Pack) (*packcache.Hold, []key.Key, error) {
	// A hold of the job's own: the request that began it may be gone
	// long before the packs are placed.
	hold := s.cache.Reserve(roots(packs)...)
	got := &progress{fetched: make(map[key.Key]bool)}
	// own is the reference's own pack, if this work fetched it. The
	// parent is never dropped with a broken reference: it was checked
	// object by object when it came, and is the base of others.
	own := func() []key.Key {
		if got.fetched[r.root] {
			return []key.Key{r.root}
		}
		return nil
	}

	lookedUp := false
	for round := 1; ; round++ {
		err := s.localize(packs, got.fetched)
		if client.StatusCode(err) == http.StatusForbidden && !lookedUp {
			// The bucket answers so when a URL has expired. A lookup gives
			// new ones, once.
			lookedUp = true
			s.log.Debug("the bucket refused a download; the reference is looked up again", "name", r.name, "error", err.Error())
			root, again, found, lerr := s.lookup(s.ctx, r.name)
			switch {
			case lerr != nil:
				return hold, nil, fmt.Errorf("%w; and the reference could not be looked up again: %w", err, lerr)
			case !found:
				return hold, nil, fmt.Errorf("%w; and the server has no such reference any more", err)
			case root != r.root:
				return hold, nil, fmt.Errorf("%w; and the reference points at %s now", err, root)
			}
			if err := admit(r.kind, root, again); err != nil {
				return hold, own(), err
			}
			fresh := s.cache.Reserve(roots(again)...)
			hold.Release()
			hold, packs = fresh, again
			err = s.localize(packs, got.fetched)
		}
		if errors.Is(err, client.ErrBadPack) {
			// The index or an object is not what the server announced or
			// what its key says. Asking again brings the same pack.
			return hold, own(), fmt.Errorf("%w: %w", errBroken, err)
		}
		if err == nil {
			err = s.verify(r, hold, got)
		}

		var stale []key.Key
		switch {
		case errors.Is(err, errGone):
			// Dropped under this work, by the work of another reference.
		case errors.Is(err, errBroken), errors.Is(err, errDamaged):
			cached, ok := s.packsOf(r.root)
			if !ok {
				break
			}
			for _, root := range cached {
				if !got.fetched[root] {
					stale = append(stale, root)
				}
			}
			if len(stale) > 0 {
				break
			}
			if errors.Is(err, errBroken) {
				return hold, own(), err
			}
			// Packs that came just now and cannot be read: the disk's
			// doing, and nothing that fetching them once more would cure.
			return hold, nil, err
		default:
			return hold, nil, err
		}
		if round == maxRounds || s.ctx.Err() != nil {
			if errors.Is(err, errBroken) {
				// No verdict: it would not be on what the server has.
				err = fmt.Errorf("%w: read from packs that were cached, the reference looks broken (%v)", errDamaged, err)
			}
			return hold, nil, err
		}
		if len(stale) > 0 {
			s.log.Warn("read from packs that were cached, a reference looks broken or cannot be read; they are dropped and fetched again",
				"name", r.name, "root", r.root.String(), "packs", fmt.Sprint(stale), "error", err.Error())
		}
		for _, root := range stale {
			s.cache.Drop(root)
			// Whatever is cached under it from here on came after this.
			got.fetched[root] = true
		}
	}
}

// localize brings the packs of a reference into the cache: its own, and
// the parent if its own is a patch. A pack that is cached counts as
// present by the rule of spec 7.2. The roots of the packs it downloads
// are entered into fetched.
func (s *Store) localize(packs []client.Pack, fetched map[key.Key]bool) error {
	own := packs[0]
	var parent *key.Key
	if len(packs) == 2 {
		parent = &packs[1].Root
	}
	if err := s.ensure(own, parent, fetched); err != nil {
		return fmt.Errorf("the pack of %s: %w", own.Root, err)
	}
	beside, err := s.parentOf(packs)
	if err != nil || beside == nil {
		return err
	}
	if err := s.ensure(*beside, nil, fetched); err != nil {
		return fmt.Errorf("the parent pack of %s: %w", beside.Root, err)
	}
	return nil
}

// parentOf returns the pack that the own pack of packs needs beside it as
// it is cached: nil for a complete pack, which needs no parent whatever
// the server names, and the pack the server names as the parent for a
// patch. What ensure found under the root may have been dropped and
// placed anew since, by the work of another reference: a patch for which
// the server names no parent, or another one, is a failure for now.
func (s *Store) parentOf(packs []client.Pack) (*client.Pack, error) {
	own := packs[0].Root
	meta, ok := s.cache.Meta(own)
	switch {
	case !ok:
		return nil, fmt.Errorf("%w: the pack of %s", errGone, own)
	case meta.Complete:
		return nil, nil
	case len(packs) < 2:
		return nil, fmt.Errorf("the pack of %s is cached as a patch of %s, and the server names no parent", own, meta.Parent)
	case meta.Parent != packs[1].Root:
		return nil, fmt.Errorf("the pack of %s is cached as a patch of %s, and the server names the parent %s", own, meta.Parent, packs[1].Root)
	}
	return &packs[1], nil
}

// ensure sees to it that the pack p is cached: as a complete pack, or,
// when parent is not nil, as a patch of *parent. A pack cached as
// something else is stale: it is dropped and fetched anew.
func (s *Store) ensure(p client.Pack, parent *key.Key, fetched map[key.Key]bool) error {
	// A download that another reference began may place the pack as the
	// server named it then, which may not be how it is named now. That is
	// rare enough to be tried a few times and no more.
	for attempt := 0; ; attempt++ {
		meta, ok := s.cache.Meta(p.Root)
		if ok && (meta.Complete || (parent != nil && meta.Parent == *parent)) {
			return nil
		}
		if attempt == 3 {
			return errors.New("it is cached as another pack than the server names, again and again")
		}
		if ok {
			s.cache.Drop(p.Root)
		}
		if err := s.fetch(p, parent); err != nil {
			return err
		}
		fetched[p.Root] = true
	}
}

// fetch downloads the pack p into the cache, or waits for the download of
// that pack that is under way: one runs per root at a time.
func (s *Store) fetch(p client.Pack, parent *key.Key) error {
	s.mu.Lock()
	if f := s.fetches[p.Root]; f != nil {
		s.mu.Unlock()
		<-f.done
		return f.err
	}
	f := &flight{done: make(chan struct{})}
	s.fetches[p.Root] = f
	s.mu.Unlock()

	f.err = s.download(p, parent)
	s.mu.Lock()
	delete(s.fetches, p.Root)
	s.mu.Unlock()
	close(f.done)
	return f.err
}

// download fetches the pack p and places it in the cache: as a patch of
// *parent, or as a complete pack when parent is nil. Every object is
// checked against its key before it is written, and what was written is
// removed again unless all of the pack arrived.
func (s *Store) download(p client.Pack, parent *key.Key) error {
	// A pack is a narinfo's or content's by its root. Narinfo packs have
	// places of their own and never wait for content.
	places := s.contentPlaces
	if t := p.Root.Type(); t == key.Blob || t == key.FileNode {
		places = s.narinfoPlaces
	}
	select {
	case places <- struct{}{}:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	defer func() { <-places }()

	started := time.Now()
	w, err := s.cache.NewDownload()
	if err != nil {
		return err
	}
	defer w.Discard()
	if _, err := s.rem.Fetch(s.ctx, p, func(o packfile.Object) error { return w.Add(o.Key, o.Data) }); err != nil {
		return err
	}
	sealed, err := w.Seal()
	if err != nil {
		return err
	}
	if err := s.cache.Place(sealed, p.Root, parent); err != nil {
		return err
	}
	s.log.Debug("a pack was downloaded", "root", p.Root.String(), "objects", p.Objects, "bytes", p.Bytes,
		"patch", parent != nil, "took", time.Since(started).Round(time.Millisecond))
	return nil
}

// verify checks r once its packs are local, reading through hold: the
// tree must be whole, a narinfo must be the narinfo of its name, and
// content must rebuild to the NAR of its recipe and its name. What passes
// is recorded. Every object that is read is held to its key.
func (s *Store) verify(r ref, hold *packcache.Hold, got *progress) error {
	if r.kind == kindContent {
		// Walking a large tree and rebuilding its NAR takes a processor
		// for a while: no more of that at once than of downloads.
		select {
		case s.checkPlaces <- struct{}{}:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
		defer func() { <-s.checkPlaces }()
	}
	if err := complete(r.root, hold); err != nil {
		return err
	}
	switch r.kind {
	case kindNarinfo:
		if _, err := readNarinfo(r, sound(hold.Get)); err != nil {
			return err
		}
	case kindContent:
		if err := s.checkContent(r, hold, got); err != nil {
			return err
		}
	}

	// The packs were read through the job's hold, which reads a pack that
	// was dropped as long as it likes. A request needs them in the cache.
	packs, ok := s.packsOf(r.root)
	if ok && len(packs) == 2 {
		parent, there := s.cache.Meta(packs[1])
		ok = there && parent.Complete
	}
	if !ok {
		return fmt.Errorf("%w: a pack of %s, while it was checked", errGone, r.root)
	}
	rec := packcache.Record{Root: r.root, Packs: packs, Checked: r.kind == kindContent}
	if err := s.cache.PutRecord(r.name, rec); err != nil {
		// The reference is local all the same; without the record the
		// next request checks it again.
		s.log.Warn("the record of a reference could not be written", "name", r.name, "error", err.Error())
	}
	return nil
}

// sound returns get with every object it gives held to its key. The cache
// checks nothing when it is read, and takes a pack for whole by the sizes
// of its files: bytes that changed on disk under an unchanged size show
// here and nowhere else. An object that is not what its key says is a
// cached pack that cannot be read, errDamaged.
func sound(get func(key.Key) ([]byte, error)) func(key.Key) ([]byte, error) {
	return func(k key.Key) ([]byte, error) {
		data, err := get(k)
		if err != nil {
			return nil, err
		}
		if err := verify.Object(k, data); err != nil {
			return nil, fmt.Errorf("%w: an object is not what it was when it was placed: %v", errDamaged, err)
		}
		return data, nil
	}
}

// firstError keeps the first error it is given. It is safe for
// concurrent use.
type firstError struct {
	mu  sync.Mutex
	err error
}

func (f *firstError) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil {
		f.err = err
	}
}

func (f *firstError) get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// complete walks the tree from root through hold and returns nil when
// every object is there. A tree that is not whole, or holds an object
// that does not decode, is broken; a pack that cannot be read is damaged.
func complete(root key.Key, hold *packcache.Hold) error {
	// CheckComplete returns the failure of whichever lookup it saw fail
	// first. A read that failed is told from an object that is absent or
	// does not decode by noting it here.
	var failed firstError
	read := sound(hold.Get)
	get := func(k key.Key) ([]byte, error) {
		data, err := read(k)
		if err != nil && !errors.Is(err, packcache.ErrAbsent) {
			failed.set(err)
		}
		return data, err
	}
	has := func(k key.Key) (bool, error) {
		ok, err := hold.Has(k)
		if err != nil {
			failed.set(err)
		}
		return ok, err
	}
	_, err := fstree.CheckComplete(root, get, has, 0)
	switch {
	case err == nil:
		return nil
	case failed.get() != nil:
		return fmt.Errorf("%w: %w", errDamaged, failed.get())
	}
	return fmt.Errorf("%w: its packs do not hold the whole tree of %s: %v", errBroken, root, err)
}
