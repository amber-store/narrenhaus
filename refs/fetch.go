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

	"github.com/amber-store/narrenhaus/packcache"
)

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
		used, err := s.make(r, packs)
		// What came of it is settled before the job leaves the table: a
		// request finds the job or what it left.
		j.err = s.settle(r, used, err)
		s.mu.Lock()
		delete(s.jobs, k)
		s.mu.Unlock()
		close(j.done)
	}()
	return j, nil
}

// make makes r local: its packs are fetched, the tree is seen to be
// whole, and the reference is checked and recorded. It returns the packs
// it went by, which are those of a second lookup when the URLs of the
// first had expired.
func (s *Store) make(r ref, packs []client.Pack) ([]client.Pack, error) {
	// A hold of the job's own: the request that began it may be gone
	// long before the packs are placed.
	hold := s.cache.Reserve(roots(packs)...)
	defer func() { hold.Release() }()

	// fetched are the packs that were downloaded for this attempt.
	fetched := make(map[key.Key]bool)
	err := s.localize(packs, fetched)
	if client.StatusCode(err) == http.StatusForbidden {
		// The bucket answers so when a URL has expired. A lookup gives
		// new ones, once.
		s.log.Debug("the bucket refused a download; the reference is looked up again", "name", r.name, "error", err.Error())
		root, again, found, lerr := s.lookup(s.ctx, r.name)
		switch {
		case lerr != nil:
			return packs, fmt.Errorf("%w; and the reference could not be looked up again: %w", err, lerr)
		case !found:
			return packs, fmt.Errorf("%w; and the server has no such reference any more", err)
		case root != r.root:
			return packs, fmt.Errorf("%w; and the reference points at %s now", err, root)
		}
		if err := admit(r.kind, root, again); err != nil {
			return packs, err
		}
		fresh := s.cache.Reserve(roots(again)...)
		hold.Release()
		hold, packs = fresh, again
		err = s.localize(packs, fetched)
	}
	if err != nil {
		if errors.Is(err, client.ErrBadPack) {
			// The index or an object is not what the server announced or
			// what its key says. Asking again brings the same pack.
			err = fmt.Errorf("%w: %w", errBroken, err)
		}
		return packs, err
	}

	err = s.verify(r, hold)
	if errors.Is(err, errBroken) && !s.allFetched(r.root, fetched) {
		// A reference is called broken on the strength of packs that
		// came from the server just now. One that was in the cache may
		// have suffered there: the packs are dropped, and the next
		// attempt fetches them and passes the verdict.
		err = fmt.Errorf("%w: read from packs that were cached, the reference looks broken (%v)", errDamaged, err)
	}
	return packs, err
}

// allFetched reports whether the packs that hold root, as they are cached
// now, are all among fetched.
func (s *Store) allFetched(root key.Key, fetched map[key.Key]bool) bool {
	packs, ok := s.packsOf(root)
	if !ok {
		return false
	}
	for _, p := range packs {
		if !fetched[p] {
			return false
		}
	}
	return true
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
	meta, ok := s.cache.Meta(own.Root)
	switch {
	case !ok:
		return fmt.Errorf("the pack of %s left the cache while it was held", own.Root)
	case meta.Complete:
		// It needs no parent, whatever the server names.
		return nil
	}
	if err := s.ensure(packs[1], nil, fetched); err != nil {
		return fmt.Errorf("the parent pack of %s: %w", packs[1].Root, err)
	}
	return nil
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
// is recorded.
func (s *Store) verify(r ref, hold *packcache.Hold) error {
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
		if _, err := readNarinfo(r, hold.Get); err != nil {
			return err
		}
	case kindContent:
		if err := s.checkContent(r, hold); err != nil {
			return err
		}
	}

	packs, ok := s.packsOf(r.root)
	if !ok {
		return fmt.Errorf("the pack of %s left the cache while it was held", r.root)
	}
	rec := packcache.Record{Root: r.root, Packs: packs, Checked: r.kind == kindContent}
	if err := s.cache.PutRecord(r.name, rec); err != nil {
		// The reference is local all the same; without the record the
		// next request checks it again.
		s.log.Warn("the record of a reference could not be written", "name", r.name, "error", err.Error())
	}
	return nil
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
	get := func(k key.Key) ([]byte, error) {
		data, err := hold.Get(k)
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
