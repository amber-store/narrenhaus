package refs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
)

// Content makes the content reference of narHash local and checked
// (spec 6.3 steps 1 and 2) and returns it held: its packs stay in the
// cache until Release, which the caller owes. ctx bounds the wait, not
// the work: when it ends while the packs are fetched or the NAR is
// checked, that goes on for the next request.
//
// The root is taken from the record of the name when the reference was
// checked before and its packs are all cached; the server is not asked
// then. Otherwise it is asked, and without an answer there is nothing to
// serve.
//
// Every error wraps ErrNotFound.
func (s *Store) Content(ctx context.Context, narHash string) (*Content, error) {
	sum, ok := narSum(narHash)
	if !ok {
		return nil, notFoundf("%s is not the SHA-256 of a NAR in Nix's base 32", show(narHash))
	}
	name := s.ContentName(narHash)

	if root, hold, ok := s.fromRecord(name, true); ok {
		r := ref{kind: kindContent, name: name, hash: narHash, root: root}
		c, err := s.open(r, sum, hold)
		if err == nil {
			return c, nil
		}
		// What is cached is not what was recorded. It is dropped, and
		// the reference is fetched like one that was never here.
		hold.Release()
		s.unreadable(r, err)
	}

	root, packs, found, err := s.lookup(ctx, name)
	switch {
	case err == nil && !found:
		return nil, notFoundf("the server has no reference %s", name)
	case err != nil && ctx.Err() != nil:
		return nil, gaveUp(ctx, name)
	case err != nil:
		s.log.Debug("the server could not be asked and the content is not cached", "name", name, "error", err.Error())
		return nil, fmt.Errorf("%w: %s is not cached and the server could not be asked: %w", ErrNotFound, name, err)
	}

	r := ref{kind: kindContent, name: name, hash: narHash, root: root}
	hold, err := s.obtain(ctx, r, packs)
	if err != nil {
		return nil, err
	}
	c, err := s.open(r, sum, hold)
	if err != nil {
		hold.Release()
		return nil, s.unreadable(r, err)
	}
	return c, nil
}

// open reads the recipe of r through hold and returns the content, which
// takes the hold over. The recipe must be of the NAR r is named for.
func (s *Store) open(r ref, sum [32]byte, hold *packcache.Hold) (*Content, error) {
	opened, err := nartree.Open(r.root, hold.Get)
	if err != nil {
		return nil, err
	}
	if opened.NarHash != sum {
		return nil, fmt.Errorf("its recipe is of the NAR %s", nix32.Encode(opened.NarHash[:]))
	}
	return &Content{store: s, ref: r, hold: hold, node: opened.Node, narSize: opened.NarSize}, nil
}

// checkContent is the check of a content reference whose packs are local
// and whole (spec 6.3 step 2): the recipe is read, it must be of the NAR
// the reference is named for, and the NAR rebuilt from the tree must have
// the hash and length the recipe states. The rebuilding is done once per
// root: a reference that is recorded as checked, and is here again
// because a pack of it had left the cache, is not rebuilt.
func (s *Store) checkContent(r ref, hold *packcache.Hold) error {
	opened, err := nartree.Open(r.root, hold.Get)
	switch {
	case errors.Is(err, nartree.ErrUnknownRecipe):
		return err
	case errors.Is(err, nartree.ErrBroken):
		return fmt.Errorf("%w: %w", errBroken, err)
	case err != nil:
		return fmt.Errorf("%w: %w", errDamaged, err)
	}
	if sum, _ := narSum(r.hash); opened.NarHash != sum {
		return brokenf("its recipe is of the NAR %s, not of %s", nix32.Encode(opened.NarHash[:]), r.hash)
	}
	if rec, ok := s.cache.Record(r.name); ok && rec.Root == r.root && rec.Checked {
		return nil
	}
	return s.rebuild(r, opened, hold)
}

// rebuild writes the NAR of opened into a hash and compares what comes
// out with the recipe. It stops when the store's context ends, and when
// the NAR grows beyond the length the recipe states.
func (s *Store) rebuild(r ref, opened nartree.Content, hold *packcache.Hold) error {
	s.log.Debug("rebuilding the archive to compare it with its recipe", "name", r.name, "bytes", opened.NarSize)
	started := time.Now()
	sum := sha256.New()
	out := &measured{w: &guarded{ctx: s.ctx, w: sum}, limit: opened.NarSize}
	err := nartree.WriteNAR(out, opened.Node, hold.Get)
	switch {
	case errors.Is(out.err, errTooLong):
		return brokenf("its NAR is longer than the %d bytes its recipe states", opened.NarSize)
	case out.err != nil:
		// The store's context ended.
		return out.err
	case errors.Is(err, nartree.ErrBroken):
		return fmt.Errorf("%w: %w", errBroken, err)
	case err != nil:
		return fmt.Errorf("%w: %w", errDamaged, err)
	case out.n != opened.NarSize:
		return brokenf("its NAR has %d bytes, its recipe states %d", out.n, opened.NarSize)
	}
	if got := [32]byte(sum.Sum(nil)); got != opened.NarHash {
		return brokenf("its NAR has the SHA-256 %s, its recipe states %s", nix32.Encode(got[:]), nix32.Encode(opened.NarHash[:]))
	}
	s.log.Debug("the archive is what its recipe states", "name", r.name, "took", time.Since(started).Round(time.Millisecond))
	return nil
}

// Content is a content reference that is local and checked, with a hold
// on its packs. Release ends the hold.
type Content struct {
	store    *Store
	ref      ref
	hold     *packcache.Hold
	node     nartree.Node
	narSize  uint64
	released atomic.Bool
}

// NarSize returns the length of the NAR, as its recipe states it and as
// the check found it.
func (c *Content) NarSize() uint64 { return c.narSize }

// WriteTo writes the NAR; it fails if the bytes written would differ
// from NarSize. It returns the number of bytes written to w.
//
// An error of w is returned as it is. Any other failure is a defect, of
// the cache or of narrenhaus, since the reference was checked: it is
// logged as one, the reference's pack is dropped from the cache, and w
// has received a beginning of the NAR and never a byte beyond NarSize.
// WriteTo does not buffer: w gets one Write per node and per chunk of a
// file.
func (c *Content) WriteTo(w io.Writer) (int64, error) {
	if c.released.Load() {
		return 0, fmt.Errorf("refs: %s is written after its release", c.ref.name)
	}
	out := &measured{w: w, limit: c.narSize}
	err := nartree.WriteNAR(out, c.node, c.hold.Get)
	switch {
	case out.err != nil && !errors.Is(out.err, errTooLong):
		return int64(out.n), out.err
	case err == nil && out.n == c.narSize:
		return int64(out.n), nil
	case c.released.Load():
		// Released under the write: the caller's doing.
		return int64(out.n), fmt.Errorf("refs: %s was released while it was written: %w", c.ref.name, err)
	}
	if err == nil {
		err = errors.New("the archive ended early")
	}
	err = fmt.Errorf("refs: defect: the NAR of %s is not the %d bytes that were checked; %d were written: %w",
		c.ref.name, c.narSize, out.n, err)
	c.store.log.Error("defect: a checked reference does not rebuild to its NAR; its pack is dropped from the cache",
		"name", c.ref.name, "root", c.ref.root.String(), "error", err.Error())
	c.store.cache.Drop(c.ref.root)
	return int64(out.n), err
}

// Release lets go of the packs. Calling it again does nothing.
func (c *Content) Release() {
	if c.released.Swap(true) {
		return
	}
	c.hold.Release()
}

// measured passes on what is written to it, counts it, and refuses the
// write that would take the count beyond limit: nothing of that write is
// passed on. err is the error it failed with.
type measured struct {
	w     io.Writer
	limit uint64
	n     uint64
	err   error
}

func (m *measured) Write(p []byte) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	if uint64(len(p)) > m.limit-m.n {
		m.err = errTooLong
		return 0, m.err
	}
	n, err := m.w.Write(p)
	m.n += uint64(n)
	if err != nil {
		m.err = err
	}
	return n, err
}

// guarded is a writer that fails once ctx is done: the way to end
// nartree.WriteNAR, which takes no context.
type guarded struct {
	ctx context.Context
	w   io.Writer
}

func (g *guarded) Write(p []byte) (int, error) {
	if err := g.ctx.Err(); err != nil {
		return 0, err
	}
	return g.w.Write(p)
}
