package packcache

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/amber-store/core/key"
)

// Hold keeps the packs of some roots from being evicted, and reads them.
// It holds roots, not files: whatever is cached under a held root, now or
// later, stays until the hold is released.
//
// What it reads depends on how it was taken. A hold from Acquire reads the
// packs it found, to its end: if one of them is dropped it stays readable
// through the hold, and a pack placed under the same root meanwhile is not
// read by it. A hold from Reserve follows its roots: it reads the pack
// cached under each of them at the time of the read, so one that was
// placed after the reservation, or in place of a pack that was dropped, is
// read as soon as it is there. Until then a dropped pack stays readable
// through a reservation too.
//
// Get and Has are safe for concurrent calls. Release ends the hold; it
// belongs after the last read, not beside it.
type Hold struct {
	c        *Cache
	slots    []*slot
	follows  bool // taken with Reserve
	released atomic.Bool

	// past are the packs a reservation read before others took their
	// places. They are let go of with the rest, so that a read under way
	// never loses its file. Guarded by Cache.mu.
	past []*pack
}

// slot is one root of a hold, with the pack the hold reads for it once
// there is one.
type slot struct {
	root key.Key
	pack atomic.Pointer[pack]
}

var errReleased = errors.New("packcache: read through a hold that was released")

// Acquire holds the packs cached under roots, a reference's own pack
// first and then its parent, and stamps them. The parent of a patch among
// them is held and stamped with it whether it was named or not. ok is
// false, and nothing is held, unless every pack is cached and every
// patch's parent is cached as a complete pack: a patch is of no use
// without what its parent holds.
func (c *Cache) Acquire(roots ...key.Key) (h *Hold, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false
	}
	list := distinct(roots)
	var parents []key.Key
	for _, root := range list {
		p := c.packs[root]
		if p == nil {
			return nil, false
		}
		if p.meta.Complete {
			continue
		}
		if parent := c.packs[p.meta.Parent]; parent == nil || !parent.meta.Complete {
			return nil, false
		}
		parents = append(parents, p.meta.Parent)
	}
	return c.hold(distinct(append(list, parents...)), false), true
}

// Reserve holds roots whether or not packs are cached under them, so that
// a pack placed for one of them is not evicted before the holder has used
// it. The packs that are cached are stamped. Unlike Acquire it holds the
// roots it is given and no others, and reads what is cached under them at
// the time of each read (see Hold).
func (c *Cache) Reserve(roots ...key.Key) *Hold {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hold(distinct(roots), true)
}

// distinct returns roots without repeats, in their order.
func distinct(roots []key.Key) []key.Key {
	out := make([]key.Key, 0, len(roots))
	for _, root := range roots {
		if !slices.Contains(out, root) {
			out = append(out, root)
		}
	}
	return out
}

// hold makes a hold of roots, which are distinct. The caller has c.mu.
func (c *Cache) hold(roots []key.Key, follows bool) *Hold {
	h := &Hold{c: c, slots: make([]*slot, len(roots)), follows: follows}
	for i, root := range roots {
		s := &slot{root: root}
		c.reserved[root]++
		// Stamped in order, a parent comes out newer than its patch and
		// is evicted after it.
		if p := c.packs[root]; p != nil {
			p.holds++
			s.pack.Store(p)
			c.stamp(p)
		}
		h.slots[i] = s
	}
	return h
}

// bound returns the pack h reads for s, or nil while it has none.
func (h *Hold) bound(s *slot) *pack {
	p := s.pack.Load()
	if !h.follows || (p != nil && !p.parked.Load()) {
		return p
	}
	// A reservation without a pack for this root, or with one that was
	// dropped: it reads the pack that is cached under the root now, if
	// there is one.
	c := h.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.released.Load() {
		return nil
	}
	now := c.packs[s.root]
	if now == nil || now == s.pack.Load() {
		return s.pack.Load()
	}
	if p = s.pack.Load(); p != nil {
		h.past = append(h.past, p)
	}
	now.holds++
	s.pack.Store(now)
	return now
}

// Get returns the object k in a slice of the caller's own, from the first
// of the held packs that has it: a reference's own pack, then its parent.
// The error wraps ErrAbsent if none has it, and is another one if a pack
// could not be read.
func (h *Hold) Get(k key.Key) ([]byte, error) {
	if h.released.Load() {
		return nil, errReleased
	}
	for _, s := range h.slots {
		p := h.bound(s)
		if p == nil {
			continue
		}
		data, ok, err := p.flat.get(k)
		if err != nil {
			return nil, err
		}
		if ok {
			return data, nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrAbsent, k)
}

// Has reports whether one of the held packs has the object k.
func (h *Hold) Has(k key.Key) (bool, error) {
	if h.released.Load() {
		return false, errReleased
	}
	for _, s := range h.slots {
		p := h.bound(s)
		if p == nil {
			continue
		}
		ok, err := p.flat.has(k)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

// Release ends the hold and runs eviction. Calling it again, or on the nil
// hold of a failed Acquire, does nothing.
func (h *Hold) Release() {
	if h == nil {
		return
	}
	c := h.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.released.Swap(true) {
		return
	}
	for _, s := range h.slots {
		if c.reserved[s.root]--; c.reserved[s.root] == 0 {
			delete(c.reserved, s.root)
		}
		if p := s.pack.Load(); p != nil {
			c.unbind(p)
		}
	}
	for _, p := range h.past {
		c.unbind(p)
	}
	h.past = nil
	c.evict()
}

// unbind ends one hold's reading of p. The caller has c.mu.
func (c *Cache) unbind(p *pack) {
	if p.holds--; p.holds > 0 || !p.parked.Load() {
		return
	}
	// Dropped while it was read, and this was its last reader. A closed
	// cache deletes nothing: Open empties tmp/.
	if c.closed {
		p.flat.close()
	} else {
		p.flat.remove()
	}
}
