package packcache

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/amber-store/core/key"
)

// stamp marks p as used now: in the table, which orders eviction in this
// process, and as the modification time of its data file, which is what
// the next process goes by. Access time is left alone. The caller has c.mu.
func (c *Cache) stamp(p *pack) {
	if c.closed {
		return
	}
	now := time.Now()
	if !now.After(c.last) {
		// Two uses within the clock's resolution, or a clock set back:
		// the order of use is kept all the same.
		now = c.last.Add(time.Nanosecond)
	}
	c.last = now
	p.stamp = now
	// A stamp that cannot be written is one the next process does not
	// see; this one goes by the table.
	_ = os.Chtimes(c.packPath(p.root, dataExt), time.Time{}, now)
}

// evict deletes packs that nobody holds, the one stamped longest ago
// first, while the size is over the limit. If what is left is held, the
// cache stays over the limit until a hold is released. The caller has c.mu.
func (c *Cache) evict() {
	if c.closed || c.size <= c.limit {
		return
	}
	var loose []*pack
	for root, p := range c.packs {
		if c.reserved[root] == 0 {
			loose = append(loose, p)
		}
	}
	slices.SortFunc(loose, func(a, b *pack) int {
		return cmp.Or(a.stamp.Compare(b.stamp), bytes.Compare(a.root[:], b.root[:]))
	})
	for _, p := range loose {
		if c.size <= c.limit {
			return
		}
		c.remove(p)
	}
}

// remove takes p out of the table and deletes its files and the records
// whose root it is. If holds are reading it, the data and the index are
// moved under tmp/ and deleted when the last of them is released. The
// caller has c.mu.
func (c *Cache) remove(p *pack) {
	delete(c.packs, p.root)
	c.size -= p.size
	// The .meta goes first: what is left without it is no pack.
	os.Remove(c.packPath(p.root, metaExt))
	if p.holds == 0 {
		p.flat.remove()
	} else {
		c.parked++
		base := filepath.Join(c.TempDir(), fmt.Sprintf("dropped-%d-%s", c.parked, p.root))
		if err := p.flat.move(base+dataExt, base+idxExt); err != nil {
			// It cannot be kept for its readers: their reads fail.
			p.flat.remove()
		}
		p.parked.Store(true)
	}
	c.removeRecords(p.root)
}

// Drop deletes the pack cached under root and the records whose root it
// is. It waits for no one: holds that are reading the pack go on reading
// it, its files are deleted when the last of them is released, and a new
// pack can be placed under root in the meantime.
func (c *Cache) Drop(root key.Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if p := c.packs[root]; p != nil {
		c.remove(p)
	}
}
