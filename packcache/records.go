package packcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/amber-store/core/key"
)

// Record is what the cache keeps per reference name: where the name
// pointed when it was last made local. It exists only while the pack of
// its Root is cached, and goes when that pack is evicted or dropped. The
// eviction of the parent leaves it.
type Record struct {
	Root    key.Key
	Packs   []key.Key // the roots of the packs it needs: its own first, then its parent
	Checked bool      // a content reference whose NAR was rebuilt and compared
}

// recordID is the SHA-256 of a reference name. Names contain slashes and
// are up to 1024 bytes long, so a record's file is named by this instead.
type recordID [sha256.Size]byte

const recordExt = ".json"

// recordFile is the content of a record's file.
type recordFile struct {
	Name    string   `json:"name"`
	Root    string   `json:"root"`
	Packs   []string `json:"packs"`
	Checked bool     `json:"checked"`
}

func (c *Cache) recordPath(id recordID) string {
	return filepath.Join(c.dir, refsName, hex.EncodeToString(id[:])+recordExt)
}

// Record returns the record of the reference name, and whether there is
// one.
func (c *Cache) Record(name string) (Record, bool) {
	id := recordID(sha256.Sum256([]byte(name)))
	c.mu.Lock()
	_, ok := c.records[id]
	c.mu.Unlock()
	if !ok {
		return Record{}, false
	}
	// The file is read outside the lock. If the record is replaced
	// meanwhile, this reads the one or the other whole; if it is deleted,
	// there is none.
	r, of, err := readRecord(c.recordPath(id))
	if err != nil || of != name {
		return Record{}, false
	}
	return r, true
}

// PutRecord writes the record of the reference name, in place of the one
// it has. A record whose Root has no pack in the cache is not kept: it
// would have gone with the pack.
func (c *Cache) PutRecord(name string, r Record) error {
	f := recordFile{Name: name, Root: r.Root.String(), Packs: make([]string, len(r.Packs)), Checked: r.Checked}
	for i, k := range r.Packs {
		f.Packs[i] = k.String()
	}
	b, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	if c.isClosed() {
		return errClosed
	}
	tmp, err := c.writeTemp(append(b, '\n'))
	if err != nil {
		return err
	}
	id := recordID(sha256.Sum256([]byte(name)))

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		os.Remove(tmp)
		return errClosed
	}
	if c.packs[r.Root] == nil {
		os.Remove(tmp)
		c.removeRecord(id)
		return nil
	}
	if err := os.Rename(tmp, c.recordPath(id)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("packcache: %w", err)
	}
	c.unlinkRecord(id)
	c.linkRecord(id, r.Root)
	return nil
}

// DeleteRecord removes the record of the reference name, if there is one.
func (c *Cache) DeleteRecord(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.removeRecord(recordID(sha256.Sum256([]byte(name))))
}

// linkRecord enters a record into the tables. The caller has c.mu.
func (c *Cache) linkRecord(id recordID, root key.Key) {
	c.records[id] = root
	ids := c.byRoot[root]
	if ids == nil {
		ids = make(map[recordID]struct{})
		c.byRoot[root] = ids
	}
	ids[id] = struct{}{}
}

// unlinkRecord takes a record out of the tables. The caller has c.mu.
func (c *Cache) unlinkRecord(id recordID) {
	root, ok := c.records[id]
	if !ok {
		return
	}
	delete(c.records, id)
	ids := c.byRoot[root]
	delete(ids, id)
	if len(ids) == 0 {
		delete(c.byRoot, root)
	}
}

// removeRecord deletes one record. The caller has c.mu.
func (c *Cache) removeRecord(id recordID) {
	if _, ok := c.records[id]; !ok {
		return
	}
	c.unlinkRecord(id)
	os.Remove(c.recordPath(id))
}

// removeRecords deletes the records whose root is root. The caller has
// c.mu.
func (c *Cache) removeRecords(root key.Key) {
	for id := range c.byRoot[root] {
		delete(c.records, id)
		os.Remove(c.recordPath(id))
	}
	delete(c.byRoot, root)
}

// readRecord reads a record's file and returns the record and the name it
// is of.
func readRecord(path string) (Record, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Record{}, "", err
	}
	var f recordFile
	if err := json.Unmarshal(b, &f); err != nil {
		return Record{}, "", err
	}
	r := Record{Packs: make([]key.Key, len(f.Packs)), Checked: f.Checked}
	if r.Root, err = parseKey(f.Root); err != nil {
		return Record{}, "", err
	}
	for i, s := range f.Packs {
		if r.Packs[i], err = parseKey(s); err != nil {
			return Record{}, "", err
		}
	}
	return r, f.Name, nil
}

// loadRecords fills the record tables from refs/ and removes the records
// that cannot be read or whose pack is not cached. Files that are not
// named as a record's are left alone. It runs in Open, after loadPacks.
func (c *Cache) loadRecords() error {
	dir := filepath.Join(c.dir, refsName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("packcache: %w", err)
	}
	for _, e := range entries {
		stem, isRecord := strings.CutSuffix(e.Name(), recordExt)
		if e.IsDir() || !isRecord || len(stem) != 2*sha256.Size {
			continue
		}
		path := filepath.Join(dir, e.Name())
		r, name, err := readRecord(path)
		id := recordID(sha256.Sum256([]byte(name)))
		if err != nil || hex.EncodeToString(id[:]) != stem || c.packs[r.Root] == nil {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("packcache: %w", err)
			}
			continue
		}
		c.linkRecord(id, r.Root)
	}
	return nil
}
