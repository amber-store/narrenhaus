package packcache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/packfile"
)

// testServer is the RFC 8032 test-vector key, written as an endpoint ID is.
const testServer = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"

var testOrigin = Origin{Server: testServer, Prefix: "acme/"}

// roomy is a size limit no test reaches.
const roomy = 1 << 30

// kilo is the size of the one object of the packs the eviction tests place:
// such a pack counts for kiloPack bytes.
const (
	kilo     = 1000
	kiloPack = kilo + packfile.IndexHeaderSize + packfile.EntrySize
)

func open(t *testing.T, dir string, limit uint64) *Cache {
	t.Helper()
	c, err := Open(dir, limit, testOrigin)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// blob returns the key core gives data as a Blob.
func blob(t testing.TB, data []byte) key.Key {
	t.Helper()
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatalf("key.New: %v", err)
	}
	return k
}

// body returns size bytes that differ from seed to seed.
func body(seed string, size int) []byte {
	out := make([]byte, 0, size+sha256.Size)
	sum := sha256.Sum256([]byte(seed))
	for len(out) < size {
		out = append(out, sum[:]...)
		sum = sha256.Sum256(sum[:])
	}
	return out[:size]
}

// seal writes objects to a download file and seals it.
func seal(t *testing.T, c *Cache, objects ...[]byte) *Sealed {
	t.Helper()
	w, err := c.NewDownload()
	if err != nil {
		t.Fatalf("NewDownload: %v", err)
	}
	for _, o := range objects {
		if err := w.Add(blob(t, o), o); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	s, err := w.Seal()
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return s
}

// place puts a pack of objects into the cache under the key of the first of
// them and returns that root.
func place(t *testing.T, c *Cache, parent *key.Key, objects ...[]byte) key.Key {
	t.Helper()
	root := blob(t, objects[0])
	if err := c.Place(seal(t, c, objects...), root, parent); err != nil {
		t.Fatalf("Place: %v", err)
	}
	return root
}

// packSize is what a pack of objects counts for: its data and its index.
func packSize(objects ...[]byte) uint64 {
	size := packfile.IndexSize(uint64(len(objects)))
	for _, o := range objects {
		size += uint64(len(o))
	}
	return size
}

// names returns the names in a directory, sorted.
func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// packFiles returns the names a pack has under packs/.
func packFiles(roots ...key.Key) []string {
	var out []string
	for _, r := range roots {
		out = append(out, r.String()+".data", r.String()+".idx", r.String()+".meta")
	}
	slices.Sort(out)
	return out
}

func wantNames(t *testing.T, dir string, want ...string) {
	t.Helper()
	if got := names(t, dir); !slices.Equal(got, want) {
		t.Fatalf("%s holds %q, want %q", filepath.Base(dir), got, want)
	}
}

func wantPresent(t *testing.T, c *Cache, root key.Key, present bool) {
	t.Helper()
	if _, ok := c.Meta(root); ok != present {
		t.Fatalf("pack %s present = %v, want %v", root, ok, present)
	}
}

func wantSize(t *testing.T, c *Cache, want uint64) {
	t.Helper()
	if got := c.Size(); got != want {
		t.Fatalf("Size() = %d, want %d", got, want)
	}
}

// getter is what both a Sealed and a Hold are to a reader.
type getter interface {
	Get(key.Key) ([]byte, error)
}

func wantObject(t *testing.T, g getter, data []byte) {
	t.Helper()
	got, err := g.Get(blob(t, data))
	if err != nil {
		t.Fatalf("Get of a %d-byte object: %v", len(data), err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get of a %d-byte object returned other bytes (%d of them)", len(data), len(got))
	}
}

func wantAbsent(t *testing.T, g getter, data []byte) {
	t.Helper()
	if _, err := g.Get(blob(t, data)); !errors.Is(err, ErrAbsent) {
		t.Fatalf("Get of an object that is not there: %v, want ErrAbsent", err)
	}
}

func TestStagedObjectsReadBack(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	objects := [][]byte{body("a", 300), body("b", 70_000), {}, body("c", 1)}

	w, err := c.NewStaged()
	if err != nil {
		t.Fatalf("NewStaged: %v", err)
	}
	for _, o := range append(objects, objects[0], objects[2]) {
		if err := w.Add(blob(t, o), o); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if got := names(t, filepath.Join(dir, "staging")); len(got) != 1 {
		t.Fatalf("staging holds %q while the writer is open, want its one file", got)
	}
	wantNames(t, filepath.Join(dir, "tmp"))
	s, err := w.Seal()
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := w.Add(blob(t, []byte("late")), []byte("late")); err == nil {
		t.Fatal("Add after Seal succeeded")
	}

	var sum uint64
	for _, o := range objects {
		wantObject(t, s, o)
		if !s.Has(blob(t, o)) {
			t.Fatalf("Has is false for a %d-byte object that was added", len(o))
		}
		sum += uint64(len(o))
	}
	if s.Bytes() != sum {
		t.Fatalf("Bytes() = %d, want %d, each object counted once", s.Bytes(), sum)
	}
	other := []byte("never added")
	wantAbsent(t, s, other)
	if s.Has(blob(t, other)) {
		t.Fatal("Has is true for an object that was never added")
	}

	// A slice from Get is the caller's: writing to it changes nothing.
	got, err := s.Get(blob(t, objects[0]))
	if err != nil {
		t.Fatal(err)
	}
	clear(got)
	wantObject(t, s, objects[0])

	// The index beside the data is jaccard-store's, and tiles the data.
	staged := names(t, filepath.Join(dir, "staging"))
	if len(staged) != 2 || !strings.HasSuffix(staged[0], ".data") || !strings.HasSuffix(staged[1], ".idx") {
		t.Fatalf("staging holds %q, want one .data and one .idx", staged)
	}
	data, err := os.ReadFile(filepath.Join(dir, "staging", staged[0]))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Join(dir, "staging", staged[1]))
	if err != nil {
		t.Fatal(err)
	}
	index, err := packfile.ParseIndex(encoded)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if index.Len() != len(objects) {
		t.Fatalf("the index has %d entries, want %d", index.Len(), len(objects))
	}
	if index.DataSize() != uint64(len(data)) || index.DataSize() != sum {
		t.Fatalf("the index covers %d bytes, the data file has %d, the objects %d", index.DataSize(), len(data), sum)
	}
	for _, i := range index.ByOffset() {
		e := index.Entry(i)
		if k := blob(t, data[e.Offset:e.Offset+uint64(e.Length)]); k != e.Key {
			t.Fatalf("the bytes at offset %d are not the object %s the index names there", e.Offset, e.Key)
		}
	}

	s.Discard()
	s.Discard()
	wantNames(t, filepath.Join(dir, "staging"))
	if _, err := s.Get(blob(t, objects[0])); err == nil {
		t.Fatal("Get after Discard succeeded")
	}

	// A writer given up leaves nothing either.
	w, err = c.NewStaged()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(blob(t, objects[1]), objects[1]); err != nil {
		t.Fatal(err)
	}
	w.Discard()
	w.Discard()
	wantNames(t, filepath.Join(dir, "staging"))
}

func TestWriterRefusesAKeyThatIsNotCanonical(t *testing.T) {
	c := open(t, t.TempDir(), roomy)
	w, err := c.NewStaged()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Discard()
	var bad key.Key
	bad[key.Size-1] = 0xf0
	if err := w.Add(bad, []byte("x")); err == nil {
		t.Fatal("Add took a key of no defined type")
	}
}

func TestConcurrentGet(t *testing.T) {
	c := open(t, t.TempDir(), roomy)
	var objects [][]byte
	for i := range 300 {
		objects = append(objects, body(fmt.Sprint("object ", i), 1+i*37))
	}
	readAll := func(g getter) {
		t.Helper()
		var wg sync.WaitGroup
		for range 64 {
			wg.Go(func() {
				for _, i := range rand.Perm(len(objects)) {
					got, err := g.Get(blob(t, objects[i]))
					if err != nil {
						t.Errorf("Get: %v", err)
						return
					}
					if !bytes.Equal(got, objects[i]) {
						t.Errorf("Get returned other bytes for object %d", i)
						return
					}
				}
			})
		}
		wg.Wait()
	}

	s := seal(t, c, objects...)
	readAll(s)

	root := blob(t, objects[0])
	if err := c.Place(s, root, nil); err != nil {
		t.Fatalf("Place: %v", err)
	}
	h, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire of a placed pack failed")
	}
	defer h.Release()
	readAll(h)
}

func TestPlaceAndAcquire(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	objects := [][]byte{body("root", 500), body("leaf", 9000)}
	root := place(t, c, nil, objects...)

	if m, ok := c.Meta(root); !ok || !m.Complete {
		t.Fatalf("Meta = %+v, %v, want a complete pack", m, ok)
	}
	wantNames(t, filepath.Join(dir, "packs"), packFiles(root)...)
	wantNames(t, filepath.Join(dir, "tmp"))
	wantSize(t, c, packSize(objects...))

	if _, ok := c.Acquire(blob(t, []byte("no such root"))); ok {
		t.Fatal("Acquire of a root that is not cached succeeded")
	}
	h, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}
	for _, o := range objects {
		wantObject(t, h, o)
		if has, err := h.Has(blob(t, o)); err != nil || !has {
			t.Fatalf("Has = %v, %v, want true", has, err)
		}
	}
	wantAbsent(t, h, []byte("elsewhere"))
	if has, err := h.Has(blob(t, []byte("elsewhere"))); err != nil || has {
		t.Fatalf("Has of an object that is not there = %v, %v, want false", has, err)
	}
	h.Release()
	h.Release()
	if _, err := h.Get(root); err == nil {
		t.Fatal("Get through a released hold succeeded")
	}
	if _, err := h.Has(root); err == nil {
		t.Fatal("Has through a released hold succeeded")
	}

	// Placing under a root that is cached keeps what is there.
	if err := c.Place(seal(t, c, objects[0]), root, nil); err != nil {
		t.Fatalf("Place over a cached pack: %v", err)
	}
	wantNames(t, filepath.Join(dir, "tmp"))
	wantSize(t, c, packSize(objects...))

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c = open(t, dir, roomy)
	if m, ok := c.Meta(root); !ok || !m.Complete {
		t.Fatalf("after a second Open, Meta = %+v, %v, want a complete pack", m, ok)
	}
	wantSize(t, c, packSize(objects...))
	h, ok = c.Acquire(root)
	if !ok {
		t.Fatal("Acquire after a second Open failed")
	}
	defer h.Release()
	for _, o := range objects {
		wantObject(t, h, o)
	}
}

func TestPlaceTakesTheSealedFile(t *testing.T) {
	c := open(t, t.TempDir(), roomy)
	object := body("once", 100)
	s := seal(t, c, object)
	root := blob(t, object)
	if err := c.Place(s, root, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Place(s, root, nil); err == nil {
		t.Fatal("placing one sealed file twice succeeded")
	}
	if _, err := s.Get(root); err == nil {
		t.Fatal("Get from a sealed file that was placed succeeded")
	}
	// Discard after Place must not take the pack's files away.
	s.Discard()
	h, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}
	defer h.Release()
	wantObject(t, h, object)
}

func TestPatchReadsThroughItsParent(t *testing.T) {
	c := open(t, t.TempDir(), roomy)
	inParent, inPatch := body("only in the parent", 4000), body("only in the patch", 300)
	parent := place(t, c, nil, body("parent root", 100), inParent)
	patch := place(t, c, &parent, body("patch root", 100), inPatch)

	if m, ok := c.Meta(patch); !ok || m.Complete || m.Parent != parent {
		t.Fatalf("Meta of the patch = %+v, %v, want a patch of %s", m, ok, parent)
	}
	for _, roots := range [][]key.Key{{patch, parent}, {patch}} {
		h, ok := c.Acquire(roots...)
		if !ok {
			t.Fatalf("Acquire of %d roots failed with patch and parent cached", len(roots))
		}
		wantObject(t, h, inPatch)
		wantObject(t, h, inParent)
		wantAbsent(t, h, []byte("in neither"))
		h.Release()
	}

	c.Drop(parent)
	if _, ok := c.Acquire(patch); ok {
		t.Fatal("Acquire of a patch whose parent is absent succeeded")
	}
	if _, ok := c.Acquire(patch, parent); ok {
		t.Fatal("Acquire of a patch and its absent parent succeeded")
	}
	if n := len(c.reserved); n != 0 {
		t.Fatalf("%d roots are held after Acquire failed", n)
	}
	wantPresent(t, c, patch, true)

	// A pack that is itself a patch does not serve as a parent: it lacks
	// what its own parent holds.
	other := place(t, c, nil, body("other base", 100))
	if err := c.Place(seal(t, c, body("parent root", 100)), parent, &other); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Acquire(patch); ok {
		t.Fatal("Acquire of a patch whose parent is cached as a patch succeeded")
	}
}

func TestEvictionOrder(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, 2*kiloPack+kiloPack/2)
	record := func(name string, root key.Key) {
		t.Helper()
		if err := c.PutRecord(name, Record{Root: root, Packs: []key.Key{root}}); err != nil {
			t.Fatalf("PutRecord: %v", err)
		}
	}
	a := place(t, c, nil, body("a", kilo))
	record("acme/a.narinfo", a)
	b := place(t, c, nil, body("b", kilo))
	record("acme/b.narinfo", b)
	record("acme/b-again.narinfo", b)

	// Using a makes b the one used longest ago.
	h, ok := c.Acquire(a)
	if !ok {
		t.Fatal("Acquire failed")
	}
	h.Release()
	wantSize(t, c, 2*kiloPack)

	third := place(t, c, nil, body("c", kilo))
	wantPresent(t, c, a, true)
	wantPresent(t, c, b, false)
	wantPresent(t, c, third, true)
	wantSize(t, c, 2*kiloPack)
	wantNames(t, filepath.Join(dir, "packs"), packFiles(a, third)...)
	if _, ok := c.Record("acme/a.narinfo"); !ok {
		t.Fatal("the record of a pack that stayed is gone")
	}
	for _, name := range []string{"acme/b.narinfo", "acme/b-again.narinfo"} {
		if _, ok := c.Record(name); ok {
			t.Fatalf("the record %s outlived its pack", name)
		}
	}
	if got := names(t, filepath.Join(dir, "refs")); len(got) != 1 {
		t.Fatalf("refs holds %q, want the one record left", got)
	}

	// The stamp is the data file's modification time, and is all a later
	// process goes by: make the pack placed last the one used longest ago.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for root, age := range map[key.Key]time.Duration{a: time.Hour, third: 2 * time.Hour} {
		if err := os.Chtimes(filepath.Join(dir, "packs", root.String()+".data"), now, now.Add(-age)); err != nil {
			t.Fatal(err)
		}
	}
	c = open(t, dir, kiloPack)
	wantPresent(t, c, a, true)
	wantPresent(t, c, third, false)
	wantSize(t, c, kiloPack)
	wantNames(t, filepath.Join(dir, "packs"), packFiles(a)...)
}

func TestStampIsTheModificationTime(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	root := place(t, c, nil, body("stamped", kilo))
	path := filepath.Join(dir, "packs", root.String()+".data")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	h, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}
	defer h.Release()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if age := time.Since(info.ModTime()); age > time.Minute {
		t.Fatalf("after Acquire the data file was modified %v ago, want now", age)
	}
}

func TestHeldPackIsNotEvicted(t *testing.T) {
	c := open(t, t.TempDir(), kiloPack+kiloPack/2)
	a := place(t, c, nil, body("a", kilo))
	held, ok := c.Acquire(a)
	if !ok {
		t.Fatal("Acquire failed")
	}

	// A second pack that nobody holds goes at once, although the held one
	// was used longer ago.
	b := place(t, c, nil, body("b", kilo))
	wantPresent(t, c, a, true)
	wantPresent(t, c, b, false)
	wantSize(t, c, kiloPack)

	// With both held the cache stays over its limit.
	reserved := c.Reserve(b)
	b = place(t, c, nil, body("b", kilo))
	wantPresent(t, c, a, true)
	wantPresent(t, c, b, true)
	wantSize(t, c, 2*kiloPack)
	wantObject(t, held, body("a", kilo))

	held.Release()
	wantPresent(t, c, a, false)
	wantPresent(t, c, b, true)
	wantSize(t, c, kiloPack)

	reserved.Release()
	wantPresent(t, c, b, true)
}

func TestUsingAPatchStampsItsParent(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, 3*kiloPack+kiloPack/2)
	parent := place(t, c, nil, body("parent", kilo))
	patch := place(t, c, &parent, body("patch", kilo))
	other := place(t, c, nil, body("other", kilo))

	old := time.Now().Add(-time.Hour)
	parentData := filepath.Join(dir, "packs", parent.String()+".data")
	if err := os.Chtimes(parentData, old, old); err != nil {
		t.Fatal(err)
	}
	h, ok := c.Acquire(patch)
	if !ok {
		t.Fatal("Acquire failed")
	}
	h.Release()
	info, err := os.Stat(parentData)
	if err != nil {
		t.Fatal(err)
	}
	if age := time.Since(info.ModTime()); age > time.Minute {
		t.Fatalf("after its patch was used the parent's data file was modified %v ago, want now", age)
	}

	// The pack used longest ago is now the one that is neither.
	fourth := place(t, c, nil, body("fourth", kilo))
	wantPresent(t, c, other, false)
	wantPresent(t, c, parent, true)
	wantPresent(t, c, patch, true)
	wantPresent(t, c, fourth, true)
}

func TestPlacingAPatchStampsItsParent(t *testing.T) {
	c := open(t, t.TempDir(), 2*kiloPack+kiloPack/2)
	parent := place(t, c, nil, body("parent", kilo))
	other := place(t, c, nil, body("other", kilo))
	patch := place(t, c, &parent, body("patch", kilo))
	wantPresent(t, c, other, false)
	wantPresent(t, c, parent, true)
	wantPresent(t, c, patch, true)
}

func TestReserveKeepsAPlacedPack(t *testing.T) {
	c := open(t, t.TempDir(), kiloPack/2)

	// Without a reservation a pack over the limit is gone as it arrives.
	loose := place(t, c, nil, body("loose", kilo))
	wantPresent(t, c, loose, false)
	wantSize(t, c, 0)

	object := body("reserved", kilo)
	root := blob(t, object)
	h := c.Reserve(root)
	wantAbsent(t, h, object)
	place(t, c, nil, object)
	wantPresent(t, c, root, true)
	wantSize(t, c, kiloPack)
	wantObject(t, h, object)
	if has, err := h.Has(root); err != nil || !has {
		t.Fatalf("Has through the reservation = %v, %v, want true", has, err)
	}

	h.Release()
	wantPresent(t, c, root, false)
	wantSize(t, c, 0)
}

func TestDropWhileHeld(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	objects := [][]byte{body("root", 200), body("old only", 5000)}
	root := place(t, c, nil, objects...)
	if err := c.PutRecord("acme/nar/x.nar", Record{Root: root, Packs: []key.Key{root}}); err != nil {
		t.Fatal(err)
	}
	h, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}

	c.Drop(root)
	wantPresent(t, c, root, false)
	wantSize(t, c, 0)
	wantNames(t, filepath.Join(dir, "packs"))
	if _, ok := c.Acquire(root); ok {
		t.Fatal("Acquire of a dropped pack succeeded")
	}
	if _, ok := c.Record("acme/nar/x.nar"); ok {
		t.Fatal("the record outlived the drop of its pack")
	}
	for _, o := range objects {
		wantObject(t, h, o)
	}

	// The root can be cached anew while the old pack is still being read,
	// and the holder goes on reading the old one.
	if err := c.Place(seal(t, c, objects[0]), root, nil); err != nil {
		t.Fatalf("Place under a dropped root: %v", err)
	}
	wantPresent(t, c, root, true)
	for _, o := range objects {
		wantObject(t, h, o)
	}
	fresh, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire of the pack placed anew failed")
	}
	defer fresh.Release()
	wantObject(t, fresh, objects[0])
	wantAbsent(t, fresh, objects[1])

	h.Release()
	wantNames(t, filepath.Join(dir, "tmp"))
	wantNames(t, filepath.Join(dir, "packs"), packFiles(root)...)
	if _, err := h.Get(blob(t, objects[1])); err == nil {
		t.Fatal("Get through the released hold of a dropped pack succeeded")
	}

	// Dropping what nobody holds, or what is not there, is immediate.
	c.Drop(blob(t, []byte("never cached")))
	loose := place(t, c, nil, body("loose", 10))
	c.Drop(loose)
	wantNames(t, filepath.Join(dir, "packs"), packFiles(root)...)
}

// Two holds of one root, one of them released twice: the root is still
// held once.
func TestReleaseTwiceLetsGoOnce(t *testing.T) {
	c := open(t, t.TempDir(), kiloPack/2)
	object := body("held twice", kilo)
	root := blob(t, object)
	first, second := c.Reserve(root), c.Reserve(root)
	place(t, c, nil, object)

	first.Release()
	first.Release()
	wantPresent(t, c, root, true)
	wantObject(t, second, object)

	second.Release()
	wantPresent(t, c, root, false)

	// What a failed Acquire returns can be released as well.
	failed, ok := c.Acquire(root)
	if ok {
		t.Fatal("Acquire of an evicted pack succeeded")
	}
	failed.Release()
}

// The one who makes a reference local reserves its roots first and may
// then find that what is cached under one of them has to be fetched anew.
// What it reads through its reservation afterwards is the pack it placed,
// not the one it dropped.
func TestReservationFollowsItsRoot(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	shared, stale, fresh := body("root", 100), body("only in the stale pack", 2000), body("only in the fresh pack", 3000)
	parent := place(t, c, nil, body("parent", 100))
	root := place(t, c, &parent, shared, stale)

	h := c.Reserve(root)
	wantObject(t, h, stale)
	wantAbsent(t, h, fresh)
	inHand := h.bound(h.slots[0])

	c.Drop(root)
	wantObject(t, h, stale)
	if err := c.Place(seal(t, c, shared, fresh), root, nil); err != nil {
		t.Fatal(err)
	}
	wantObject(t, h, fresh)
	wantObject(t, h, shared)
	wantAbsent(t, h, stale)
	if has, err := h.Has(blob(t, fresh)); err != nil || !has {
		t.Fatalf("Has of an object of the pack placed anew = %v, %v, want true", has, err)
	}
	// A read that had the dropped pack in hand when the reservation moved
	// on still finds its file: it is kept until the release.
	if _, ok, err := inHand.flat.get(blob(t, stale)); err != nil || !ok {
		t.Fatalf("a read of the pack the reservation left = %v, %v, want the object", ok, err)
	}

	h.Release()
	wantNames(t, filepath.Join(dir, "tmp"))
	wantNames(t, filepath.Join(dir, "packs"), packFiles(parent, root)...)
}

// Reads through a reservation go on, without an error, while the pack
// under its root is dropped and placed anew again and again.
func TestReservationReadsWhileItsPackIsReplaced(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	object := body("in every pack", 4000)
	root := place(t, c, nil, object)
	h := c.Reserve(root)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := h.Get(root)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if !bytes.Equal(got, object) {
					t.Error("Get returned other bytes")
					return
				}
				if has, err := h.Has(root); err != nil || !has {
					t.Errorf("Has = %v, %v, want true", has, err)
					return
				}
			}
		})
	}
	for i := range 200 {
		c.Drop(root)
		if err := c.Place(seal(t, c, object, body(fmt.Sprint("version ", i), 10)), root, nil); err != nil {
			t.Errorf("Place: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()

	h.Release()
	wantNames(t, filepath.Join(dir, "tmp"))
	wantNames(t, filepath.Join(dir, "packs"), packFiles(root)...)
}

// A reservation reads a pack that was placed after it was made, and keeps
// reading it when the pack is dropped, as a hold taken with Acquire does.
func TestDropWhileReserved(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	object := body("placed later", 3000)
	root := blob(t, object)
	h := c.Reserve(root)
	place(t, c, nil, object)
	wantObject(t, h, object)

	c.Drop(root)
	wantPresent(t, c, root, false)
	wantNames(t, filepath.Join(dir, "packs"))
	wantObject(t, h, object)

	h.Release()
	wantNames(t, filepath.Join(dir, "tmp"))
}

func TestRecoveryAfterAKill(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	keptObject := body("kept", kilo)
	kept := place(t, c, nil, keptObject)
	noMeta := place(t, c, nil, body("without meta", kilo))
	short := place(t, c, nil, body("cut short", kilo))
	for name, root := range map[string]key.Key{"acme/kept.narinfo": kept, "acme/lost.narinfo": noMeta} {
		if err := c.PutRecord(name, Record{Root: root, Packs: []key.Key{root}}); err != nil {
			t.Fatal(err)
		}
	}
	// An upload cut off while it was written, and a download that was
	// sealed and never placed.
	w, err := c.NewStaged()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(blob(t, keptObject), keptObject); err != nil {
		t.Fatal(err)
	}
	seal(t, c, body("sealed", 10))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	packs := filepath.Join(dir, "packs")
	if err := os.Remove(filepath.Join(packs, noMeta.String()+".meta")); err != nil {
		t.Fatal(err)
	}
	// A file the system cut short when it went down counts as no pack too.
	if err := os.Truncate(filepath.Join(packs, short.String()+".data"), 10); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"tmp", "staging"} {
		if err := os.MkdirAll(filepath.Join(dir, sub, "left", "over"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, sub, "left", "over", "file"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	c = open(t, dir, roomy)
	wantNames(t, packs, packFiles(kept)...)
	wantNames(t, filepath.Join(dir, "tmp"))
	wantNames(t, filepath.Join(dir, "staging"))
	wantPresent(t, c, kept, true)
	wantPresent(t, c, noMeta, false)
	wantPresent(t, c, short, false)
	wantSize(t, c, kiloPack)
	h, ok := c.Acquire(kept)
	if !ok {
		t.Fatal("Acquire of the pack that was whole failed")
	}
	defer h.Release()
	wantObject(t, h, keptObject)
	if r, ok := c.Record("acme/kept.narinfo"); !ok || r.Root != kept {
		t.Fatalf("the record of the pack that was whole = %+v, %v", r, ok)
	}
	if _, ok := c.Record("acme/lost.narinfo"); ok {
		t.Fatal("the record of a pack removed at Open is still there")
	}
	if got := names(t, filepath.Join(dir, "refs")); len(got) != 1 {
		t.Fatalf("refs holds %q, want one record", got)
	}
}

func TestOrigin(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	root := place(t, c, nil, body("ours", 10))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	otherServer := strings.Repeat("ab", 32)
	for _, other := range []Origin{
		{Server: otherServer, Prefix: testOrigin.Prefix},
		{Server: testOrigin.Server, Prefix: "globex/"},
	} {
		_, err := Open(dir, roomy, other)
		if !errors.Is(err, ErrOrigin) {
			t.Fatalf("Open with %+v: %v, want ErrOrigin", other, err)
		}
		for _, want := range []string{dir, testOrigin.Server, testOrigin.Prefix, other.Server, other.Prefix} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the message %q does not contain %q", err, want)
			}
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("the message %q is more than one line", err)
		}
	}

	// A refused Open touched nothing and holds nothing.
	c = open(t, dir, roomy)
	wantPresent(t, c, root, true)
}

func TestOpenRefusesADirectoryInUse(t *testing.T) {
	dir := t.TempDir()
	first := open(t, dir, roomy)
	root := place(t, first, nil, body("first", 10))

	_, err := Open(dir, roomy, testOrigin)
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("a second Open of a directory in use: %v, want ErrInUse", err)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("the message %q does not name the directory %s", err, dir)
	}
	// The refused Open left the first cache as it was.
	wantNames(t, filepath.Join(dir, "packs"), packFiles(root)...)
	h, ok := first.Acquire(root)
	if !ok {
		t.Fatal("the first cache lost its pack")
	}
	h.Release()

	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	second, err := Open(dir, roomy, testOrigin)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	defer second.Close()
	wantPresent(t, second, root, true)
}

func TestClosedCacheChangesNothing(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	root := place(t, c, nil, body("kept", 10))
	s := seal(t, c, body("late", 10))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := c.NewStaged(); err == nil {
		t.Error("NewStaged on a closed cache succeeded")
	}
	if _, err := c.NewDownload(); err == nil {
		t.Error("NewDownload on a closed cache succeeded")
	}
	if err := c.Place(s, blob(t, body("late", 10)), nil); err == nil {
		t.Error("Place on a closed cache succeeded")
	}
	if err := c.PutRecord("acme/x.narinfo", Record{Root: root}); err == nil {
		t.Error("PutRecord on a closed cache succeeded")
	}
	if _, ok := c.Acquire(root); ok {
		t.Error("Acquire on a closed cache succeeded")
	}
	c.Drop(root)
	wantNames(t, filepath.Join(dir, "packs"), packFiles(root)...)
}

func TestOpenRecreatesItsDirectories(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	root := place(t, c, nil, body("kept", 10))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"tmp", "refs"} {
		if err := os.RemoveAll(filepath.Join(dir, sub)); err != nil {
			t.Fatal(err)
		}
	}

	c = open(t, dir, roomy)
	for _, sub := range []string{"packs", "refs", "staging", "tmp"} {
		if info, err := os.Stat(filepath.Join(dir, sub)); err != nil || !info.IsDir() {
			t.Fatalf("after Open, %s: %v", sub, err)
		}
	}
	if c.TempDir() != filepath.Join(dir, "tmp") {
		t.Fatalf("TempDir() = %q, want %q", c.TempDir(), filepath.Join(dir, "tmp"))
	}
	wantPresent(t, c, root, true)
	if err := c.PutRecord("acme/x.narinfo", Record{Root: root}); err != nil {
		t.Fatalf("PutRecord into the recreated refs: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if os.Getuid() == 0 {
		t.Skip("root writes to read-only directories")
	}
	readOnly := func(path string) {
		t.Helper()
		if err := os.Chmod(path, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(path, 0o700) })
	}

	// The directory itself read-only, and tmp/ gone: it cannot be made.
	if err := os.Remove(filepath.Join(dir, "tmp")); err != nil {
		t.Fatal(err)
	}
	readOnly(dir)
	_, err := Open(dir, roomy, testOrigin)
	if err == nil {
		t.Fatal("Open of a read-only directory without tmp/ succeeded")
	}
	if !strings.Contains(err.Error(), filepath.Join(dir, "tmp")) || strings.Contains(err.Error(), "\n") {
		t.Fatalf("the error %q does not name %s on one line", err, filepath.Join(dir, "tmp"))
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	// Everything in place, but packs/ cannot be written.
	packs := filepath.Join(dir, "packs")
	readOnly(packs)
	_, err = Open(dir, roomy, testOrigin)
	if err == nil {
		t.Fatal("Open with a read-only packs/ succeeded")
	}
	if !strings.Contains(err.Error(), packs) {
		t.Fatalf("the error %q does not name %s", err, packs)
	}
	if err := os.Chmod(packs, 0o700); err != nil {
		t.Fatal(err)
	}

	// Neither refusal kept the lock or lost the pack.
	c = open(t, dir, roomy)
	wantPresent(t, c, root, true)
	wantNames(t, packs, packFiles(root)...)
}

func TestOpenCreatesTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not", "yet", "there")
	c := open(t, dir, roomy)
	wantSize(t, c, 0)
	root := place(t, c, nil, body("first", 10))
	wantPresent(t, c, root, true)
}

func TestRecords(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	own := place(t, c, nil, body("own", 10))
	parent := place(t, c, nil, body("parent", 10))
	const name = "acme/nar/0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73.nar"

	if _, ok := c.Record(name); ok {
		t.Fatal("a record that was never put is there")
	}
	want := Record{Root: own, Packs: []key.Key{own, parent}, Checked: true}
	if err := c.PutRecord(name, want); err != nil {
		t.Fatalf("PutRecord: %v", err)
	}
	check := func(c *Cache, want Record) {
		t.Helper()
		got, ok := c.Record(name)
		if !ok {
			t.Fatal("the record is not there")
		}
		if got.Root != want.Root || !slices.Equal(got.Packs, want.Packs) || got.Checked != want.Checked {
			t.Fatalf("Record = %+v, want %+v", got, want)
		}
	}
	check(c, want)
	if _, ok := c.Record(name + "x"); ok {
		t.Fatal("a record under another name is there")
	}
	sum := sha256.Sum256([]byte(name))
	wantNames(t, filepath.Join(dir, "refs"), hex.EncodeToString(sum[:])+".json")
	wantNames(t, filepath.Join(dir, "tmp"))

	// Records are kept across runs.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = open(t, dir, roomy)
	check(c, want)

	// Putting again replaces, also with another root.
	want = Record{Root: parent, Packs: []key.Key{parent}}
	if err := c.PutRecord(name, want); err != nil {
		t.Fatal(err)
	}
	check(c, want)
	c.Drop(own)
	check(c, want)
	if got := names(t, filepath.Join(dir, "refs")); len(got) != 1 {
		t.Fatalf("refs holds %q, want one record", got)
	}

	c.DeleteRecord(name)
	c.DeleteRecord(name)
	if _, ok := c.Record(name); ok {
		t.Fatal("the record outlived DeleteRecord")
	}
	wantNames(t, filepath.Join(dir, "refs"))

	// A record goes with the pack that is its root.
	if err := c.PutRecord(name, want); err != nil {
		t.Fatal(err)
	}
	c.Drop(parent)
	if _, ok := c.Record(name); ok {
		t.Fatal("the record outlived its pack")
	}
	wantNames(t, filepath.Join(dir, "refs"))

	// And one whose pack is not cached is not kept at all: it would have
	// gone with the pack.
	if err := c.PutRecord(name, want); err != nil {
		t.Fatalf("PutRecord for a root that is not cached: %v", err)
	}
	if _, ok := c.Record(name); ok {
		t.Fatal("a record without its pack was kept")
	}
	wantNames(t, filepath.Join(dir, "refs"))
	wantNames(t, filepath.Join(dir, "tmp"))
}

// randomEntries returns n entries that tile a stream, with keys core would
// accept. prefix, if not nil, is given to the leading bytes of every key.
func randomEntries(t *testing.T, r *rand.Rand, n int, prefix []byte) []packfile.Entry {
	t.Helper()
	entries := make([]packfile.Entry, 0, n)
	seen := map[key.Key]bool{}
	var offset uint64
	for len(entries) < n {
		var hash [key.Size]byte
		for i := range hash {
			hash[i] = byte(r.Uint32())
		}
		length := r.Uint32N(5000)
		k, err := key.NewFromHash(key.Blob, uint64(length), hash)
		if err != nil {
			t.Fatal(err)
		}
		copy(k[:], prefix)
		if seen[k] {
			continue
		}
		seen[k] = true
		entries = append(entries, packfile.Entry{Key: k, Offset: offset, Length: length})
		offset += uint64(length)
	}
	return entries
}

// The index is searched where it lies, a few entries read at a time. The
// search must find what packfile's own, in memory, finds, whatever the
// number of entries and however the keys are spread.
func TestFindReadsTheIndexByPosition(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	dir := t.TempDir()
	type shape struct {
		n      int
		prefix []byte
	}
	var shapes []shape
	for _, n := range []int{0, 1, 2, window - 1, window, window + 1, 2*window + 1, 1000, 20_000} {
		shapes = append(shapes, shape{n: n})
	}
	// Keys that share their leading bytes give the search nothing to
	// estimate a position from.
	shapes = append(shapes, shape{3000, bytes.Repeat([]byte{0x77}, 8)}, shape{3000, bytes.Repeat([]byte{0xff}, 12)}, shape{3000, make([]byte, 9)})

	for i, sh := range shapes {
		entries := randomEntries(t, r, sh.n, sh.prefix)
		index, err := packfile.NewIndex(entries)
		if err != nil {
			t.Fatalf("NewIndex of %d entries: %v", sh.n, err)
		}
		dataPath := filepath.Join(dir, fmt.Sprint(i, ".data"))
		idxPath := filepath.Join(dir, fmt.Sprint(i, ".idx"))
		if err := os.WriteFile(idxPath, index.Encode(), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(dataPath, int64(index.DataSize())); err != nil {
			t.Fatal(err)
		}
		f, err := openFiles(dataPath, idxPath)
		if err != nil {
			t.Fatalf("openFiles: %v", err)
		}
		for _, want := range entries {
			got, ok, err := f.find(want.Key)
			if err != nil || !ok || got != want {
				t.Fatalf("%d entries, prefix %x: find(%s) = %+v, %v, %v, want %+v", sh.n, sh.prefix, want.Key, got, ok, err, want)
			}
		}
		absent := randomEntries(t, r, 300, sh.prefix)
		absent = append(absent, randomEntries(t, r, 300, nil)...)
		absent = append(absent, packfile.Entry{}, packfile.Entry{Key: key.Key(bytes.Repeat([]byte{0xff}, key.Size))})
		for _, e := range absent {
			if index.Has(e.Key) {
				continue
			}
			if _, ok, err := f.find(e.Key); err != nil || ok {
				t.Fatalf("%d entries, prefix %x: find of a key that is not there = %v, %v", sh.n, sh.prefix, ok, err)
			}
		}
		f.close()
	}
}

// A damaged index or data file is an error of the read, not a wrong answer
// and not an allocation of whatever the index asks for.
func TestDamagedFilesAreErrors(t *testing.T) {
	dir := t.TempDir()
	c := open(t, dir, roomy)
	objects := [][]byte{body("root", 100), body("leaf", 5000)}
	root := place(t, c, nil, objects...)
	h, ok := c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}
	defer h.Release()
	wantObject(t, h, objects[1])

	// The files are opened anew by a hold taken after this one's are closed.
	h.Release()
	c.closeFiles()
	if err := os.Truncate(filepath.Join(dir, "packs", root.String()+".data"), 50); err != nil {
		t.Fatal(err)
	}
	h, ok = c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}
	defer h.Release()
	for _, o := range objects {
		if _, err := h.Get(blob(t, o)); err == nil || errors.Is(err, ErrAbsent) {
			t.Fatalf("Get from a data file cut short: %v, want an error that is not ErrAbsent", err)
		}
	}

	h.Release()
	c.closeFiles()
	if err := os.Truncate(filepath.Join(dir, "packs", root.String()+".idx"), 30); err != nil {
		t.Fatal(err)
	}
	h, ok = c.Acquire(root)
	if !ok {
		t.Fatal("Acquire failed")
	}
	defer h.Release()
	if _, err := h.Get(blob(t, objects[0])); err == nil || errors.Is(err, ErrAbsent) {
		t.Fatalf("Get through an index cut short: %v, want an error that is not ErrAbsent", err)
	}
	if _, err := h.Has(blob(t, objects[0])); err == nil {
		t.Fatal("Has through an index cut short gave an answer")
	}
}

// Thousands of uploads can be staged at once, and each is two files: they
// are open only while they are read, and no more than maxOpen of them at a
// time.
func TestOpenFilesAreBounded(t *testing.T) {
	c := open(t, t.TempDir(), roomy)
	const extra = 20
	var staged []*Sealed
	var objects [][]byte
	for i := range maxOpen + extra {
		o := body(fmt.Sprint("staged ", i), 64)
		objects = append(objects, o)
		staged = append(staged, seal(t, c, o))
	}
	openNow := func() int {
		c.openMu.Lock()
		defer c.openMu.Unlock()
		return len(c.open)
	}
	if n := openNow(); n != 0 {
		t.Fatalf("%d files are open before any was read", n)
	}
	for range 2 {
		for i, s := range staged {
			wantObject(t, s, objects[i])
			if n := openNow(); n > maxOpen {
				t.Fatalf("%d files are open, the limit is %d", n, maxOpen)
			}
		}
	}
	if n := openNow(); n != maxOpen {
		t.Fatalf("%d files are open after reading more than the limit of %d", n, maxOpen)
	}
	for _, s := range staged {
		s.Discard()
	}
	if n := openNow(); n != 0 {
		t.Fatalf("%d files are open after all were discarded", n)
	}
}
