package nartree

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/amber-store/core/chunkers"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/verify"

	"github.com/amber-store/narrenhaus/nar"
)

// event is one call of a nar.Handler, or of a nar.Writer.
type event struct {
	op     string // "dir", "leave", "file" or "link"
	name   string
	exec   bool
	body   string
	target string
}

func dir(name string) event          { return event{op: "dir", name: name} }
func leave() event                   { return event{op: "leave"} }
func file(name, body string) event   { return event{op: "file", name: name, body: body} }
func script(name, body string) event { return event{op: "file", name: name, exec: true, body: body} }
func link(name, target string) event { return event{op: "link", name: name, target: target} }

// roundTripArchives is the table of canonical archives of package nar's
// round trip, copied from its test.
var roundTripArchives = []struct {
	name   string
	events []event
}{
	{"directory root", []event{
		dir(""),
		file("empty", ""),
		file("file", "plain\n"),
		link("link", "file"),
		dir("nothing"),
		leave(),
		script("run", "#!/bin/sh\n"),
		dir("sub"),
		dir("deeper"),
		link("up", "../.."),
		leave(),
		file("eight", "12345678"),
		file("inner", "x"),
		leave(),
		leave(),
	}},
	{"empty directory root", []event{dir(""), leave()}},
	{"file root", []event{file("", "hello\n")}},
	{"empty file root", []event{file("", "")}},
	{"executable file root", []event{script("", "#!/bin/sh\nexit 0\n")}},
	{"symlink root", []event{link("", "/nix/store/0000-target")}},
	{"names in byte order", []event{
		dir(""),
		file("B", "upper"),
		file("a", "lower"),
		file("é", "accent"),
		leave(),
	}},
	{"names that are not UTF-8", []event{
		dir(""),
		file("\x80", "a"),
		link("\xfe\xff", "\xff"),
		leave(),
	}},
}

// replay makes the calls of events on w, without closing it.
func replay(w *nar.Writer, events []event) error {
	for i, e := range events {
		var err error
		switch e.op {
		case "dir":
			err = w.Directory(e.name)
		case "leave":
			err = w.Leave()
		case "file":
			var body io.Writer
			if body, err = w.Regular(e.name, e.exec, uint64(len(e.body))); err == nil {
				_, err = io.WriteString(body, e.body)
			}
		case "link":
			err = w.Symlink(e.name, e.target)
		default:
			panic("unknown event " + e.op)
		}
		if err != nil {
			return fmt.Errorf("event %d (%s %q): %w", i, e.op, e.name, err)
		}
	}
	return nil
}

// write returns the archive a nar.Writer makes of events.
func write(t *testing.T, events []event) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := nar.NewWriter(&buf)
	if err := replay(w, events); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	return buf.Bytes()
}

// narString is one string of the archive format.
func narString(s string) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(len(s)))
	b = append(b, s...)
	for len(b)%8 != 0 {
		b = append(b, 0)
	}
	return b
}

// fileArchive returns a reader of the archive whose root is a file of
// size bytes read from body. The archive is never held whole.
func fileArchive(body io.Reader, size int64) io.Reader {
	var head []byte
	for _, s := range []string{"nix-archive-1", "(", "type", "regular", "contents"} {
		head = append(head, narString(s)...)
	}
	head = binary.LittleEndian.AppendUint64(head, uint64(size))
	tail := append(make([]byte, -size&7), narString(")")...)
	return io.MultiReader(bytes.NewReader(head), io.LimitReader(body, size), bytes.NewReader(tail))
}

// noise is an endless stream of pseudo-random bytes that depends on its
// seed alone: not on the Go release, and not on the sizes of the reads.
// It is SplitMix64, each word little-endian.
type noise struct {
	state uint64
	word  [8]byte
	left  int
}

func newNoise(seed uint64) *noise { return &noise{state: seed} }

func (n *noise) Read(p []byte) (int, error) {
	for i := range p {
		if n.left == 0 {
			n.state += 0x9e3779b97f4a7c15
			z := n.state
			z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
			z = (z ^ z>>27) * 0x94d049bb133111eb
			z ^= z >> 31
			binary.LittleEndian.PutUint64(n.word[:], z)
			n.left = 8
		}
		p[i] = n.word[8-n.left]
		n.left--
	}
	return len(p), nil
}

// noiseBytes returns the first n bytes of the noise of seed.
func noiseBytes(seed uint64, n int) []byte {
	b := make([]byte, n)
	newNoise(seed).Read(b)
	return b
}

// objects is a store of objects in memory. It keeps the slices it is
// given, as a consumer of fstree.Emit may.
type objects struct {
	mu sync.Mutex
	m  map[key.Key][]byte
	// gets counts the calls of get.
	gets int
}

func newObjects() *objects { return &objects{m: map[key.Key][]byte{}} }

func (o *objects) emit(obj fstree.Object) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.m[obj.Key] = obj.Bytes
	return nil
}

var errAbsent = errors.New("the object is absent")

func (o *objects) get(k key.Key) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.gets++
	b, ok := o.m[k]
	if !ok {
		return nil, fmt.Errorf("%s: %w", k, errAbsent)
	}
	return bytes.Clone(b), nil
}

// keys returns the keys held, sorted.
func (o *objects) keys() []key.Key {
	keys := make([]key.Key, 0, len(o.m))
	for k := range o.m {
		keys = append(keys, k)
	}
	sortKeys(keys)
	return keys
}

func sortKeys(keys []key.Key) {
	slices.SortFunc(keys, func(a, b key.Key) int { return bytes.Compare(a[:], b[:]) })
}

// blob stores data as a Blob and returns its key.
func (o *objects) blob(t *testing.T, data string) key.Key {
	t.Helper()
	obj, err := fstree.EncodeBlob([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	o.emit(obj)
	return obj.Key
}

// directory stores a directory of entries, built as core builds one, and
// returns its key.
func (o *objects) directory(t *testing.T, entries ...fstree.Entry) key.Key {
	t.Helper()
	b := fstree.NewDirBuilder(chunkers.NewItemChunker(ingest.DefaultItemBits))
	for _, e := range entries {
		if err := b.AddEntry(o.emit, e); err != nil {
			t.Fatal(err)
		}
	}
	k, err := b.Finish(o.emit)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// leaf stores entries as one DirLeaf exactly as given, in whatever order
// they are, and returns its key.
func (o *objects) leaf(t *testing.T, entries ...fstree.Entry) key.Key {
	t.Helper()
	obj, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	o.emit(obj)
	return obj.Key
}

// raw stores data under a key of type typ and length field length.
func (o *objects) raw(t *testing.T, typ key.Type, length uint64, data []byte) key.Key {
	t.Helper()
	k, err := key.New(typ, length, data)
	if err != nil {
		t.Fatal(err)
	}
	o.emit(fstree.Object{Key: k, Bytes: data})
	return k
}

// The entries of the convention of the design's section 4.2, written out
// here independently of the package's own constants.
func regularEntry(name string, k key.Key) fstree.Entry {
	return fstree.Entry{Name: []byte(name), Mode: 0o100444, Mtime: 1_000_000_000, ContentKey: k[:]}
}

func executableEntry(name string, k key.Key) fstree.Entry {
	return fstree.Entry{Name: []byte(name), Mode: 0o100555, Mtime: 1_000_000_000, ContentKey: k[:]}
}

func directoryEntry(name string, k key.Key) fstree.Entry {
	return fstree.Entry{Name: []byte(name), Mode: 0o040555, Mtime: 1_000_000_000, ContentKey: k[:]}
}

func symlinkEntry(name, target string) fstree.Entry {
	return fstree.Entry{Name: []byte(name), Mode: 0o120777, Mtime: 1_000_000_000, LinkTarget: []byte(target)}
}

// ingested is a NAR turned into a content reference in memory.
type ingested struct {
	store   *objects
	node    Node
	root    key.Key
	narHash [32]byte
	narSize uint64
}

// ingestArchive ingests archive and builds the reference's root.
func ingestArchive(t *testing.T, archive []byte) ingested {
	t.Helper()
	in := ingested{store: newObjects()}
	var err error
	in.node, in.narHash, in.narSize, err = Ingest(bytes.NewReader(archive), in.store.emit, MaxNARSize)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	in.root, err = Root(in.node, in.narHash, in.narSize, in.store.emit)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	return in
}

// checkReference checks that the objects of in are exactly those
// reachable from its root, that each is what its key says, and that the
// reference gives archive back.
func checkReference(t *testing.T, in ingested, archive []byte) {
	t.Helper()
	if want := sha256.Sum256(archive); in.narHash != want {
		t.Errorf("NarHash %x, want %x", in.narHash, want)
	}
	if in.narSize != uint64(len(archive)) {
		t.Errorf("NarSize %d, want %d", in.narSize, len(archive))
	}

	reachable, err := fstree.ReachableKeys(in.root, in.store.get)
	if err != nil {
		t.Fatalf("ReachableKeys: %v", err)
	}
	sortKeys(reachable)
	if held := in.store.keys(); !slices.Equal(held, reachable) {
		t.Errorf("%d objects were emitted and %d are reachable from the root", len(held), len(reachable))
	}
	// What a verifying server checks of each object.
	for k, data := range in.store.m {
		if err := verify.Object(k, data); err != nil {
			t.Errorf("object %s: %v", k, err)
		}
	}

	content, err := Open(in.root, in.store.get)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if content.NarHash != in.narHash || content.NarSize != in.narSize {
		t.Errorf("Open gives hash %x and size %d, ingest gave %x and %d", content.NarHash, content.NarSize, in.narHash, in.narSize)
	}
	if !sameEntry(content.Node, in.node) {
		t.Errorf("Open gives the node %+v, ingest gave %+v", content.Node, in.node)
	}
	var out bytes.Buffer
	if err := WriteNAR(&out, content.Node, in.store.get); err != nil {
		t.Fatalf("WriteNAR: %v", err)
	}
	if !bytes.Equal(out.Bytes(), archive) {
		t.Fatalf("the archive written differs from the one read: %d bytes, want %d", out.Len(), len(archive))
	}
}

// sameEntry compares the fields of two entries that the convention uses.
func sameEntry(a, b fstree.Entry) bool {
	return bytes.Equal(a.Name, b.Name) && a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID &&
		a.Mtime == b.Mtime && bytes.Equal(a.ContentKey, b.ContentKey) && bytes.Equal(a.LinkTarget, b.LinkTarget) &&
		len(a.Rdev) == 0 && len(b.Rdev) == 0 && len(a.XattrsIn) == 0 && len(b.XattrsIn) == 0 &&
		len(a.XattrsKey) == 0 && len(b.XattrsKey) == 0
}

func TestRoundTrip(t *testing.T) {
	for _, tc := range roundTripArchives {
		t.Run(tc.name, func(t *testing.T) {
			archive := write(t, tc.events)
			in := ingestArchive(t, archive)
			if string(in.node.Name) != "nar" {
				t.Errorf("the root node is named %q, want %q", in.node.Name, "nar")
			}
			checkReference(t, in, archive)
		})
	}
}

func TestMultiChunkFile(t *testing.T) {
	const size = 8 << 20
	body := noiseBytes(1, size)
	archive, err := io.ReadAll(fileArchive(bytes.NewReader(body), size))
	if err != nil {
		t.Fatal(err)
	}
	in := ingestArchive(t, archive)
	ck, err := key.Parse(in.node.ContentKey)
	if err != nil {
		t.Fatalf("content key: %v", err)
	}
	if ck.Type() != key.FileNode {
		t.Errorf("the content key of an 8 MiB file is a %s, want a FileNode", ck.Type())
	}
	if ck.Length() != size {
		t.Errorf("the content key says %d bytes, want %d", ck.Length(), size)
	}
	checkReference(t, in, archive)

	// The file inside a directory, beside others, is the same object.
	events := []event{dir(""), file("a", "before"), file("big", string(body)), file("c", "after"), leave()}
	archive = write(t, events)
	in = ingestArchive(t, archive)
	checkReference(t, in, archive)
	if _, ok := in.store.m[ck]; !ok {
		t.Errorf("the file has another content key inside a directory")
	}
}

// TestLargeDirectory has a directory of more entries than one DirLeaf
// holds, so that the directory's key is a DirNode.
func TestLargeDirectory(t *testing.T) {
	events := []event{dir("")}
	for i := range 3000 {
		name := fmt.Sprintf("entry-%05d", i)
		switch i % 3 {
		case 0:
			events = append(events, file(name, name))
		case 1:
			events = append(events, link(name, "target-of-"+name))
		default:
			events = append(events, dir(name), script("run", name), leave())
		}
	}
	events = append(events, leave())
	archive := write(t, events)
	in := ingestArchive(t, archive)
	ck, err := key.Parse(in.node.ContentKey)
	if err != nil {
		t.Fatalf("content key: %v", err)
	}
	if ck.Type() != key.DirNode {
		t.Fatalf("a directory of 3000 entries has a %s key, the test wants a DirNode", ck.Type())
	}
	checkReference(t, in, archive)
}

func TestRecipeBytes(t *testing.T) {
	const want = `{"format":"narrenhaus-recipe","narHash":"sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73","narSize":120,"node":"nar","version":1}`
	if got := Recipe(sha256.Sum256(nil), 120); string(got) != want {
		t.Fatalf("Recipe:\n got %s\nwant %s", got, want)
	}
	// The largest size is written in full.
	if got := Recipe(sha256.Sum256(nil), 1<<64-1); !bytes.Contains(got, []byte(`"narSize":18446744073709551615,`)) {
		t.Fatalf("Recipe of the largest size: %s", got)
	}
}

// wrapper stores a root directory holding node, named "nar", and a recipe
// of the given text, and returns its key.
func wrapper(t *testing.T, o *objects, node fstree.Entry, recipe string) key.Key {
	t.Helper()
	node.Name = []byte("nar")
	return o.directory(t, node, regularEntry("recipe", o.blob(t, recipe)))
}

// someRecipe is a recipe of version 1.
func someRecipe() string {
	return string(Recipe(sha256.Sum256([]byte("some archive")), 4711))
}

func TestOpen(t *testing.T) {
	o := newObjects()
	node := regularEntry("nar", o.blob(t, "contents"))
	root := wrapper(t, o, node, someRecipe())
	content, err := Open(root, o.get)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !sameEntry(content.Node, node) {
		t.Errorf("node %+v, want %+v", content.Node, node)
	}
	if want := sha256.Sum256([]byte("some archive")); content.NarHash != want || content.NarSize != 4711 {
		t.Errorf("hash %x and size %d, want %x and 4711", content.NarHash, content.NarSize, want)
	}
	// The wrapper and the recipe, and nothing of the store path.
	if o.gets != 2 {
		t.Errorf("Open read %d objects, want 2", o.gets)
	}
}

func TestOpenUnknownRecipeVersion(t *testing.T) {
	v1 := someRecipe()
	cases := []struct {
		name   string
		recipe string
	}{
		{"version 2", strings.Replace(v1, `"version":1}`, `"version":2}`, 1)},
		{"version 0", strings.Replace(v1, `"version":1}`, `"version":0}`, 1)},
		{"version 10", strings.Replace(v1, `"version":1}`, `"version":10}`, 1)},
		{"another format", strings.Replace(v1, `"narrenhaus-recipe"`, `"narrenhaus-delta"`, 1)},
		{"another format with other fields", `{"format":"something-else","version":1,"parts":[1,2,3]}`},
		{"a later version with other fields", `{"format":"narrenhaus-recipe","framing":{"a":[1]},"version":3}`},
		{"a later version of another layout", " {\n  \"version\": 7,\n  \"format\": \"narrenhaus-recipe\"\n}\n"},
		{"a format that is no text", `{"format":1,"version":1}`},
		{"a format that is null", `{"format":null,"version":1}`},
		{"a version that is text", strings.Replace(v1, `"version":1}`, `"version":"1"}`, 1)},
		{"a version with a fraction", strings.Replace(v1, `"version":1}`, `"version":1.0}`, 1)},
		{"a version that is null", strings.Replace(v1, `"version":1}`, `"version":null}`, 1)},
		{"a recipe too long to be version 1", `{"format":"narrenhaus-recipe","pad":"` + strings.Repeat("p", 100<<10) + `","version":9}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := newObjects()
			root := wrapper(t, o, regularEntry("nar", o.blob(t, "contents")), tc.recipe)
			_, err := Open(root, o.get)
			if !errors.Is(err, ErrUnknownRecipe) {
				t.Fatalf("Open: %v, want ErrUnknownRecipe", err)
			}
			if errors.Is(err, ErrBroken) {
				t.Fatalf("Open: %v counts as broken", err)
			}
			if len(err.Error()) > 512 {
				t.Errorf("the error has %d bytes: %v", len(err.Error()), err)
			}
			t.Log(err)
		})
	}
}

func TestOpenNotAWrapper(t *testing.T) {
	v1 := someRecipe()
	type build func(t *testing.T, o *objects) key.Key
	withRecipe := func(recipe string) build {
		return func(t *testing.T, o *objects) key.Key {
			return wrapper(t, o, regularEntry("nar", o.blob(t, "contents")), recipe)
		}
	}
	cases := []struct {
		name  string
		build build
	}{
		{"a plain directory", func(t *testing.T, o *objects) key.Key {
			return o.directory(t, regularEntry("a", o.blob(t, "a")), regularEntry("b", o.blob(t, "b")))
		}},
		{"an empty directory", func(t *testing.T, o *objects) key.Key {
			return o.directory(t)
		}},
		{"a Blob", func(t *testing.T, o *objects) key.Key {
			return o.blob(t, "contents")
		}},
		{"a FileNode", func(t *testing.T, o *objects) key.Key {
			return o.raw(t, key.FileNode, 0, []byte{0x80})
		}},
		{"a third entry after the two", func(t *testing.T, o *objects) key.Key {
			return o.directory(t,
				regularEntry("nar", o.blob(t, "contents")),
				regularEntry("recipe", o.blob(t, v1)),
				regularEntry("zzz", o.blob(t, "more")))
		}},
		{"a third entry before the two", func(t *testing.T, o *objects) key.Key {
			return o.directory(t,
				regularEntry("extra", o.blob(t, "more")),
				regularEntry("nar", o.blob(t, "contents")),
				regularEntry("recipe", o.blob(t, v1)))
		}},
		{"no recipe", func(t *testing.T, o *objects) key.Key {
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")))
		}},
		{"no node", func(t *testing.T, o *objects) key.Key {
			return o.directory(t, regularEntry("recipe", o.blob(t, v1)))
		}},
		{"the two entries in the wrong order", func(t *testing.T, o *objects) key.Key {
			return o.leaf(t, regularEntry("recipe", o.blob(t, v1)), regularEntry("nar", o.blob(t, "contents")))
		}},
		{"the node twice", func(t *testing.T, o *objects) key.Key {
			return o.leaf(t, regularEntry("nar", o.blob(t, "contents")), regularEntry("nar", o.blob(t, v1)))
		}},
		{"a directory of many entries", func(t *testing.T, o *objects) key.Key {
			entries := []fstree.Entry{regularEntry("nar", o.blob(t, "contents")), regularEntry("recipe", o.blob(t, v1))}
			for i := range 2000 {
				entries = append(entries, symlinkEntry(fmt.Sprintf("s%05d", i), "target"))
			}
			k := o.directory(t, entries...)
			if k.Type() != key.DirNode {
				t.Fatalf("the directory's key is a %s, the test wants a DirNode", k.Type())
			}
			return k
		}},
		{"a wrapper that does not decode", func(t *testing.T, o *objects) key.Key {
			return o.raw(t, key.DirLeaf, 5, []byte("\xffnope"))
		}},
		{"a recipe that is a symlink", func(t *testing.T, o *objects) key.Key {
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")), symlinkEntry("recipe", "nar"))
		}},
		{"a recipe that is a directory", func(t *testing.T, o *objects) key.Key {
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")), directoryEntry("recipe", o.directory(t)))
		}},
		{"a recipe entry without a key", func(t *testing.T, o *objects) key.Key {
			e := regularEntry("recipe", key.Key{})
			e.ContentKey = nil
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")), e)
		}},
		{"a recipe entry with the key of a directory", func(t *testing.T, o *objects) key.Key {
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")), regularEntry("recipe", o.directory(t)))
		}},
		{"a recipe whose Blob is not of its key's length", func(t *testing.T, o *objects) key.Key {
			rk := o.blob(t, v1)
			o.m[rk] = append(o.m[rk], ' ')
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")), regularEntry("recipe", rk))
		}},
		{"a recipe with an index over an empty Blob", func(t *testing.T, o *objects) key.Key {
			rk := o.fileNode(t, uint64(len(v1)), o.blob(t, v1), o.blob(t, ""))
			return o.directory(t, regularEntry("nar", o.blob(t, "contents")), regularEntry("recipe", rk))
		}},
		{"an empty recipe", withRecipe("")},
		{"a recipe that is not JSON", withRecipe("version 1")},
		{"a recipe that is a JSON array", withRecipe(`["narrenhaus-recipe",1]`)},
		{"a recipe that is null", withRecipe(`null`)},
		{"a recipe without a format", withRecipe(`{"version":1}`)},
		{"a recipe without a version", withRecipe(`{"format":"narrenhaus-recipe"}`)},
		{"version 1 with a newline after it", withRecipe(v1 + "\n")},
		{"version 1 with a space in it", withRecipe(strings.Replace(v1, `,"node"`, `, "node"`, 1))},
		{"version 1 with its keys in another order", withRecipe(`{"version":1,` + strings.TrimSuffix(v1[1:], `,"version":1}`) + `}`)},
		{"version 1 with another node", withRecipe(strings.Replace(v1, `"node":"nar"`, `"node":"recipe"`, 1))},
		{"version 1 with one more field", withRecipe(strings.Replace(v1, `,"node"`, `,"more":true,"node"`, 1))},
		{"version 1 without a hash", withRecipe(`{"format":"narrenhaus-recipe","narSize":120,"node":"nar","version":1}`)},
		{"version 1 with a hash in hex", withRecipe(`{"format":"narrenhaus-recipe","narHash":"sha256:` + strings.Repeat("0", 64) + `","narSize":120,"node":"nar","version":1}`)},
		{"version 1 with a hash of another kind", withRecipe(strings.Replace(v1, `sha256:`, `sha512:`, 1))},
		{"version 1 with a hash in capitals", withRecipe(`{"format":"narrenhaus-recipe","narHash":"sha256:` + strings.Repeat("A", 52) + `","narSize":120,"node":"nar","version":1}`)},
		{"version 1 with a size that has a sign", withRecipe(strings.Replace(v1, `"narSize":4711`, `"narSize":+4711`, 1))},
		{"version 1 with a negative size", withRecipe(strings.Replace(v1, `"narSize":4711`, `"narSize":-4711`, 1))},
		{"version 1 with a size that begins with zero", withRecipe(strings.Replace(v1, `"narSize":4711`, `"narSize":04711`, 1))},
		{"version 1 with a size beyond 64 bits", withRecipe(strings.Replace(v1, `"narSize":4711`, `"narSize":18446744073709551616`, 1))},
		{"version 1 with a size as text", withRecipe(strings.Replace(v1, `"narSize":4711`, `"narSize":"4711"`, 1))},
		{"version 1 with a fraction as size", withRecipe(strings.Replace(v1, `"narSize":4711`, `"narSize":4711.0`, 1))},
		{"a node that is a FIFO", func(t *testing.T, o *objects) key.Key {
			return wrapper(t, o, fstree.Entry{Mode: 0o010644, Mtime: 1_000_000_000}, v1)
		}},
		{"a node that is a file without a key", func(t *testing.T, o *objects) key.Key {
			return wrapper(t, o, fstree.Entry{Mode: 0o100444, Mtime: 1_000_000_000}, v1)
		}},
		{"a node that is a file with a directory's key", func(t *testing.T, o *objects) key.Key {
			return wrapper(t, o, regularEntry("nar", o.directory(t)), v1)
		}},
		{"a node that is a directory with a file's key", func(t *testing.T, o *objects) key.Key {
			return wrapper(t, o, directoryEntry("nar", o.blob(t, "contents")), v1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := newObjects()
			root := tc.build(t, o)
			_, err := Open(root, o.get)
			if !errors.Is(err, ErrBroken) {
				t.Fatalf("Open: %v, want ErrBroken", err)
			}
			if errors.Is(err, ErrUnknownRecipe) {
				t.Fatalf("Open: %v counts as an unknown recipe", err)
			}
			if len(err.Error()) > 512 {
				t.Errorf("the error has %d bytes: %v", len(err.Error()), err)
			}
			t.Log(err)
		})
	}
}

// TestOpenReadsNoObjectOfAWrongRoot checks that a root which cannot be a
// wrapper by its key is refused without a read.
func TestOpenReadsNoObjectOfAWrongRoot(t *testing.T) {
	o := newObjects()
	roots := map[string]key.Key{
		"Blob":     o.blob(t, "contents"),
		"FileNode": o.raw(t, key.FileNode, 0, []byte{0x80}),
		"DirNode":  o.raw(t, key.DirNode, 1, []byte{0x80}),
		"XattrSet": o.raw(t, key.XattrSet, 1, []byte{0xa0}),
		"Commit":   o.raw(t, key.Commit, 1, []byte{0xa0}),
	}
	for name, root := range roots {
		if _, err := Open(root, o.get); !errors.Is(err, ErrBroken) {
			t.Errorf("%s root: %v, want ErrBroken", name, err)
		}
	}
	if o.gets != 0 {
		t.Errorf("Open read %d objects, want none", o.gets)
	}
}

// TestOpenReturnsTheErrorOfTheGetter checks that an object that cannot be
// read does not make the reference broken.
func TestOpenReturnsTheErrorOfTheGetter(t *testing.T) {
	o := newObjects()
	recipe := o.blob(t, someRecipe())
	root := o.directory(t, regularEntry("nar", o.blob(t, "contents")), regularEntry("recipe", recipe))
	for name, missing := range map[string]key.Key{"wrapper": root, "recipe": recipe} {
		held := o.m[missing]
		delete(o.m, missing)
		_, err := Open(root, o.get)
		if !errors.Is(err, errAbsent) || errors.Is(err, ErrBroken) || errors.Is(err, ErrUnknownRecipe) {
			t.Errorf("without the %s: %v, want the getter's error and neither sentinel", name, err)
		}
		o.m[missing] = held
	}
}

func TestModesAsCoreReadsThem(t *testing.T) {
	const second = 1_000_000_000
	check := func(t *testing.T, where string, e fstree.Entry, mode uint64) {
		t.Helper()
		if e.Mode != mode {
			t.Errorf("%s: mode %#o, want %#o", where, e.Mode, mode)
		}
		if e.UID != 0 || e.GID != 0 {
			t.Errorf("%s: owner %d:%d, want 0:0", where, e.UID, e.GID)
		}
		if e.Mtime != second {
			t.Errorf("%s: mtime %d, want %d", where, e.Mtime, second)
		}
		if len(e.Rdev) != 0 || len(e.XattrsIn) != 0 || len(e.XattrsKey) != 0 {
			t.Errorf("%s: the entry has a device number or extended attributes", where)
		}
	}
	// modes maps each path of the first archive of the table to its mode.
	modes := map[string]uint64{
		"empty":         0o100444,
		"file":          0o100444,
		"link":          0o120777,
		"nothing":       0o040555,
		"run":           0o100555,
		"sub":           0o040555,
		"sub/deeper":    0o040555,
		"sub/deeper/up": 0o120777,
		"sub/eight":     0o100444,
		"sub/inner":     0o100444,
	}
	in := ingestArchive(t, write(t, roundTripArchives[0].events))
	top, err := fstree.CollectEntries(in.root, in.store.get)
	if err != nil {
		t.Fatalf("CollectEntries of the root: %v", err)
	}
	if len(top) != 2 || string(top[0].Name) != "nar" || string(top[1].Name) != "recipe" {
		t.Fatalf("the root holds %d entries, want nar and recipe", len(top))
	}
	check(t, "nar", top[0], 0o040555)
	check(t, "recipe", top[1], 0o100444)

	seen := 0
	var walk func(prefix string, e fstree.Entry)
	walk = func(prefix string, e fstree.Entry) {
		k, err := key.Parse(e.ContentKey)
		if err != nil {
			t.Fatalf("%s: content key: %v", prefix, err)
		}
		entries, err := fstree.CollectEntries(k, in.store.get)
		if err != nil {
			t.Fatalf("%s: CollectEntries: %v", prefix, err)
		}
		for _, e := range entries {
			path := prefix + string(e.Name)
			mode, ok := modes[path]
			if !ok {
				t.Errorf("%s is not in the archive", path)
				continue
			}
			seen++
			check(t, path, e, mode)
			switch mode {
			case 0o040555:
				walk(path+"/", e)
			case 0o120777:
				if len(e.ContentKey) != 0 || len(e.LinkTarget) == 0 {
					t.Errorf("%s: a symlink has a target and no content key", path)
				}
			default:
				if len(e.LinkTarget) != 0 {
					t.Errorf("%s: a file has a link target", path)
				}
			}
		}
	}
	walk("", top[0])
	if seen != len(modes) {
		t.Errorf("core lists %d entries, want %d", seen, len(modes))
	}

	// A root node that is not a directory keeps its kind in the entry.
	roots := map[string]uint64{
		"file root":            0o100444,
		"empty file root":      0o100444,
		"executable file root": 0o100555,
		"symlink root":         0o120777,
		"empty directory root": 0o040555,
	}
	for _, tc := range roundTripArchives {
		mode, ok := roots[tc.name]
		if !ok {
			continue
		}
		in := ingestArchive(t, write(t, tc.events))
		top, err := fstree.CollectEntries(in.root, in.store.get)
		if err != nil {
			t.Fatalf("%s: CollectEntries of the root: %v", tc.name, err)
		}
		if len(top) != 2 {
			t.Fatalf("%s: the root holds %d entries", tc.name, len(top))
		}
		check(t, tc.name+": nar", top[0], mode)
		check(t, tc.name+": recipe", top[1], 0o100444)
		if tc.name == "symlink root" && string(top[0].LinkTarget) != "/nix/store/0000-target" {
			t.Errorf("symlink root: target %q", top[0].LinkTarget)
		}
	}
}

// diskObjects keeps objects in a file, so that a test of memory in use
// measures the package and not the test's own store.
type diskObjects struct {
	f      *os.File
	end    int64
	at     map[key.Key][2]int64 // offset and length
	sample func()
}

func newDiskObjects(t *testing.T, sample func()) *diskObjects {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return &diskObjects{f: f, at: map[key.Key][2]int64{}, sample: sample}
}

func (d *diskObjects) emit(obj fstree.Object) error {
	d.sample()
	if _, ok := d.at[obj.Key]; ok {
		return nil
	}
	if _, err := d.f.WriteAt(obj.Bytes, d.end); err != nil {
		return err
	}
	d.at[obj.Key] = [2]int64{d.end, int64(len(obj.Bytes))}
	d.end += int64(len(obj.Bytes))
	return nil
}

func (d *diskObjects) get(k key.Key) ([]byte, error) {
	d.sample()
	at, ok := d.at[k]
	if !ok {
		return nil, fmt.Errorf("%s: %w", k, errAbsent)
	}
	b := make([]byte, at[1])
	if _, err := d.f.ReadAt(b, at[0]); err != nil {
		return nil, err
	}
	return b, nil
}

// sampled is a writer that takes a sample of the heap at every Write.
type sampled struct {
	w      io.Writer
	sample func()
}

func (s sampled) Write(p []byte) (int, error) {
	s.sample()
	return s.w.Write(p)
}

// TestIngestIsStreaming ingests and writes back an archive of one 64 MiB
// file, and checks that the heap in use stays far below the file's size
// throughout.
func TestIngestIsStreaming(t *testing.T) {
	const (
		size  = 64 << 20
		limit = 16 << 20
	)
	// The bound is on what the package keeps, not on how seldom the
	// collector runs: it is measured with the collector's default setting,
	// whatever GOGC and GOMEMLIMIT say where the test runs.
	defer debug.SetGCPercent(debug.SetGCPercent(100))
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(math.MaxInt64))
	var peak uint64
	sample := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		peak = max(peak, m.HeapInuse)
	}
	store := newDiskObjects(t, sample)
	discarded := 0
	discard := func(fstree.Object) error {
		sample()
		discarded++
		return nil
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	// Once with an emit that keeps nothing.
	_, _, _, err := Ingest(fileArchive(newNoise(64), size), discard, MaxNARSize)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// Once with one that writes the objects to a file.
	sent := sha256.New()
	node, narHash, narSize, err := Ingest(io.TeeReader(fileArchive(newNoise(64), size), sent), store.emit, MaxNARSize)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if !bytes.Equal(narHash[:], sent.Sum(nil)) {
		t.Errorf("NarHash %x, want %x", narHash, sent.Sum(nil))
	}
	if want := uint64(size + 120 - 8); narSize != want {
		t.Errorf("NarSize %d, want %d", narSize, want)
	}
	root, err := Root(node, narHash, narSize, store.emit)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	if discarded < 64 || len(store.at) != discarded+2 {
		t.Errorf("%d objects were emitted to the one emit and %d to the other", discarded, len(store.at))
	}

	content, err := Open(root, store.get)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	written := sha256.New()
	if err := WriteNAR(sampled{written, sample}, content.Node, store.get); err != nil {
		t.Fatalf("WriteNAR: %v", err)
	}
	if !bytes.Equal(written.Sum(nil), narHash[:]) {
		t.Errorf("the archive written hashes to %x, the one read to %x", written.Sum(nil), narHash)
	}

	runtime.ReadMemStats(&after)
	t.Logf("heap in use: %d KiB before, %d KiB at its peak; %d MiB allocated in all for three passes over %d MiB",
		before.HeapInuse>>10, peak>>10, (after.TotalAlloc-before.TotalAlloc)>>20, size>>20)
	if peak >= limit {
		t.Errorf("the heap in use reached %d bytes, the limit is %d", peak, limit)
	}
	// Each pass may allocate the file's bytes once: the chunks the
	// chunker hands out when ingesting, the objects this test's store
	// reads when writing. More than that is a copy nobody needs.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= 4*size {
		t.Errorf("%d bytes were allocated for three passes over %d", allocated, size)
	}
}

func TestSameNARSameRoot(t *testing.T) {
	for _, tc := range roundTripArchives {
		archive := write(t, tc.events)
		first := ingestArchive(t, archive)
		second := ingestArchive(t, archive)
		if first.root != second.root {
			t.Errorf("%s: two ingests give the roots %s and %s", tc.name, first.root, second.root)
		}
		// The sizes of the reads decide nothing.
		o := newObjects()
		node, narHash, narSize, err := Ingest(iotest.OneByteReader(bytes.NewReader(archive)), o.emit, MaxNARSize)
		if err != nil {
			t.Fatalf("%s: Ingest byte by byte: %v", tc.name, err)
		}
		root, err := Root(node, narHash, narSize, o.emit)
		if err != nil {
			t.Fatalf("%s: Root: %v", tc.name, err)
		}
		if root != first.root {
			t.Errorf("%s: read byte by byte the root is %s, not %s", tc.name, root, first.root)
		}
	}

	// Two archives that differ give two roots.
	a := ingestArchive(t, write(t, []event{file("", "one")}))
	b := ingestArchive(t, write(t, []event{script("", "one")}))
	if a.root == b.root {
		t.Errorf("a file and the same file made executable share the root %s", a.root)
	}
}

// TestFileAsCoreIngests compares File, and the file of an archive, with
// the key core's own ingest gives a file of the same bytes on disk.
func TestFileAsCoreIngests(t *testing.T) {
	sizes := []int{0, 1, 4096, 32<<10 - 1, 32 << 10, 32<<10 + 1, 100_000, 1 << 20, 1<<20 + 1, 3<<20 + 17}
	dir := t.TempDir()
	for _, size := range sizes {
		body := noiseBytes(uint64(size)+7, size)
		path := filepath.Join(dir, fmt.Sprintf("file-%d", size))
		if err := os.WriteFile(path, body, 0o644); err != nil {
			t.Fatal(err)
		}
		seq, root, err := ingest.Objects(path, ingest.Opts{})
		if err != nil {
			t.Fatalf("%d bytes: core's ingest: %v", size, err)
		}
		var want []key.Key
		for obj, err := range seq {
			if err != nil {
				t.Fatalf("%d bytes: core's ingest: %v", size, err)
			}
			want = append(want, obj.Key)
		}
		sortKeys(want)
		want = slices.Compact(want)

		o := newObjects()
		got, err := File(bytes.NewReader(body), o.emit)
		if err != nil {
			t.Fatalf("%d bytes: File: %v", size, err)
		}
		if got != *root {
			t.Errorf("%d bytes: File gives %s, core's ingest %s", size, got, *root)
		}
		if held := o.keys(); !slices.Equal(held, want) {
			t.Errorf("%d bytes: File emitted %d objects, core's ingest %d", size, len(held), len(want))
		}

		o = newObjects()
		node, _, _, err := Ingest(fileArchive(bytes.NewReader(body), int64(size)), o.emit, MaxNARSize)
		if err != nil {
			t.Fatalf("%d bytes: Ingest: %v", size, err)
		}
		if !bytes.Equal(node.ContentKey, root[:]) {
			t.Errorf("%d bytes: Ingest gives the file %x, core's ingest %s", size, node.ContentKey, *root)
		}
		if held := o.keys(); !slices.Equal(held, want) {
			t.Errorf("%d bytes: Ingest emitted %d objects, core's ingest %d", size, len(held), len(want))
		}
	}
}

// failing reads from r and then fails with err.
type failing struct {
	r   io.Reader
	err error
}

func (f *failing) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		err = f.err
	}
	return n, err
}

func TestFileReturnsTheErrorOfTheReader(t *testing.T) {
	// The reader's own io.ErrUnexpectedEOF is not the end of the file.
	for _, cause := range []error{errors.New("the reader failed"), io.ErrUnexpectedEOF} {
		for _, size := range []int{0, 100, 32 << 10, 200_000, 3 << 20} {
			o := newObjects()
			_, err := File(&failing{bytes.NewReader(noiseBytes(3, size)), cause}, o.emit)
			if err != cause {
				t.Errorf("after %d bytes: File: %v, want %v", size, err, cause)
			}
		}
	}
	// An error of emit, at each object in turn.
	sentinel := errors.New("emit failed")
	for _, size := range []int{0, 100, 3 << 20} {
		body := noiseBytes(3, size)
		total := 0
		if _, err := File(bytes.NewReader(body), func(fstree.Object) error { total++; return nil }); err != nil {
			t.Fatalf("%d bytes: File: %v", size, err)
		}
		for failAt := range total {
			n := 0
			emit := func(fstree.Object) error {
				if n++; n > failAt {
					return sentinel
				}
				return nil
			}
			if _, err := File(bytes.NewReader(body), emit); err != sentinel {
				t.Errorf("%d bytes, emit failing at call %d of %d: File: %v, want the error of emit", size, failAt+1, total, err)
			}
		}
	}
}

func TestIngestRefusesAMalformedArchive(t *testing.T) {
	big := []event{dir(""), file("a", "small"), file("big", string(noiseBytes(5, 300_000))), leave()}
	archives := map[string][]byte{"with a file of several chunks": write(t, big)}
	for _, tc := range roundTripArchives {
		archives[tc.name] = write(t, tc.events)
	}
	for name, whole := range archives {
		step := 1
		if len(whole) > 10_000 {
			step = 4099
		}
		for n := 0; n < len(whole); n += step {
			node, narHash, narSize, err := Ingest(bytes.NewReader(whole[:n]), newObjects().emit, MaxNARSize)
			if !errors.Is(err, nar.ErrMalformed) {
				t.Fatalf("%s cut to %d of %d bytes: %v, want an error wrapping nar.ErrMalformed", name, n, len(whole), err)
			}
			if node.Name != nil || node.ContentKey != nil || narHash != [32]byte{} || narSize != 0 {
				t.Fatalf("%s cut to %d bytes: Ingest failed and returned a node, a hash or a size", name, n)
			}
		}
		if _, _, _, err := Ingest(bytes.NewReader(append(bytes.Clone(whole), 0)), newObjects().emit, MaxNARSize); !errors.Is(err, nar.ErrMalformed) {
			t.Fatalf("%s with a byte after it: %v, want an error wrapping nar.ErrMalformed", name, err)
		}
	}
	// Names out of order: the parser's rule, seen through Ingest.
	unsorted := bytes.Replace(write(t, []event{dir(""), file("a", ""), file("b", ""), leave()}), narString("a"), narString("c"), 1)
	if _, _, _, err := Ingest(bytes.NewReader(unsorted), newObjects().emit, MaxNARSize); !errors.Is(err, nar.ErrMalformed) {
		t.Fatalf("names out of order: %v, want an error wrapping nar.ErrMalformed", err)
	}
}

func TestIngestReturnsOtherErrorsAsTheyAre(t *testing.T) {
	events := []event{
		dir(""),
		file("big", string(noiseBytes(9, 300_000))),
		dir("sub"),
		file("small", "small"),
		leave(),
		link("z", "big"),
		leave(),
	}
	archive := write(t, events)
	total := len(ingestArchive(t, archive).store.m)

	// An error of emit, at each object in turn.
	sentinel := errors.New("no space left")
	failures := 0
	for failAt := range total + 2 {
		n := 0
		emit := func(fstree.Object) error {
			if n++; n > failAt {
				return sentinel
			}
			return nil
		}
		_, _, _, err := Ingest(bytes.NewReader(archive), emit, MaxNARSize)
		switch {
		case err == sentinel:
			failures++
		case err != nil:
			t.Fatalf("emit failing at call %d: Ingest: %v, want the error of emit as it is", failAt+1, err)
		}
	}
	if failures < 4 {
		t.Errorf("emit failed in %d runs only; the archive has more objects than that", failures)
	}

	// An error of the reader, at several places, is not the archive's.
	for _, cause := range []error{errors.New("connection reset"), io.ErrUnexpectedEOF} {
		for _, at := range []int{0, 50, 200, 100_000, len(archive) - 1} {
			r := &failing{bytes.NewReader(archive[:at]), cause}
			_, _, _, err := Ingest(r, newObjects().emit, MaxNARSize)
			if err != cause {
				t.Errorf("the reader failing after %d bytes: Ingest: %v, want %v as it is", at, err, cause)
			}
		}
	}
}

func TestRootRefusesWhatIsNotANode(t *testing.T) {
	o := newObjects()
	file := regularEntry("nar", o.blob(t, "contents"))
	cases := map[string]fstree.Entry{
		"another name":             regularEntry("store-path", o.blob(t, "contents")),
		"no name":                  {Mode: 0o100444, ContentKey: file.ContentKey},
		"a FIFO":                   {Name: []byte("nar"), Mode: 0o010644},
		"a file without a key":     {Name: []byte("nar"), Mode: 0o100444},
		"a directory with a Blob":  directoryEntry("nar", o.blob(t, "contents")),
		"a file with a directory":  regularEntry("nar", o.directory(t)),
		"a symlink without target": symlinkEntry("nar", ""),
	}
	for name, node := range cases {
		before := len(o.m)
		if _, err := Root(node, [32]byte{}, 0, o.emit); err == nil {
			t.Errorf("%s: Root accepted the node", name)
		}
		if len(o.m) != before {
			t.Errorf("%s: Root emitted objects for a node it refused", name)
		}
	}
	sentinel := errors.New("emit failed")
	for failAt := range 2 {
		n := 0
		emit := func(fstree.Object) error {
			if n++; n > failAt {
				return sentinel
			}
			return nil
		}
		if _, err := Root(file, [32]byte{}, 0, emit); err != sentinel {
			t.Errorf("emit failing at call %d: Root: %v, want the error of emit", failAt+1, err)
		}
	}
}

// fileNode stores a FileNode over children whose key says length bytes.
func (o *objects) fileNode(t *testing.T, length uint64, children ...key.Key) key.Key {
	t.Helper()
	obj, err := fstree.EncodeFileNode(children)
	if err != nil {
		t.Fatal(err)
	}
	return o.raw(t, key.FileNode, length, obj.Bytes)
}

// dirNode stores a DirNode of pairs exactly as given and returns its key.
func (o *objects) dirNode(t *testing.T, pairs ...fstree.DirPair) key.Key {
	t.Helper()
	obj, err := fstree.EncodeDirNode(pairs)
	if err != nil {
		t.Fatal(err)
	}
	o.emit(obj)
	return obj.Key
}

func TestWriteNARRefusesABrokenTree(t *testing.T) {
	type build func(t *testing.T, o *objects) fstree.Entry
	inDir := func(entries func(t *testing.T, o *objects) []fstree.Entry) build {
		return func(t *testing.T, o *objects) fstree.Entry {
			return directoryEntry("nar", o.leaf(t, entries(t, o)...))
		}
	}
	cases := []struct {
		name  string
		build build
	}{
		{"a Blob longer than its key says", func(t *testing.T, o *objects) fstree.Entry {
			k := o.blob(t, "contents")
			o.m[k] = []byte("contents and more")
			return regularEntry("nar", k)
		}},
		{"a Blob shorter than its key says", func(t *testing.T, o *objects) fstree.Entry {
			k := o.blob(t, "contents")
			o.m[k] = []byte("cont")
			return regularEntry("nar", k)
		}},
		{"an empty Blob where its key says more", func(t *testing.T, o *objects) fstree.Entry {
			k := o.blob(t, "contents")
			o.m[k] = nil
			return regularEntry("nar", k)
		}},
		{"a Blob of a file in a directory that is too long", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			k := o.blob(t, "contents")
			o.m[k] = []byte("contents and more")
			return []fstree.Entry{regularEntry("a", k), regularEntry("b", o.blob(t, "b"))}
		})},
		{"a Blob of a file in a directory that is too short", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			k := o.blob(t, "contents")
			o.m[k] = []byte("c")
			return []fstree.Entry{regularEntry("a", k), regularEntry("b", o.blob(t, "b"))}
		})},
		{"a FileNode whose key says more than its children", func(t *testing.T, o *objects) fstree.Entry {
			return regularEntry("nar", o.fileNode(t, 11, o.blob(t, "one"), o.blob(t, "two")))
		}},
		{"a FileNode whose key says less than its children", func(t *testing.T, o *objects) fstree.Entry {
			return regularEntry("nar", o.fileNode(t, 5, o.blob(t, "one"), o.blob(t, "two")))
		}},
		{"a FileNode whose children's lengths overflow", func(t *testing.T, o *objects) fstree.Entry {
			huge := o.fileNode(t, 1<<63, o.blob(t, "one"))
			return regularEntry("nar", o.fileNode(t, 3, huge, huge, o.blob(t, "two")))
		}},
		{"a FileNode over a directory", func(t *testing.T, o *objects) fstree.Entry {
			d := o.directory(t)
			return regularEntry("nar", o.fileNode(t, d.Length(), d))
		}},
		{"a FileNode that does not decode", func(t *testing.T, o *objects) fstree.Entry {
			return regularEntry("nar", o.raw(t, key.FileNode, 4, []byte("nope")))
		}},
		{"FileNodes nested without end", func(t *testing.T, o *objects) fstree.Entry {
			k := o.blob(t, "contents")
			for range 200 {
				k = o.fileNode(t, k.Length(), k)
			}
			return regularEntry("nar", k)
		}},
		{"a FileNode over an empty Blob", func(t *testing.T, o *objects) fstree.Entry {
			return regularEntry("nar", o.fileNode(t, 3, o.blob(t, "one"), o.blob(t, "")))
		}},
		{"a FileNode over a FileNode of nothing", func(t *testing.T, o *objects) fstree.Entry {
			return regularEntry("nar", o.fileNode(t, 3, o.fileNode(t, 0), o.blob(t, "one")))
		}},
		{"a file whose key is a directory's", func(t *testing.T, o *objects) fstree.Entry {
			return regularEntry("nar", o.directory(t))
		}},
		{"a file whose key is no key", func(t *testing.T, o *objects) fstree.Entry {
			e := regularEntry("nar", key.Key{})
			e.ContentKey = []byte("short")
			return e
		}},
		{"a file whose key has a reserved type", func(t *testing.T, o *objects) fstree.Entry {
			k := o.blob(t, "contents")
			k[31] = 0xf0
			return fstree.Entry{Name: []byte("nar"), Mode: 0o100444, ContentKey: k[:]}
		}},
		{"a directory whose key is a Blob's", func(t *testing.T, o *objects) fstree.Entry {
			return directoryEntry("nar", o.blob(t, "contents"))
		}},
		{"a directory whose key is a Commit's", func(t *testing.T, o *objects) fstree.Entry {
			return directoryEntry("nar", o.raw(t, key.Commit, 1, []byte{0xa0}))
		}},
		{"a directory that does not decode", func(t *testing.T, o *objects) fstree.Entry {
			return directoryEntry("nar", o.raw(t, key.DirLeaf, 4, []byte("nope")))
		}},
		{"a directory index that does not decode", func(t *testing.T, o *objects) fstree.Entry {
			return directoryEntry("nar", o.raw(t, key.DirNode, 4, []byte("nope")))
		}},
		{"a directory index over a Blob", func(t *testing.T, o *objects) fstree.Entry {
			b := o.blob(t, "contents")
			obj, err := fstree.EncodeDirNode([]fstree.DirPair{{SepName: []byte("a"), ChildKey: b[:]}})
			if err != nil {
				t.Fatal(err)
			}
			o.emit(obj)
			return directoryEntry("nar", obj.Key)
		}},
		{"a directory index with a child that is no key", func(t *testing.T, o *objects) fstree.Entry {
			obj, err := fstree.EncodeDirNode([]fstree.DirPair{{SepName: []byte("a"), ChildKey: keyBytes(o.blob(t, "x"))}})
			if err != nil {
				t.Fatal(err)
			}
			// The same pair with a key of five bytes: 0x58 0x20 begins a
			// byte string of 32 bytes, 0x45 one of five.
			at := bytes.Index(obj.Bytes, []byte{0x58, 0x20})
			if at < 0 {
				t.Fatal("the child key is not where the test expects it")
			}
			data := append(bytes.Clone(obj.Bytes[:at]), 0x45, 'n', 'o', 'k', 'e', 'y')
			return directoryEntry("nar", o.raw(t, key.DirNode, uint64(len(data)), data))
		}},
		{"a directory index over an empty leaf", func(t *testing.T, o *objects) fstree.Entry {
			first, empty := o.leaf(t, symlinkEntry("a", "x")), o.leaf(t)
			return directoryEntry("nar", o.dirNode(t, fstree.DirPair{SepName: []byte("a"), ChildKey: first[:]}, fstree.DirPair{SepName: []byte("b"), ChildKey: empty[:]}))
		}},
		{"a directory index without a child", func(t *testing.T, o *objects) fstree.Entry {
			return directoryEntry("nar", o.dirNode(t))
		}},
		{"a directory index over one without a child", func(t *testing.T, o *objects) fstree.Entry {
			first, empty := o.leaf(t, symlinkEntry("a", "x")), o.dirNode(t)
			return directoryEntry("nar", o.dirNode(t, fstree.DirPair{SepName: []byte("a"), ChildKey: first[:]}, fstree.DirPair{SepName: []byte("b"), ChildKey: empty[:]}))
		}},
		{"directory indexes nested without end", func(t *testing.T, o *objects) fstree.Entry {
			k := o.leaf(t, symlinkEntry("a", "b"))
			for range 200 {
				obj, err := fstree.EncodeDirNode([]fstree.DirPair{{SepName: []byte("a"), ChildKey: k[:]}})
				if err != nil {
					t.Fatal(err)
				}
				o.emit(obj)
				k = obj.Key
			}
			return directoryEntry("nar", k)
		}},
		{"a FIFO", func(t *testing.T, o *objects) fstree.Entry {
			return fstree.Entry{Name: []byte("nar"), Mode: 0o010644}
		}},
		{"a device in a directory", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{{Name: []byte("null"), Mode: 0o020666, Rdev: []uint64{1, 3}}}
		})},
		{"a socket with a long name", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{{Name: bytes.Repeat([]byte{0xff}, 255), Mode: 0o140755}}
		})},
		{"an entry without a type", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{{Name: []byte("a"), Mode: 0o644}}
		})},
		{"a symlink without a target", func(t *testing.T, o *objects) fstree.Entry {
			return symlinkEntry("nar", "")
		}},
		{"a symlink with NUL in its target", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry("a", "b\x00c")}
		})},
		{"entries out of order", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry("b", "x"), symlinkEntry("a", "x")}
		})},
		{"an entry twice", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry("a", "x"), symlinkEntry("a", "y")}
		})},
		{"entries out of order across two leaves", func(t *testing.T, o *objects) fstree.Entry {
			first := o.leaf(t, symlinkEntry("m", "x"))
			second := o.leaf(t, symlinkEntry("a", "x"))
			obj, err := fstree.EncodeDirNode([]fstree.DirPair{{SepName: []byte("m"), ChildKey: first[:]}, {SepName: []byte("a"), ChildKey: second[:]}})
			if err != nil {
				t.Fatal(err)
			}
			o.emit(obj)
			return directoryEntry("nar", obj.Key)
		}},
		{"a name with a slash", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry("a/b", "x")}
		})},
		{"an empty name", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry("", "x")}
		})},
		{"the name ..", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry("..", "x")}
		})},
		{"a name of 256 bytes", inDir(func(t *testing.T, o *objects) []fstree.Entry {
			return []fstree.Entry{symlinkEntry(strings.Repeat("n", 256), "x")}
		})},
		{"directories nested 65 deep", func(t *testing.T, o *objects) fstree.Entry {
			k := o.directory(t)
			for range 64 {
				k = o.directory(t, directoryEntry("d", k))
			}
			return directoryEntry("nar", k)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := newObjects()
			node := tc.build(t, o)
			err := WriteNAR(io.Discard, node, o.get)
			if !errors.Is(err, ErrBroken) {
				t.Fatalf("WriteNAR: %v, want ErrBroken", err)
			}
			if len(err.Error()) > 512 {
				t.Errorf("the error has %d bytes: %v", len(err.Error()), err)
			}
			t.Log(err)
		})
	}
}

// TestIndexThatHoldsNothingCostsNoWork checks that an index cannot make
// its reader work without giving it anything: one that names a child of no
// bytes, or of no entries, twice at each of its levels would be read in a
// number of steps that doubles with every level.
func TestIndexThatHoldsNothingCostsNoWork(t *testing.T) {
	const levels = 22
	v1 := someRecipe()
	cases := []struct {
		name string
		read func(o *objects) error
	}{
		{"a file", func(o *objects) error {
			k := o.blob(t, "")
			for range levels {
				k = o.fileNode(t, 0, k, k)
			}
			return WriteNAR(io.Discard, regularEntry("nar", k), o.get)
		}},
		{"a directory", func(o *objects) error {
			k := o.leaf(t)
			for range levels {
				k = o.dirNode(t, fstree.DirPair{SepName: []byte("a"), ChildKey: k[:]}, fstree.DirPair{SepName: []byte("b"), ChildKey: k[:]})
			}
			return WriteNAR(io.Discard, directoryEntry("nar", k), o.get)
		}},
		{"a recipe", func(o *objects) error {
			k := o.blob(t, "")
			for range levels {
				k = o.fileNode(t, 0, k, k)
			}
			rk := o.fileNode(t, uint64(len(v1)), o.blob(t, v1), k)
			_, err := Open(o.directory(t, regularEntry("nar", o.blob(t, "contents")), regularEntry("recipe", rk)), o.get)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := newObjects()
			err := tc.read(o)
			if !errors.Is(err, ErrBroken) {
				t.Errorf("%v, want ErrBroken", err)
			}
			if o.gets > 2*levels {
				t.Errorf("%d objects were read of an index of %d levels", o.gets, levels)
			}
		})
	}
}

// TestIngestRefusesAnArchiveThatIsTooLarge checks the limit on the bytes
// of an archive, with limits a test can reach.
func TestIngestRefusesAnArchiveThatIsTooLarge(t *testing.T) {
	if MaxNARSize != 64<<30 {
		t.Errorf("MaxNARSize is %d, want 64 GiB", uint64(MaxNARSize))
	}
	archive := write(t, []event{dir(""), file("a", "small"), file("big", string(noiseBytes(5, 300_000))), link("z", "big"), leave()})
	size := uint64(len(archive))

	node, narHash, narSize, err := Ingest(bytes.NewReader(archive), newObjects().emit, size)
	if err != nil || narSize != size || narHash != sha256.Sum256(archive) || string(node.Name) != "nar" {
		t.Fatalf("an archive of the limit's length: size %d, hash %x, node %q, %v", narSize, narHash, node.Name, err)
	}
	for _, limit := range []uint64{size - 1, size / 2, 100, 0} {
		// The reader has no end: what stops the reading is the limit.
		read := &counted{r: io.MultiReader(bytes.NewReader(archive), newNoise(1))}
		node, narHash, narSize, err := Ingest(read, newObjects().emit, limit)
		if !errors.Is(err, ErrTooLarge) || errors.Is(err, nar.ErrMalformed) {
			t.Fatalf("limit %d: %v, want ErrTooLarge", limit, err)
		}
		if node.Name != nil || node.ContentKey != nil || narHash != [32]byte{} || narSize != 0 {
			t.Errorf("limit %d: Ingest failed and returned a node, a hash or a size", limit)
		}
		// It stops where the limit is passed, not at the archive's end:
		// within what the parser reads ahead.
		if read.n > limit+1<<20 {
			t.Errorf("limit %d: %d bytes were read", limit, read.n)
		}
	}
	// A reader's error at the limit is not the archive's being too large.
	cause := errors.New("connection reset")
	if _, _, _, err := Ingest(&failing{bytes.NewReader(archive[:100]), cause}, newObjects().emit, 100); err != cause {
		t.Errorf("the reader failing at the limit: %v, want %v as it is", err, cause)
	}
}

// counted counts the bytes read through it.
type counted struct {
	r io.Reader
	n uint64
}

func (c *counted) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += uint64(n)
	return n, err
}

// TestWriteNARWritesNoByteBeyondAFile checks that a file whose objects
// give other bytes than its key says ends the archive at once: nothing
// that follows the file is written.
func TestWriteNARWritesNoByteBeyondAFile(t *testing.T) {
	o := newObjects()
	k := o.blob(t, "payload!")
	node := directoryEntry("nar", o.directory(t, regularEntry("a", k), regularEntry("b", o.blob(t, "the file after"))))
	var good bytes.Buffer
	if err := WriteNAR(&good, node, o.get); err != nil {
		t.Fatalf("WriteNAR: %v", err)
	}
	at := bytes.Index(good.Bytes(), []byte("payload!"))
	if at < 0 {
		t.Fatal("the file's bytes are not in the archive")
	}
	for _, tampered := range []string{"payload! and more", "payl", ""} {
		o.m[k] = []byte(tampered)
		var out bytes.Buffer
		if err := WriteNAR(&out, node, o.get); !errors.Is(err, ErrBroken) {
			t.Fatalf("a Blob of %d bytes for a key of 8: WriteNAR: %v, want ErrBroken", len(tampered), err)
		}
		if out.Len() != at || !bytes.HasPrefix(good.Bytes(), out.Bytes()) {
			t.Errorf("a Blob of %d bytes for a key of 8: %d bytes were written, the file's bytes begin at %d", len(tampered), out.Len(), at)
		}
	}
}

// limited fails with err once more than n bytes were written to it.
type limited struct {
	n   int
	err error
}

func (l *limited) Write(p []byte) (int, error) {
	if len(p) > l.n {
		return 0, l.err
	}
	l.n -= len(p)
	return len(p), nil
}

func TestWriteNARReturnsOtherErrorsAsTheyAre(t *testing.T) {
	events := []event{
		dir(""),
		file("big", string(noiseBytes(11, 300_000))),
		dir("sub"),
		file("small", "small"),
		leave(),
		link("z", "big"),
		leave(),
	}
	archive := write(t, events)
	in := ingestArchive(t, archive)

	// The destination's error, at several places.
	sentinel := errors.New("connection closed")
	for _, n := range []int{0, 8, 100, 150_000, len(archive) - 1} {
		err := WriteNAR(&limited{n, sentinel}, in.node, in.store.get)
		if err != sentinel {
			t.Errorf("the destination failing after %d bytes: WriteNAR: %v, want its error as it is", n, err)
		}
	}
	if err := WriteNAR(&limited{len(archive), sentinel}, in.node, in.store.get); err != nil {
		t.Errorf("a destination with room for the archive: WriteNAR: %v", err)
	}

	// An object that cannot be read is not a broken reference.
	reachable, err := fstree.ReachableKeys(in.root, in.store.get)
	if err != nil {
		t.Fatal(err)
	}
	wrapperOnly := 0
	for _, k := range reachable {
		held := in.store.m[k]
		delete(in.store.m, k)
		err := WriteNAR(io.Discard, in.node, in.store.get)
		in.store.m[k] = held
		switch {
		case err == nil:
			wrapperOnly++ // the wrapper and the recipe are not part of the node
		case !errors.Is(err, errAbsent) || errors.Is(err, ErrBroken):
			t.Errorf("without %s (%s): WriteNAR: %v, want the getter's error and not ErrBroken", k, k.Type(), err)
		case !strings.Contains(err.Error(), k.String()):
			t.Errorf("without %s: the error does not name the object: %v", k, err)
		}
	}
	if wrapperOnly != 2 {
		t.Errorf("WriteNAR did without %d objects, want 2: the wrapper and the recipe", wrapperOnly)
	}
}

// TestWriteNARIgnoresWhatANARDoesNotHold writes a tree whose entries have
// owners, times and permissions other than the convention's: the archive
// depends on the kind, the executable bit, the bytes and the target.
func TestWriteNARIgnoresWhatANARDoesNotHold(t *testing.T) {
	o := newObjects()
	entries := []fstree.Entry{
		{Name: []byte("file"), Mode: 0o100640, UID: 1000, GID: 100, Mtime: -5, ContentKey: keyBytes(o.blob(t, "plain\n"))},
		{Name: []byte("link"), Mode: 0o120755, UID: 7, LinkTarget: []byte("file")},
		{Name: []byte("run"), Mode: 0o100700, Mtime: 1 << 40, ContentKey: keyBytes(o.blob(t, "#!/bin/sh\n"))},
		{Name: []byte("sub"), Mode: 0o040700, GID: 3, ContentKey: keyBytes(o.directory(t))},
	}
	node := fstree.Entry{Name: []byte("anything"), Mode: 0o041777, ContentKey: keyBytes(o.directory(t, entries...))}
	var out bytes.Buffer
	if err := WriteNAR(&out, node, o.get); err != nil {
		t.Fatalf("WriteNAR: %v", err)
	}
	want := write(t, []event{dir(""), file("file", "plain\n"), link("link", "file"), script("run", "#!/bin/sh\n"), dir("sub"), leave(), leave()})
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("the archive differs from that of the same tree with the convention's metadata")
	}
}

func keyBytes(k key.Key) []byte { return k[:] }
