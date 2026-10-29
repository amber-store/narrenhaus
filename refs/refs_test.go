package refs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/core/chunkers"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"

	"github.com/amber-store/narrenhaus/nar"
	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/remote"
	"github.com/amber-store/narrenhaus/testserver"
)

const testPrefix = "acme/"

// quiet drops what a Remote logs.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// logBuffer keeps what the store logs. A test that fails prints it, and
// so does every test when NARRENHAUS_TEST_LOG is set.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// has reports whether a line of the log has every one of parts.
func (l *logBuffer) has(parts ...string) bool { return l.count(parts...) > 0 }

// count returns how many lines of the log have every one of parts.
func (l *logBuffer) count(parts ...string) int {
	n := 0
lines:
	for line := range strings.SplitSeq(l.String(), "\n") {
		for _, part := range parts {
			if !strings.Contains(line, part) {
				continue lines
			}
		}
		n++
	}
	return n
}

// bucket sees every request of a client to the server's bucket.
type bucket struct {
	mu   sync.Mutex
	seen []string // "GET /jaccard-test/packs/<root>/<upload>.data"
	hook func(w http.ResponseWriter, r *http.Request) bool
}

func (b *bucket) serve(w http.ResponseWriter, r *http.Request) bool {
	b.mu.Lock()
	b.seen = append(b.seen, r.Method+" "+r.URL.Path)
	hook := b.hook
	b.mu.Unlock()
	return hook != nil && hook(w, r)
}

// answer puts hook before the bucket; nil removes it.
func (b *bucket) answer(hook func(w http.ResponseWriter, r *http.Request) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hook = hook
}

// forget drops what was seen so far: the requests of the pushes that set
// a test up.
func (b *bucket) forget() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen = nil
}

// gets counts the downloads seen: of the pack of root when one is given,
// and of the object with the extension ext (".idx" or ".data") when that
// is not empty.
func (b *bucket) gets(ext string, root ...key.Key) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, s := range b.seen {
		if !strings.HasPrefix(s, "GET ") || !strings.HasSuffix(s, ext) {
			continue
		}
		if len(root) > 0 && !strings.Contains(s, "/"+root[0].String()+"/") {
			continue
		}
		n++
	}
	return n
}

// of reports whether the request r is for the object with extension ext of
// the pack of root.
func of(r *http.Request, root key.Key, ext string) bool {
	return strings.Contains(r.URL.Path, "/"+root.String()+"/") && strings.HasSuffix(r.URL.Path, ext)
}

// fixture is a server, a cache directory and a store on them.
type fixture struct {
	t      *testing.T
	srv    *testserver.Server
	bucket *bucket
	pusher *remote.Remote // writes the references a test needs
	rem    *remote.Remote // the store's
	cache  *packcache.Cache
	store  *Store
	log    *logBuffer
}

type config struct {
	options Options
	server  testserver.Options
	limit   uint64 // of the cache; 0 means 1 GiB
}

func newFixture(t *testing.T, cfg config) *fixture {
	t.Helper()
	f := &fixture{t: t, bucket: &bucket{}, log: &logBuffer{}}
	f.srv = testserver.Start(t, cfg.server)
	f.srv.Intercept(f.bucket.serve)

	f.pusher = remote.New(remote.Config{Dial: f.srv.Dial(), Log: quiet})
	t.Cleanup(f.pusher.Close)
	f.rem = remote.New(remote.Config{
		Dial:          f.srv.Dial(),
		Log:           quiet,
		LookupTimeout: 3 * time.Second,
		DialTimeout:   300 * time.Millisecond,
		FailFast:      500 * time.Millisecond,
	})
	t.Cleanup(f.rem.Close)

	limit := cfg.limit
	if limit == 0 {
		limit = 1 << 30
	}
	var err error
	f.cache, err = packcache.Open(t.TempDir(), limit, packcache.Origin{Server: "test", Prefix: testPrefix})
	if err != nil {
		t.Fatalf("opening the cache: %v", err)
	}
	t.Cleanup(func() { f.cache.Close() })

	opts := cfg.options
	opts.Prefix = testPrefix
	opts.Log = slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.store = New(context.Background(), f.cache, f.rem, opts)
	t.Cleanup(func() {
		f.store.Close()
		if t.Failed() || os.Getenv("NARRENHAUS_TEST_LOG") != "" {
			t.Logf("the store's log:\n%s", f.log.String())
		}
	})
	return f
}

// objects is a set of objects in memory: what a push reads.
type objects struct {
	mu sync.Mutex
	m  map[key.Key][]byte
}

func newObjects() *objects { return &objects{m: map[key.Key][]byte{}} }

func (o *objects) emit(obj fstree.Object) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.m[obj.Key] = bytes.Clone(obj.Bytes)
	return nil
}

func (o *objects) Get(k key.Key) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.m[k]
	if !ok {
		return nil, fmt.Errorf("no object %s", k)
	}
	return bytes.Clone(b), nil
}

func (o *objects) clone() *objects {
	o.mu.Lock()
	defer o.mu.Unlock()
	return &objects{m: maps.Clone(o.m)}
}

// noise returns n bytes that do not compress, the same for the same seed.
func noise(seed byte, n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

// node is a node of an archive to be written.
type node struct {
	entries map[string]node // a directory, when not nil
	body    []byte
	exec    bool
	target  string // a symlink, when not empty
}

func dir(entries map[string]node) node { return node{entries: entries} }
func file(body []byte) node            { return node{body: body} }
func text(body string) node            { return node{body: []byte(body)} }
func script(body string) node          { return node{body: []byte(body), exec: true} }
func link(target string) node          { return node{target: target} }

// archive returns the canonical archive whose root is n.
func archive(t *testing.T, n node) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := nar.NewWriter(&buf)
	var put func(name string, n node) error
	put = func(name string, n node) error {
		switch {
		case n.entries != nil:
			if err := w.Directory(name); err != nil {
				return err
			}
			for _, name := range slices.Sorted(maps.Keys(n.entries)) {
				if err := put(name, n.entries[name]); err != nil {
					return err
				}
			}
			return w.Leave()
		case n.target != "":
			return w.Symlink(name, n.target)
		default:
			body, err := w.Regular(name, n.exec, uint64(len(n.body)))
			if err != nil {
				return err
			}
			_, err = body.Write(n.body)
			return err
		}
	}
	if err := put("", n); err != nil {
		t.Fatalf("writing an archive: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writing an archive: %v", err)
	}
	return buf.Bytes()
}

// smallTree is a store path of a few files; what differs between two of
// them follows from seed.
func smallTree(seed byte) node {
	return dir(map[string]node{
		"bin":    dir(map[string]node{"tool": script("#!/bin/sh\necho " + string('a'+rune(seed%26)) + "\n")}),
		"data":   file(noise(seed, 100<<10)),
		"empty":  text(""),
		"latest": link("data"),
	})
}

// reference is a content reference in memory: an archive and the objects
// narrenhaus makes of it.
type reference struct {
	archive []byte
	sum     [32]byte
	hash    string // sum in Nix's base 32
	size    uint64
	node    nartree.Node
	root    key.Key
	objs    *objects
}

// ingested returns the content reference of an archive.
func ingested(t *testing.T, archive []byte) *reference {
	t.Helper()
	r := &reference{archive: archive, objs: newObjects()}
	var err error
	r.node, r.sum, r.size, err = nartree.Ingest(bytes.NewReader(archive), r.objs.emit, nartree.MaxNARSize)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if r.sum != sha256.Sum256(archive) || r.size != uint64(len(archive)) {
		t.Fatalf("Ingest gives hash %x and size %d for an archive of %d bytes", r.sum, r.size, len(archive))
	}
	r.hash = nix32.Encode(r.sum[:])
	r.root, err = nartree.Root(r.node, r.sum, r.size, r.objs.emit)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	return r
}

// stating returns r with a recipe that states another hash and size: the
// tree is the same, the root is another.
func (r *reference) stating(t *testing.T, sum [32]byte, size uint64) *reference {
	t.Helper()
	out := *r
	out.objs = r.objs.clone()
	out.sum, out.size, out.hash = sum, size, nix32.Encode(sum[:])
	var err error
	out.root, err = nartree.Root(r.node, sum, size, out.objs.emit)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	return &out
}

// wrapped returns r with recipe as the text of its recipe, whatever it
// says: the directory of the two entries is built here as nartree builds
// it.
func (r *reference) wrapped(t *testing.T, recipe []byte) *reference {
	t.Helper()
	out := *r
	out.objs = r.objs.clone()
	blob, err := fstree.EncodeBlob(recipe)
	if err != nil {
		t.Fatal(err)
	}
	out.objs.emit(blob)
	b := fstree.NewDirBuilder(chunkers.NewItemChunker(ingest.DefaultItemBits))
	for _, e := range []fstree.Entry{
		r.node,
		{Name: []byte("recipe"), Mode: 0o100444, Mtime: 1_000_000_000, ContentKey: blob.Key[:]},
	} {
		if err := b.AddEntry(out.objs.emit, e); err != nil {
			t.Fatal(err)
		}
	}
	if out.root, err = b.Finish(out.objs.emit); err != nil {
		t.Fatal(err)
	}
	return &out
}

// storeHash returns the hash part of a store path, 32 characters that
// follow from seed.
func storeHash(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return nix32.Encode(sum[:20])
}

// pathInfo is a narinfo reference in memory.
type pathInfo struct {
	hash string // the store path's hash part
	text []byte
	root key.Key
	objs *objects
}

// textOf returns the reference whose content is text, as the narinfo of
// the store path with hash.
func textOf(t *testing.T, hash string, text []byte) *pathInfo {
	t.Helper()
	p := &pathInfo{hash: hash, text: text, objs: newObjects()}
	var err error
	if p.root, err = nartree.File(bytes.NewReader(text), p.objs.emit); err != nil {
		t.Fatalf("File: %v", err)
	}
	return p
}

// nixText is the narinfo Nix writes for the store path <hash>-<name> with
// the content r, uploaded without compression.
func nixText(hash, name string, r *reference, references ...string) []byte {
	return fmt.Appendf(nil, "StorePath: /nix/store/%s-%s\nURL: nar/%s.nar\nCompression: none\nNarHash: sha256:%s\nNarSize: %d\nReferences: %s\nSig: cache.example-1:%s\n",
		hash, name, r.hash, r.hash, r.size, strings.Join(references, " "),
		base64.StdEncoding.EncodeToString(make([]byte, 64)))
}

// narinfoOf returns the narinfo reference of the store path <hash>-<name>
// with the content r, as narrenhaus stores it.
func narinfoOf(t *testing.T, hash, name string, r *reference, references ...string) *pathInfo {
	t.Helper()
	info, err := narinfo.Parse(nixText(hash, name, r, references...))
	if err != nil {
		t.Fatalf("the narinfo of the test: %v", err)
	}
	return textOf(t, hash, info.Stored())
}

// push writes the reference name on the server.
func (f *fixture) push(name string, root key.Key, objs client.ObjectSource, minDedup float64) client.PushResult {
	f.t.Helper()
	res, err := f.pusher.Push(context.Background(), objs, name, root, client.PushOptions{MinDedup: minDedup, TempDir: f.t.TempDir()})
	if err != nil {
		f.t.Fatalf("pushing %q: %v", name, err)
	}
	return res
}

// pushContent writes a content reference under the name of its hash, with
// the parent rule narrenhaus uploads with.
func (f *fixture) pushContent(r *reference) client.PushResult {
	f.t.Helper()
	return f.push(f.store.ContentName(r.hash), r.root, r.objs, 0.5)
}

// pushNarinfo writes a narinfo reference as narrenhaus does: a base pack.
func (f *fixture) pushNarinfo(p *pathInfo) {
	f.t.Helper()
	f.push(f.store.NarinfoName(p.hash), p.root, p.objs, 2)
}

// remove deletes the reference name on the server.
func (f *fixture) remove(name string) {
	f.t.Helper()
	if err := f.srv.Client(f.t).Delete(context.Background(), name); err != nil {
		f.t.Fatalf("deleting %q: %v", name, err)
	}
}

func background(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// nar returns the archive the store serves for hash.
func (f *fixture) nar(hash string) ([]byte, error) {
	f.t.Helper()
	c, err := f.store.Content(background(f.t), hash)
	if err != nil {
		return nil, err
	}
	defer c.Release()
	var buf bytes.Buffer
	n, err := c.WriteTo(&buf)
	if err != nil {
		f.t.Fatalf("WriteTo: %v", err)
	}
	if n != int64(buf.Len()) || uint64(n) != c.NarSize() {
		f.t.Fatalf("WriteTo returned %d, wrote %d bytes, NarSize is %d", n, buf.Len(), c.NarSize())
	}
	return buf.Bytes(), nil
}

// serves fails the test unless the store serves r's archive for its hash.
func (f *fixture) serves(r *reference) {
	f.t.Helper()
	got, err := f.nar(r.hash)
	if err != nil {
		f.t.Fatalf("Content(%s): %v", r.hash, err)
	}
	if !bytes.Equal(got, r.archive) {
		f.t.Fatalf("Content(%s) wrote %d bytes that are not the archive of %d bytes", r.hash, len(got), len(r.archive))
	}
}

// notFound fails the test unless err is ErrNotFound.
func notFound(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("%s: error %v, want ErrNotFound", what, err)
	}
}

// packSize is what a pack of one object of n bytes counts in the cache.
func packSize(objects int, n int) uint64 {
	return uint64(n) + packfile.IndexSize(uint64(objects))
}

// tmpFiles returns the names of what lies under the cache's tmp/.
func (f *fixture) tmpFiles() []string {
	f.t.Helper()
	entries, err := os.ReadDir(f.cache.TempDir())
	if err != nil {
		f.t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

func TestNames(t *testing.T) {
	s := New(context.Background(), nil, nil, Options{Prefix: "acme/"})
	defer s.Close()
	if got, want := s.NarinfoName("0c6kzph7l0dcbfmjap64f0czdafn3b7x"), "acme/0c6kzph7l0dcbfmjap64f0czdafn3b7x.narinfo"; got != want {
		t.Errorf("NarinfoName = %q, want %q", got, want)
	}
	hash := strings.Repeat("0", 52)
	if got, want := s.ContentName(hash), "acme/nar/"+hash+".nar"; got != want {
		t.Errorf("ContentName = %q, want %q", got, want)
	}
	// The prefix is taken as it is: no separator is added.
	bare := New(context.Background(), nil, nil, Options{Prefix: "team"})
	defer bare.Close()
	if got, want := bare.NarinfoName("abc"), "teamabc.narinfo"; got != want {
		t.Errorf("NarinfoName = %q, want %q", got, want)
	}
}

func TestNarinfoFetchesNoContent(t *testing.T) {
	f := newFixture(t, config{})
	content := ingested(t, archive(t, smallTree(1)))
	f.pushContent(content)
	hash := storeHash("one")
	info := narinfoOf(t, hash, "one-1.0", content, storeHash("dep")+"-dep-2", hash+"-one-1.0")
	f.pushNarinfo(info)
	f.bucket.forget()

	got, err := f.store.Narinfo(background(t), hash)
	if err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if !bytes.Equal(got.Stored(), info.text) {
		t.Errorf("Narinfo gives\n%s\nwant\n%s", got.Stored(), info.text)
	}
	if got.NarHash != content.sum || got.NarSize != content.size || got.HashPart() != hash {
		t.Errorf("Narinfo gives hash %x, size %d, path %s", got.NarHash, got.NarSize, got.StorePath)
	}

	// One pack is cached, the narinfo's, and nothing of the content was
	// asked of the bucket.
	if got, want := f.cache.Size(), packSize(1, len(info.text)); got != want {
		t.Errorf("the cache holds %d bytes, want %d: the narinfo's pack alone", got, want)
	}
	if _, ok := f.cache.Meta(info.root); !ok {
		t.Error("the narinfo's pack is not cached")
	}
	if n := f.bucket.gets("", content.root); n != 0 {
		t.Errorf("%d downloads of the content reference's pack, want none", n)
	}
	if idx, data := f.bucket.gets(".idx", info.root), f.bucket.gets(".data", info.root); idx != 1 || data != 1 {
		t.Errorf("%d index and %d data downloads of the narinfo's pack, want one each", idx, data)
	}
	rec, ok := f.cache.Record(f.store.NarinfoName(hash))
	if !ok || rec.Root != info.root || !slices.Equal(rec.Packs, []key.Key{info.root}) {
		t.Errorf("the record is %+v (%v), want root and pack %s", rec, ok, info.root)
	}

	// A second request reads the cached pack.
	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatalf("Narinfo again: %v", err)
	}
	if n := f.bucket.gets(""); n != 2 {
		t.Errorf("%d downloads after the second request, want the 2 of the first", n)
	}
}

func TestNarinfoNotFound(t *testing.T) {
	f := newFixture(t, config{})
	_, err := f.store.Narinfo(background(t), storeHash("nothing"))
	notFound(t, "Narinfo of an unknown path", err)

	// A reference that is deleted on the server is not found any more,
	// and its record goes.
	content := ingested(t, archive(t, smallTree(1)))
	hash := storeHash("one")
	f.pushNarinfo(narinfoOf(t, hash, "one", content))
	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	name := f.store.NarinfoName(hash)
	if _, ok := f.cache.Record(name); !ok {
		t.Fatal("no record after the narinfo was served")
	}
	f.remove(name)
	_, err = f.store.Narinfo(background(t), hash)
	notFound(t, "Narinfo of a deleted reference", err)
	if _, ok := f.cache.Record(name); ok {
		t.Error("the record of a reference the server does not know is still there")
	}
}

func TestHashesThatAreNoneAreNotAsked(t *testing.T) {
	var dials atomic.Int32
	rem := remote.New(remote.Config{
		Log: quiet,
		Dial: func(context.Context) (remote.Conn, error) {
			dials.Add(1)
			return nil, errors.New("no server in this test")
		},
	})
	defer rem.Close()
	cache, err := packcache.Open(t.TempDir(), 1<<20, packcache.Origin{Server: "test", Prefix: testPrefix})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	s := New(context.Background(), cache, rem, Options{Prefix: testPrefix, Log: quiet})
	defer s.Close()

	good := storeHash("x")
	for _, hash := range []string{"", "abc", good[:31], good + "0", strings.ToUpper(good), "../" + good[3:], good[:31] + "e", good[:31] + "/"} {
		if _, err := s.Narinfo(background(t), hash); !errors.Is(err, ErrNotFound) {
			t.Errorf("Narinfo(%q): %v, want ErrNotFound", hash, err)
		}
		if ok, err := s.HasNarinfo(background(t), hash); ok || err != nil {
			t.Errorf("HasNarinfo(%q) = %v, %v, want false", hash, ok, err)
		}
	}
	sum := sha256.Sum256([]byte("x"))
	goodNar := nix32.Encode(sum[:])
	for _, hash := range []string{"", good, goodNar[:51], goodNar + "0", "z" + goodNar[1:], strings.ToUpper(goodNar), goodNar[:50] + "/."} {
		if _, err := s.Content(background(t), hash); !errors.Is(err, ErrNotFound) {
			t.Errorf("Content(%q): %v, want ErrNotFound", hash, err)
		}
	}
	if n := dials.Load(); n != 0 {
		t.Errorf("the server was dialled %d times for hashes that are none", n)
	}
}

func TestNarinfoOffline(t *testing.T) {
	f := newFixture(t, config{})
	content := ingested(t, archive(t, smallTree(1)))
	cached, other := storeHash("cached"), storeHash("other")
	info := narinfoOf(t, cached, "cached", content)
	f.pushNarinfo(info)
	f.pushNarinfo(narinfoOf(t, other, "other", content))
	if _, err := f.store.Narinfo(background(t), cached); err != nil {
		t.Fatalf("Narinfo: %v", err)
	}

	f.srv.Stop()
	f.bucket.forget()
	got, err := f.store.Narinfo(background(t), cached)
	if err != nil {
		t.Fatalf("Narinfo with the server down: %v", err)
	}
	if !bytes.Equal(got.Stored(), info.text) {
		t.Errorf("Narinfo with the server down gives\n%s", got.Stored())
	}
	if ok, err := f.store.HasNarinfo(background(t), cached); !ok || err != nil {
		t.Errorf("HasNarinfo with the server down = %v, %v, want true", ok, err)
	}

	// What was never fetched is not found, and quickly.
	start := time.Now()
	_, err = f.store.Narinfo(background(t), other)
	notFound(t, "Narinfo of a path that is not cached, with the server down", err)
	if ok, err := f.store.HasNarinfo(background(t), other); ok || err != nil {
		t.Errorf("HasNarinfo of a path that is not cached = %v, %v, want false", ok, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("the answers took %s", took)
	}
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads with the server down, want none", n)
	}

	// Back again, the server is asked again.
	f.srv.Restart()
	eventually(t, "the other path is found after the restart", func() bool {
		_, err := f.store.Narinfo(background(t), other)
		return err == nil
	})
}

func TestHasNarinfoDownloadsNothing(t *testing.T) {
	f := newFixture(t, config{})
	content := ingested(t, archive(t, smallTree(1)))
	hash := storeHash("one")
	f.pushNarinfo(narinfoOf(t, hash, "one", content))
	f.bucket.forget()

	ok, err := f.store.HasNarinfo(background(t), hash)
	if !ok || err != nil {
		t.Fatalf("HasNarinfo = %v, %v, want true", ok, err)
	}
	ok, err = f.store.HasNarinfo(background(t), storeHash("nothing"))
	if ok || err != nil {
		t.Fatalf("HasNarinfo of an unknown path = %v, %v, want false", ok, err)
	}
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads, want none", n)
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes, want none", size)
	}
}

func TestNarinfoNamePointingAtADirectory(t *testing.T) {
	f := newFixture(t, config{})
	files := map[string]node{}
	for i := range 5 {
		files[fmt.Sprintf("part%d", i)] = file(noise(byte(40+i), 1<<20))
	}
	big := ingested(t, archive(t, dir(files)))
	hash := storeHash("directory")
	name := f.store.NarinfoName(hash)
	f.push(name, big.root, big.objs, 2)
	f.bucket.forget()

	_, err := f.store.Narinfo(background(t), hash)
	notFound(t, "Narinfo of a name that points at a directory", err)
	if ok, err := f.store.HasNarinfo(background(t), hash); ok || err != nil {
		t.Errorf("HasNarinfo = %v, %v, want false", ok, err)
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes, want none", size)
	}
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads, want none", n)
	}
	if !f.log.has("level=ERROR", name) {
		t.Error("the broken reference is not in the log as an error")
	}

	// The directory is the content of a store path as well, and cached as
	// that. A narinfo name that points at it takes nothing from the cache:
	// the pack is not that reference's.
	f.pushContent(big)
	f.serves(big)
	second := storeHash("directory again")
	f.push(f.store.NarinfoName(second), big.root, big.objs, 2)
	_, err = f.store.Narinfo(background(t), second)
	notFound(t, "Narinfo of a second name that points at the directory", err)
	if _, ok := f.cache.Meta(big.root); !ok {
		t.Error("the pack of the content reference was dropped for a narinfo name that points at it")
	}
	f.serves(big)
	f.cache.Drop(big.root)

	// A text of more than 1 MiB is no narinfo either: its root says how
	// long it is.
	long := textOf(t, storeHash("long"), bytes.Repeat([]byte("References: none\n"), 80_000))
	if long.root.Length() <= narinfo.MaxStored {
		t.Fatalf("the long text has %d bytes", long.root.Length())
	}
	f.pushNarinfo(long)
	f.bucket.forget()
	_, err = f.store.Narinfo(background(t), long.hash)
	notFound(t, "Narinfo of a text over 1 MiB", err)
	if ok, _ := f.store.HasNarinfo(background(t), long.hash); ok {
		t.Error("HasNarinfo of a text over 1 MiB is true")
	}
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads for a text over 1 MiB, want none", n)
	}
}

func TestAdmit(t *testing.T) {
	blob, err := key.New(key.Blob, 100, bytes.Repeat([]byte{1}, 100))
	if err != nil {
		t.Fatal(err)
	}
	other, err := key.New(key.Blob, 100, bytes.Repeat([]byte{2}, 100))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := key.New(key.DirLeaf, 1<<40, []byte("a directory"))
	if err != nil {
		t.Fatal(err)
	}
	dirNode, err := key.New(key.DirNode, 1<<40, []byte("a directory index"))
	if err != nil {
		t.Fatal(err)
	}
	index, err := key.New(key.FileNode, narinfo.MaxStored, []byte("a file index"))
	if err != nil {
		t.Fatal(err)
	}
	long, err := key.New(key.FileNode, narinfo.MaxStored+1, []byte("a file index"))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := key.New(key.Commit, 100, []byte("a commit"))
	if err != nil {
		t.Fatal(err)
	}
	pack := func(root key.Key, bytes uint64) client.Pack { return client.Pack{Root: root, Bytes: bytes} }
	// many is a pack of the given number of objects, most of them empty,
	// with the index that goes with them.
	many := func(root key.Key, objects uint64) client.Pack {
		return client.Pack{Root: root, Objects: objects, Bytes: 13, DataSize: 30, IndexSize: packfile.IndexSize(objects)}
	}

	for _, c := range []struct {
		name  string
		kind  kind
		root  key.Key
		packs []client.Pack
		ok    bool
	}{
		{"narinfo, a blob", kindNarinfo, blob, []client.Pack{pack(blob, 100)}, true},
		{"narinfo, a file index of 1 MiB", kindNarinfo, index, []client.Pack{pack(index, 1<<20)}, true},
		{"narinfo, a file index over 1 MiB", kindNarinfo, long, []client.Pack{pack(long, 1<<20)}, false},
		{"narinfo, a directory", kindNarinfo, leaf, []client.Pack{pack(leaf, 100)}, false},
		{"narinfo, a commit", kindNarinfo, commit, []client.Pack{pack(commit, 100)}, false},
		{"narinfo, packs of 4 MiB", kindNarinfo, blob, []client.Pack{pack(blob, 100), pack(other, 4<<20-100)}, true},
		{"narinfo, packs over 4 MiB", kindNarinfo, blob, []client.Pack{pack(blob, 100), pack(other, 4<<20-99)}, false},
		{"narinfo, sizes that wrap around", kindNarinfo, blob, []client.Pack{pack(blob, 1<<63), pack(other, 1<<63)}, false},
		{"narinfo, a pack of 1024 objects", kindNarinfo, blob, []client.Pack{many(blob, 1024)}, true},
		{"narinfo, a pack of 1025 objects", kindNarinfo, blob, []client.Pack{many(blob, 1025)}, false},
		{"narinfo, a parent pack of 1025 objects", kindNarinfo, blob, []client.Pack{pack(blob, 100), many(other, 1025)}, false},
		{"narinfo, an index of 704 MiB for 13 bytes of objects", kindNarinfo, blob, []client.Pack{many(blob, packfile.MaxEntries)}, false},
		{"content, a pack of very many objects", kindContent, leaf, []client.Pack{many(leaf, packfile.MaxEntries)}, true},
		{"content, a directory", kindContent, leaf, []client.Pack{pack(leaf, 1<<40)}, true},
		{"content, a directory index", kindContent, dirNode, []client.Pack{pack(dirNode, 1<<40), pack(leaf, 1<<40)}, true},
		{"content, a blob", kindContent, blob, []client.Pack{pack(blob, 100)}, false},
		{"content, a file index", kindContent, index, []client.Pack{pack(index, 100)}, false},
		{"content, a commit", kindContent, commit, []client.Pack{pack(commit, 100)}, false},
		{"no pack", kindContent, leaf, nil, false},
		{"three packs", kindContent, leaf, []client.Pack{pack(leaf, 1), pack(dirNode, 1), pack(blob, 1)}, false},
		{"a pack of another root", kindContent, leaf, []client.Pack{pack(dirNode, 1)}, false},
		{"its own parent", kindContent, leaf, []client.Pack{pack(leaf, 1), pack(leaf, 1)}, false},
	} {
		err := admit(c.kind, c.root, c.packs)
		if (err == nil) != c.ok {
			t.Errorf("%s: admit says %v", c.name, err)
		}
		if err != nil && !errors.Is(err, errBroken) {
			t.Errorf("%s: %v is not a broken reference", c.name, err)
		}
	}
}

func TestNarinfoForAnotherPath(t *testing.T) {
	f := newFixture(t, config{})
	content := ingested(t, archive(t, smallTree(1)))
	asked, other := storeHash("asked"), storeHash("other")
	// The text is the narinfo of another store path.
	misnamed := narinfoOf(t, other, "other", content)
	misnamed.hash = asked
	f.pushNarinfo(misnamed)
	f.bucket.forget()

	_, err := f.store.Narinfo(background(t), asked)
	notFound(t, "Narinfo whose text is of another path", err)
	downloads := f.bucket.gets("")
	if downloads == 0 {
		t.Fatal("the narinfo was refused without being read")
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes of a broken reference", size)
	}
	if !f.log.has("level=ERROR", f.store.NarinfoName(asked)) {
		t.Error("the broken reference is not in the log as an error")
	}

	// It is remembered: nothing is downloaded for it again.
	_, err = f.store.Narinfo(background(t), asked)
	notFound(t, "Narinfo of the broken reference, again", err)
	if ok, _ := f.store.HasNarinfo(background(t), asked); ok {
		t.Error("HasNarinfo of a reference found broken is true")
	}
	if n := f.bucket.gets(""); n != downloads {
		t.Errorf("%d downloads after the second request, want the %d of the first", n, downloads)
	}

	// The narinfo as Nix uploaded it, with the URL and compression of the
	// file it sent, is not what narrenhaus stores.
	xz := storeHash("xz")
	asNix := fmt.Appendf(nil, "StorePath: /nix/store/%s-xz\nURL: nar/%s.nar.xz\nCompression: xz\nNarHash: sha256:%s\nNarSize: %d\nReferences: \n",
		xz, content.hash, content.hash, content.size)
	if _, err := narinfo.Parse(asNix); err != nil {
		t.Fatalf("the narinfo of the test: %v", err)
	}
	f.pushNarinfo(textOf(t, xz, asNix))
	_, err = f.store.Narinfo(background(t), xz)
	notFound(t, "Narinfo with the URL of a compressed file", err)

	// Each of the two alone is as little what narrenhaus stores: the URL
	// of another NAR than the one the narinfo is of, and a compression
	// that the NAR behind the URL does not have.
	elsewhere := ingested(t, archive(t, smallTree(2)))
	for what, lines := range map[string]string{
		"the URL of another NAR":        fmt.Sprintf("URL: nar/%s.nar\nCompression: none\n", elsewhere.hash),
		"a compression other than none": fmt.Sprintf("URL: nar/%s.nar\nCompression: xz\n", content.hash),
	} {
		hash := storeHash(what)
		wrong := fmt.Appendf(nil, "StorePath: /nix/store/%s-wrong\n%sNarHash: sha256:%s\nNarSize: %d\nReferences: \n",
			hash, lines, content.hash, content.size)
		if _, err := narinfo.Parse(wrong); err != nil {
			t.Fatalf("the narinfo of the test with %s: %v", what, err)
		}
		f.pushNarinfo(textOf(t, hash, wrong))
		_, err = f.store.Narinfo(background(t), hash)
		notFound(t, "Narinfo with "+what, err)
		if !f.log.has("level=ERROR", f.store.NarinfoName(hash)) {
			t.Errorf("the narinfo with %s is not in the log as an error", what)
		}
	}

	// A text that is no narinfo.
	junk := storeHash("junk")
	f.pushNarinfo(textOf(t, junk, []byte("this is not a narinfo\n")))
	_, err = f.store.Narinfo(background(t), junk)
	notFound(t, "Narinfo of a text that is none", err)
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes of broken references", size)
	}
}

func TestContentRoundTrip(t *testing.T) {
	f := newFixture(t, config{})
	for _, c := range []struct {
		name string
		root node
	}{
		{"a directory", smallTree(1)},
		{"a file", text("just a file\n")},
		{"an executable", script("#!/bin/sh\n")},
		{"a symlink", link("/nix/store/elsewhere")},
		{"an empty file", text("")},
		{"an empty directory", dir(map[string]node{})},
		{"a file of several chunks", file(noise(9, 3<<20))},
		{"nested directories", dir(map[string]node{
			"a": dir(map[string]node{"b": dir(map[string]node{"c": text("deep\n"), "d": link("../..")})}),
			"B": text("sorts before a\n"),
			"é": text("sorts after z\n"),
			"z": file(noise(10, 70<<10)),
		})},
	} {
		t.Run(c.name, func(t *testing.T) {
			f.t = t
			r := ingested(t, archive(t, c.root))
			f.pushContent(r)
			f.bucket.forget()

			content, err := f.store.Content(background(t), r.hash)
			if err != nil {
				t.Fatalf("Content: %v", err)
			}
			defer content.Release()
			if content.NarSize() != uint64(len(r.archive)) {
				t.Errorf("NarSize = %d, want %d", content.NarSize(), len(r.archive))
			}
			var buf bytes.Buffer
			n, err := content.WriteTo(&buf)
			if err != nil || n != int64(len(r.archive)) {
				t.Fatalf("WriteTo = %d, %v, want %d", n, err, len(r.archive))
			}
			if !bytes.Equal(buf.Bytes(), r.archive) {
				t.Fatal("the archive that came out is not the one that went in")
			}

			name := f.store.ContentName(r.hash)
			rec, ok := f.cache.Record(name)
			if !ok || rec.Root != r.root || !rec.Checked || !slices.Equal(rec.Packs, []key.Key{r.root}) {
				t.Errorf("the record is %+v (%v), want root %s, checked", rec, ok, r.root)
			}

			// The second time it is read from the cache, and the server
			// is not asked: the record says where the name points.
			f.bucket.forget()
			f.serves(r)
			if n := f.bucket.gets(""); n != 0 {
				t.Errorf("%d downloads for a reference that is cached", n)
			}
		})
	}
}

func TestContentNotFound(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	_, err := f.store.Content(background(t), r.hash)
	notFound(t, "Content of an unknown hash", err)

	// A name that points at something that is no directory.
	blob := textOf(t, "", []byte("a file where a directory belongs\n"))
	f.push(f.store.ContentName(r.hash), blob.root, blob.objs, 2)
	f.bucket.forget()
	_, err = f.store.Content(background(t), r.hash)
	notFound(t, "Content of a name that points at a file", err)
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads for a root that is no directory, want none", n)
	}
	if !f.log.has("level=ERROR", f.store.ContentName(r.hash)) {
		t.Error("the broken reference is not in the log as an error")
	}
}

func TestContentOffline(t *testing.T) {
	f := newFixture(t, config{})
	cached := ingested(t, archive(t, smallTree(1)))
	other := ingested(t, archive(t, smallTree(2)))
	f.pushContent(cached)
	f.pushContent(other)
	f.serves(cached)

	f.srv.Stop()
	f.bucket.forget()
	f.serves(cached)
	_, err := f.store.Content(background(t), other.hash)
	notFound(t, "Content of a hash that is not cached, with the server down", err)
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads with the server down", n)
	}
}

// patched returns two store paths of which the second differs from the
// first in the files named.
func patched(t *testing.T, seed byte, changed ...int) (first, second *reference) {
	t.Helper()
	build := func(change bool) *reference {
		files := map[string]node{}
		for i := range 8 {
			s := seed + byte(i)
			if change && slices.Contains(changed, i) {
				s += 100
			}
			files[fmt.Sprintf("lib%d.so", i)] = file(noise(s, 128<<10))
		}
		return ingested(t, archive(t, dir(files)))
	}
	return build(false), build(true)
}

func TestContentOfAPatch(t *testing.T) {
	f := newFixture(t, config{})
	v1, v2 := patched(t, 1, 3)
	f.pushContent(v1)
	res := f.pushContent(v2)
	if res.Parent == nil || *res.Parent != v1.root {
		t.Fatalf("the second version was pushed with parent %v, want a patch of %s", res.Parent, v1.root)
	}
	f.bucket.forget()

	f.serves(v2)
	meta, ok := f.cache.Meta(v2.root)
	if !ok || meta.Complete || meta.Parent != v1.root {
		t.Errorf("the pack of the second version is cached as %+v (%v), want a patch of %s", meta, ok, v1.root)
	}
	meta, ok = f.cache.Meta(v1.root)
	if !ok || !meta.Complete {
		t.Errorf("the parent's pack is cached as %+v (%v), want a complete pack", meta, ok)
	}
	rec, ok := f.cache.Record(f.store.ContentName(v2.hash))
	if !ok || !rec.Checked || !slices.Equal(rec.Packs, []key.Key{v2.root, v1.root}) {
		t.Errorf("the record is %+v (%v), want the packs %s and %s", rec, ok, v2.root, v1.root)
	}
	if a, b := f.bucket.gets(".data", v2.root), f.bucket.gets(".data", v1.root); a != 1 || b != 1 {
		t.Errorf("%d downloads of the patch and %d of the parent, want one each", a, b)
	}

	// The parent is a pack like any other: the first version needs no
	// download now.
	f.serves(v1)
	if n := f.bucket.gets(".data", v1.root); n != 1 {
		t.Errorf("%d downloads of the parent after the first version was served", n)
	}
	// And the patch is read again from the two packs.
	f.serves(v2)
	if n := f.bucket.gets(".data"); n != 2 {
		t.Errorf("%d data downloads in all, want 2", n)
	}
}

func TestContentWhoseTreeDoesNotMatchItsRecipe(t *testing.T) {
	honest := func(t *testing.T) *reference { return ingested(t, archive(t, smallTree(1))) }
	for _, c := range []struct {
		name string
		// bad returns the reference to push and the hash to push it under.
		bad func(t *testing.T) (*reference, string)
	}{
		{"the recipe states another hash", func(t *testing.T) (*reference, string) {
			r := honest(t).stating(t, sha256.Sum256([]byte("another archive")), honest(t).size)
			return r, r.hash
		}},
		{"the recipe states another size", func(t *testing.T) (*reference, string) {
			h := honest(t)
			r := h.stating(t, h.sum, h.size+8)
			return r, r.hash
		}},
		{"the recipe states a smaller size", func(t *testing.T) (*reference, string) {
			h := honest(t)
			r := h.stating(t, h.sum, h.size-8)
			return r, r.hash
		}},
		{"the name is of another hash than the recipe's", func(t *testing.T) (*reference, string) {
			sum := sha256.Sum256([]byte("another archive"))
			return honest(t), nix32.Encode(sum[:])
		}},
		{"the root is not the two entries", func(t *testing.T) (*reference, string) {
			// The store path itself, without the directory around it.
			h := honest(t)
			bare := *h
			k, err := key.Parse(h.node.ContentKey)
			if err != nil {
				t.Fatal(err)
			}
			bare.root = k
			return &bare, h.hash
		}},
		{"the recipe is no recipe", func(t *testing.T) (*reference, string) {
			h := honest(t)
			return h.wrapped(t, []byte("not a recipe")), h.hash
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, config{})
			r, hash := c.bad(t)
			name := f.store.ContentName(hash)
			f.push(name, r.root, r.objs, 0.5)
			f.bucket.forget()

			_, err := f.store.Content(background(t), hash)
			notFound(t, "Content", err)
			downloads := f.bucket.gets("")
			if downloads == 0 {
				t.Error("the reference was refused without being read")
			}
			if size := f.cache.Size(); size != 0 {
				t.Errorf("the cache holds %d bytes of a broken reference", size)
			}
			if _, ok := f.cache.Record(name); ok {
				t.Error("a broken reference has a record")
			}
			if !f.log.has("level=ERROR", name) {
				t.Error("the broken reference is not in the log as an error")
			}

			_, err = f.store.Content(background(t), hash)
			notFound(t, "Content, again", err)
			if n := f.bucket.gets(""); n != downloads {
				t.Errorf("%d downloads after the second request, want the %d of the first", n, downloads)
			}
		})
	}
}

// corrupting is a set of objects of which one is not what its key says.
type corrupting struct {
	*objects
	victim key.Key
}

func (c corrupting) Get(k key.Key) ([]byte, error) {
	data, err := c.objects.Get(k)
	if err == nil && k == c.victim {
		data[len(data)/2] ^= 0x01
	}
	return data, err
}

func TestContentWithACorruptObject(t *testing.T) {
	f := newFixture(t, config{server: testserver.Options{NoVerify: true}})
	r := ingested(t, archive(t, smallTree(1)))
	// The largest object is a chunk of the file "data".
	var victim key.Key
	for k := range r.objs.m {
		if k.Type() == key.Blob && (k.Length() > victim.Length() || (k.Length() == victim.Length() && bytes.Compare(k[:], victim[:]) > 0)) {
			victim = k
		}
	}
	if victim.Length() < 1024 {
		t.Fatalf("the largest blob of the tree has %d bytes", victim.Length())
	}
	name := f.store.ContentName(r.hash)
	f.push(name, r.root, corrupting{r.objs, victim}, 0.5)
	f.bucket.forget()

	_, err := f.store.Content(background(t), r.hash)
	notFound(t, "Content with an object that is not what its key says", err)
	downloads := f.bucket.gets("")
	if downloads == 0 {
		t.Fatal("nothing was downloaded")
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes of a broken reference", size)
	}
	if left := f.tmpFiles(); len(left) != 0 {
		t.Errorf("left under tmp/: %v", left)
	}
	if !f.log.has("level=ERROR", name) {
		t.Error("the broken reference is not in the log as an error")
	}

	_, err = f.store.Content(background(t), r.hash)
	notFound(t, "Content, again", err)
	if n := f.bucket.gets(""); n != downloads {
		t.Errorf("%d downloads after the second request, want the %d of the first", n, downloads)
	}

	// The cure is to delete the reference and upload it again. A
	// narrenhaus that saw it gone looks at it afresh, although it has the
	// root it had.
	f.remove(name)
	_, err = f.store.Content(background(t), r.hash)
	notFound(t, "Content of the deleted reference", err)
	if res := f.pushContent(r); res.Stored {
		t.Fatal("the server kept the pack of a deleted reference")
	}
	f.serves(r)
}

// twoFaced is a set of objects that gives other bytes for one key the first
// time it is asked for it, and the true ones after that. A push reads an
// object with children twice: once to learn its children, once to pack it.
type twoFaced struct {
	*objects
	key   key.Key
	first []byte
	asked atomic.Bool
}

func (o *twoFaced) Get(k key.Key) ([]byte, error) {
	if k == o.key && !o.asked.Swap(true) {
		return bytes.Clone(o.first), nil
	}
	return o.objects.Get(k)
}

func TestContentOfAnIncompleteTree(t *testing.T) {
	// A pack that lacks an object of its tree. The server does not look,
	// and every object in the pack is what its key says: the reader finds
	// it out when it walks the tree.
	f := newFixture(t, config{server: testserver.Options{NoVerify: true}})
	r := ingested(t, archive(t, smallTree(1)))
	// The push is shown the store path's directory without its file
	// "data" when it lists the objects, and so packs all but that file.
	dirKey, err := key.Parse(r.node.ContentKey)
	if err != nil || dirKey.Type() != key.DirLeaf {
		t.Fatalf("the store path's directory is %s (%v), want a DirLeaf", dirKey, err)
	}
	whole, err := r.objs.Get(dirKey)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fstree.DecodeDirLeaf(whole)
	if err != nil {
		t.Fatal(err)
	}
	entries = slices.DeleteFunc(entries, func(e fstree.Entry) bool { return string(e.Name) == "data" })
	if len(entries) != 3 {
		t.Fatalf("%d entries are left of the directory, want 3", len(entries))
	}
	lacking, err := fstree.EncodeDirLeaf(entries)
	if err != nil {
		t.Fatal(err)
	}
	name := f.store.ContentName(r.hash)
	res := f.push(name, r.root, &twoFaced{objects: r.objs, key: dirKey, first: lacking.Bytes}, 0.5)
	if res.Objects == 0 || int(res.Objects) >= len(r.objs.m) {
		t.Fatalf("the pack holds %d objects of %d, want some and not all", res.Objects, len(r.objs.m))
	}
	f.bucket.forget()

	_, err = f.store.Content(background(t), r.hash)
	notFound(t, "Content of an incomplete tree", err)
	downloads := f.bucket.gets("")
	if downloads == 0 {
		t.Fatal("nothing was downloaded")
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes of a broken reference", size)
	}
	if !f.log.has("level=ERROR", name) {
		t.Error("the broken reference is not in the log as an error")
	}
	_, err = f.store.Content(background(t), r.hash)
	notFound(t, "Content, again", err)
	if n := f.bucket.gets(""); n != downloads {
		t.Errorf("%d downloads after the second request, want the %d of the first", n, downloads)
	}
}

func TestUnknownRecipeVersionIsNotBroken(t *testing.T) {
	f := newFixture(t, config{})
	h := ingested(t, archive(t, smallTree(1)))
	later := h.wrapped(t, fmt.Appendf(nil, `{"format":"narrenhaus-recipe","narHash":"sha256:%s","narSize":%d,"node":"nar","version":2}`, h.hash, h.size))
	name := f.store.ContentName(h.hash)
	f.push(name, later.root, later.objs, 0.5)
	f.bucket.forget()

	_, err := f.store.Content(background(t), h.hash)
	notFound(t, "Content with a recipe of version 2", err)
	if meta, ok := f.cache.Meta(later.root); !ok || !meta.Complete {
		t.Errorf("the pack is cached as %+v (%v), want it kept", meta, ok)
	}
	if f.log.has("level=ERROR") {
		t.Error("a recipe of a later version is in the log as an error")
	}
	if !f.log.has("level=WARN", name) {
		t.Error("a recipe of a later version is not in the log as a warning")
	}

	downloads := f.bucket.gets("")
	_, err = f.store.Content(background(t), h.hash)
	notFound(t, "Content with a recipe of version 2, again", err)
	if n := f.bucket.gets(""); n != downloads {
		t.Errorf("%d downloads after the second request, want the %d of the first", n, downloads)
	}
}

func TestConcurrentCallsShareOneDownload(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)
	f.bucket.forget()
	f.srv.SlowBucket(200 * time.Millisecond)

	const callers = 16
	errs := make(chan error, callers)
	for range callers {
		go func() {
			c, err := f.store.Content(background(t), r.hash)
			if err != nil {
				errs <- err
				return
			}
			defer c.Release()
			var buf bytes.Buffer
			if _, err := c.WriteTo(&buf); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(buf.Bytes(), r.archive) {
				errs <- errors.New("not the archive")
				return
			}
			errs <- nil
		}()
	}
	for range callers {
		if err := <-errs; err != nil {
			t.Errorf("a caller: %v", err)
		}
	}
	if idx, data := f.bucket.gets(".idx", r.root), f.bucket.gets(".data", r.root); idx != 1 || data != 1 {
		t.Errorf("%d index and %d data downloads for %d callers, want one each", idx, data, callers)
	}
}

func TestWaitEndsWorkGoesOn(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)
	f.bucket.forget()
	// The connection is up before the short wait begins.
	if ok, err := f.store.HasNarinfo(background(t), storeHash("nothing")); ok || err != nil {
		t.Fatalf("HasNarinfo = %v, %v", ok, err)
	}
	f.srv.SlowBucket(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.store.Content(ctx, r.hash)
	notFound(t, "Content with a wait of 50 ms", err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the error %v does not say that the wait ended", err)
	}
	if took := time.Since(start); took > 400*time.Millisecond {
		t.Errorf("the caller waited %s", took)
	}

	// The download went on without its caller, to its end.
	eventually(t, "the reference is local and checked", func() bool {
		rec, ok := f.cache.Record(f.store.ContentName(r.hash))
		return ok && rec.Checked
	})
	f.serves(r)
	if idx, data := f.bucket.gets(".idx", r.root), f.bucket.gets(".data", r.root); idx != 1 || data != 1 {
		t.Errorf("%d index and %d data downloads, want one each", idx, data)
	}
	rec, ok := f.cache.Record(f.store.ContentName(r.hash))
	if !ok || !rec.Checked {
		t.Errorf("the record is %+v (%v), want one that is checked", rec, ok)
	}

	// The same for a narinfo.
	hash := storeHash("one")
	info := narinfoOf(t, hash, "one", r)
	f.srv.SlowBucket(0)
	f.pushNarinfo(info)
	f.bucket.forget()
	f.srv.SlowBucket(500 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = f.store.Narinfo(ctx, hash)
	notFound(t, "Narinfo with a wait of 50 ms", err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the error %v does not say that the wait ended", err)
	}
	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if n := f.bucket.gets(".data", info.root); n != 1 {
		t.Errorf("%d data downloads of the narinfo, want one", n)
	}
}

// stall holds up the downloads of the data of the packs of roots until
// release is called, and tells of each on arrived.
func (f *fixture) stall(roots ...key.Key) (arrived <-chan key.Key, release func()) {
	seen := make(chan key.Key, 16)
	gate := make(chan struct{})
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		for _, root := range roots {
			if r.Method == http.MethodGet && of(r, root, ".data") {
				seen <- root
				select {
				case <-gate:
				case <-r.Context().Done():
				}
			}
		}
		return false
	})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	f.t.Cleanup(release)
	return seen, release
}

func TestNarinfoIsNotBehindContentDownloads(t *testing.T) {
	f := newFixture(t, config{options: Options{ContentDownloads: 1}})
	first := ingested(t, archive(t, smallTree(1)))
	second := ingested(t, archive(t, smallTree(2)))
	f.pushContent(first)
	f.pushContent(second)
	hash := storeHash("one")
	f.pushNarinfo(narinfoOf(t, hash, "one", first))
	f.bucket.forget()

	arrived, release := f.stall(first.root, second.root)
	done := make(chan error, 2)
	get := func(r *reference) {
		got, err := f.nar(r.hash)
		if err == nil && !bytes.Equal(got, r.archive) {
			err = errors.New("not the archive")
		}
		done <- err
	}
	go get(first)
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the content download never began")
	}
	go get(second)

	// The one content download that may run is held up, and a narinfo is
	// answered all the same.
	start := time.Now()
	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatalf("Narinfo while a content download is held up: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the narinfo took %s", took)
	}

	// The second content download waits for the first: nothing of it was
	// asked of the bucket.
	time.Sleep(200 * time.Millisecond)
	if n := f.bucket.gets("", second.root); n != 0 {
		t.Errorf("%d downloads of the second content while the first holds the one place", n)
	}
	select {
	case err := <-done:
		t.Fatalf("a content request returned while its download was held up: %v", err)
	default:
	}

	release()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Content: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("a content request never returned")
		}
	}
}

func TestCachedPatchWhoseParentChanged(t *testing.T) {
	t.Run("the server names the root as a base pack", func(t *testing.T) {
		f := newFixture(t, config{})
		v1, v2 := patched(t, 1, 0)
		f.pushContent(v1)
		if res := f.pushContent(v2); res.Parent == nil || *res.Parent != v1.root {
			t.Fatalf("the second version was pushed with parent %v, want a patch of %s", res.Parent, v1.root)
		}
		f.serves(v2)
		if meta, ok := f.cache.Meta(v2.root); !ok || meta.Complete || meta.Parent != v1.root {
			t.Fatalf("the pack is cached as %+v (%v), want a patch of %s", meta, ok, v1.root)
		}

		// The parent leaves the cache, as an eviction takes it, and on
		// the server the reference is written anew as a pack of its own.
		f.cache.Drop(v1.root)
		name := f.store.ContentName(v2.hash)
		f.remove(name)
		if res := f.push(name, v2.root, v2.objs, 2); res.Stored || res.Parent != nil {
			t.Fatalf("the second push gave %+v, want a new base pack", res)
		}
		f.bucket.forget()

		f.serves(v2)
		if meta, ok := f.cache.Meta(v2.root); !ok || !meta.Complete {
			t.Errorf("the pack is cached as %+v (%v), want a complete pack", meta, ok)
		}
		if n := f.bucket.gets(".data", v2.root); n != 1 {
			t.Errorf("%d downloads of the pack, want one", n)
		}
		if n := f.bucket.gets("", v1.root); n != 0 {
			t.Errorf("%d downloads of the pack that is the parent no more", n)
		}
		rec, ok := f.cache.Record(name)
		if !ok || !rec.Checked || !slices.Equal(rec.Packs, []key.Key{v2.root}) {
			t.Errorf("the record is %+v (%v), want the one pack %s", rec, ok, v2.root)
		}
	})

	t.Run("the server names another parent", func(t *testing.T) {
		f := newFixture(t, config{})
		// v2 differs from v1 in two files and from v3 in one.
		v1, v2 := patched(t, 1, 0, 1)
		_, v3 := patched(t, 1, 0, 1, 7)

		f.pushContent(v1)
		if res := f.pushContent(v2); res.Parent == nil || *res.Parent != v1.root {
			t.Fatalf("the second version was pushed with parent %v, want a patch of %s", res.Parent, v1.root)
		}
		f.serves(v2)

		f.cache.Drop(v1.root)
		name := f.store.ContentName(v2.hash)
		f.remove(name)
		f.push(f.store.ContentName(v3.hash), v3.root, v3.objs, 2)
		if res := f.pushContent(v2); res.Parent == nil || *res.Parent != v3.root {
			t.Fatalf("the second push has parent %v, want a patch of %s", res.Parent, v3.root)
		}
		f.bucket.forget()

		f.serves(v2)
		if meta, ok := f.cache.Meta(v2.root); !ok || meta.Complete || meta.Parent != v3.root {
			t.Errorf("the pack is cached as %+v (%v), want a patch of %s", meta, ok, v3.root)
		}
		if a, b := f.bucket.gets(".data", v2.root), f.bucket.gets(".data", v3.root); a != 1 || b != 1 {
			t.Errorf("%d downloads of the patch and %d of its new parent, want one each", a, b)
		}
		if n := f.bucket.gets("", v1.root); n != 0 {
			t.Errorf("%d downloads of the pack that is the parent no more", n)
		}
		rec, ok := f.cache.Record(name)
		if !ok || !rec.Checked || !slices.Equal(rec.Packs, []key.Key{v2.root, v3.root}) {
			t.Errorf("the record is %+v (%v), want the packs %s and %s", rec, ok, v2.root, v3.root)
		}
	})

	t.Run("the parent is fetched again when it was evicted", func(t *testing.T) {
		f := newFixture(t, config{})
		v1, v2 := patched(t, 1, 0)
		f.pushContent(v1)
		if res := f.pushContent(v2); res.Parent == nil {
			t.Fatal("the second version is no patch")
		}
		f.serves(v2)
		f.cache.Drop(v1.root)
		f.bucket.forget()

		// The patch is as the server names it: it stays, and its parent
		// comes again. The archive is not rebuilt a second time to be
		// compared: the record says that it was.
		f.serves(v2)
		if a, b := f.bucket.gets(".data", v2.root), f.bucket.gets(".data", v1.root); a != 0 || b != 1 {
			t.Errorf("%d downloads of the patch and %d of the parent, want none and one", a, b)
		}
		if n := f.log.count("rebuilding the archive"); n != 1 {
			t.Errorf("the archive was rebuilt %d times, want once", n)
		}
	})
}

func TestExpiredURLIsLookedUpAgain(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)
	hash := storeHash("one")
	info := narinfoOf(t, hash, "one", r)
	f.pushNarinfo(info)
	f.bucket.forget()

	// expire answers the next n downloads matching ext as a bucket
	// answers a URL that has expired.
	expire := func(n int32, ext string) *atomic.Int32 {
		var left atomic.Int32
		left.Store(n)
		f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, ext) || left.Add(-1) < 0 {
				return false
			}
			http.Error(w, "the URL has expired", http.StatusForbidden)
			return true
		})
		return &left
	}

	expire(1, ".idx")
	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatalf("Narinfo when the first URL had expired: %v", err)
	}
	if n := f.bucket.gets(".idx", info.root); n != 2 {
		t.Errorf("%d downloads of the index, want 2: the refused one and one more", n)
	}

	expire(1, ".data")
	f.serves(r)
	if n := f.bucket.gets(".data", r.root); n != 2 {
		t.Errorf("%d downloads of the data, want 2: the refused one and one more", n)
	}

	// URLs that are refused again after the second lookup: the reference
	// is not served, and it is not broken either.
	other := ingested(t, archive(t, smallTree(2)))
	f.bucket.answer(nil)
	f.pushContent(other)
	f.bucket.forget()
	expire(1000, ".data")
	_, err := f.store.Content(background(t), other.hash)
	notFound(t, "Content whose URLs are refused", err)
	if n := f.bucket.gets(".data", other.root); n != 2 {
		t.Errorf("%d downloads of the data, want 2: one for each lookup", n)
	}
	if f.log.has("level=ERROR") {
		t.Error("a refused transfer is in the log as an error")
	}
	if left := f.tmpFiles(); len(left) != 0 {
		t.Errorf("left under tmp/: %v", left)
	}
	f.bucket.answer(nil)
	f.serves(other)
}

func TestTransferFailureIsNotBroken(t *testing.T) {
	f := newFixture(t, config{})
	v1, v2 := patched(t, 1, 2)
	f.pushContent(v1)
	if res := f.pushContent(v2); res.Parent == nil {
		t.Fatal("the second version is no patch")
	}
	f.bucket.forget()

	// The patch arrives; the parent's data is cut before its first byte.
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !of(r, v1.root, ".data") {
			return false
		}
		w.Header().Set("Content-Length", "100000")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		return true
	})
	_, err := f.store.Content(background(t), v2.hash)
	notFound(t, "Content whose parent's download is cut", err)
	if f.log.has("level=ERROR") {
		t.Error("a cut transfer is in the log as an error")
	}
	if !f.log.has("level=WARN", f.store.ContentName(v2.hash)) {
		t.Error("a cut transfer is not in the log as a warning")
	}
	// The pack that was verified is kept, the one being downloaded is
	// discarded.
	if meta, ok := f.cache.Meta(v2.root); !ok || meta.Complete {
		t.Errorf("the patch is cached as %+v (%v), want it kept", meta, ok)
	}
	if _, ok := f.cache.Meta(v1.root); ok {
		t.Error("the parent whose download was cut is cached")
	}
	if left := f.tmpFiles(); len(left) != 0 {
		t.Errorf("left under tmp/: %v", left)
	}

	// A bucket that answers with an error.
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet {
			return false
		}
		http.Error(w, "the bucket is unwell", http.StatusInternalServerError)
		return true
	})
	_, err = f.store.Content(background(t), v2.hash)
	notFound(t, "Content while the bucket fails", err)

	// Nothing of it is remembered: the next request fetches what is
	// missing, the parent alone.
	f.bucket.answer(nil)
	f.bucket.forget()
	f.serves(v2)
	if a, b := f.bucket.gets(".data", v2.root), f.bucket.gets(".data", v1.root); a != 0 || b != 1 {
		t.Errorf("%d downloads of the patch and %d of the parent, want none and one", a, b)
	}
}

func TestNarinfoWithAFileNodeRoot(t *testing.T) {
	f := newFixture(t, config{})
	content := ingested(t, archive(t, smallTree(1)))
	hash := storeHash("many")

	// A narinfo of some 900 KiB with thousands of references, as varied
	// as store paths are. The chunker cuts where eight bytes in a row are
	// near its pattern, which the text of a narinfo next to never is: its
	// characters are too much alike. A run of the letter j is such a
	// place, so one reference in the middle has a name of them, and the
	// text is cut there.
	var references []string
	for len(references) < 21_000 {
		n := len(references)
		name := fmt.Sprintf("dep-%d", n)
		if n == 10_000 {
			name = strings.Repeat("j", 24)
		}
		references = append(references, storeHash(fmt.Sprint("dep", n))+"-"+name)
	}
	info := narinfoOf(t, hash, "many", content, references...)
	if len(info.text) < 800<<10 || len(info.text) > narinfo.MaxStored {
		t.Fatalf("the narinfo has %d bytes", len(info.text))
	}
	if info.root.Type() != key.FileNode {
		t.Fatalf("the root of a narinfo of %d bytes is a %s, want a FileNode: the chunker did not cut it", len(info.text), info.root.Type())
	}
	t.Logf("a narinfo of %d bytes with %d references in %d objects", len(info.text), len(references), len(info.objs.m))
	f.pushNarinfo(info)
	f.bucket.forget()

	if ok, err := f.store.HasNarinfo(background(t), hash); !ok || err != nil {
		t.Fatalf("HasNarinfo = %v, %v, want true", ok, err)
	}
	got, err := f.store.Narinfo(background(t), hash)
	if err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if !bytes.Equal(got.Stored(), info.text) {
		t.Error("the narinfo that came out is not the one that went in")
	}
	if len(got.References) != len(references) {
		t.Errorf("%d references, want %d", len(got.References), len(references))
	}
	if n := f.bucket.gets(".data"); n != 1 {
		t.Errorf("%d data downloads, want one", n)
	}
}

func TestPlaceUploaded(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	name := f.store.ContentName(r.hash)

	// What an upload does when its push is over: the staged file becomes
	// the pack of the root.
	place := func() {
		t.Helper()
		w, err := f.cache.NewStaged()
		if err != nil {
			t.Fatal(err)
		}
		for k, data := range r.objs.m {
			if err := w.Add(k, data); err != nil {
				t.Fatal(err)
			}
		}
		sealed, err := w.Seal()
		if err != nil {
			t.Fatal(err)
		}
		hold := f.cache.Reserve(r.root)
		defer hold.Release()
		if err := f.cache.Place(sealed, r.root, nil); err != nil {
			t.Fatal(err)
		}
		if err := f.store.PlaceUploaded(name, r.root); err != nil {
			t.Fatalf("PlaceUploaded: %v", err)
		}
	}
	place()
	rec, ok := f.cache.Record(name)
	if !ok || rec.Root != r.root || !rec.Checked || !slices.Equal(rec.Packs, []key.Key{r.root}) {
		t.Fatalf("the record is %+v (%v), want root and pack %s, checked", rec, ok, r.root)
	}

	// It is served from there: the server, which does not have the
	// reference in this test, is not asked, and nothing is downloaded.
	f.serves(r)
	if n := f.bucket.gets(""); n != 0 {
		t.Errorf("%d downloads for an uploaded reference", n)
	}

	// A reference that was found broken and is then uploaded from here is
	// served again: what was remembered of it is forgotten.
	f.cache.Drop(r.root)
	f.store.refuse(ref{kind: kindContent, name: name, hash: r.hash, root: r.root}, true, errors.New("for the test"))
	f.pushContent(r)
	_, err := f.store.Content(background(t), r.hash)
	notFound(t, "Content of a reference that is remembered as broken", err)
	place()
	f.serves(r)

	// A pack that is gone again before it is recorded leaves no record
	// and no error.
	f.cache.Drop(r.root)
	if err := f.store.PlaceUploaded(name, r.root); err != nil {
		t.Errorf("PlaceUploaded of a pack that is gone: %v", err)
	}
	if _, ok := f.cache.Record(name); ok {
		t.Error("a record without a pack")
	}
}

func TestHeldPacksAreNotEvicted(t *testing.T) {
	// A cache whose limit is below any pack keeps a pack while a request
	// reads it, and no longer.
	f := newFixture(t, config{limit: 1})
	v1, v2 := patched(t, 1, 4)
	f.pushContent(v1)
	if res := f.pushContent(v2); res.Parent == nil {
		t.Fatal("the second version is no patch")
	}
	hash := storeHash("one")
	f.pushNarinfo(narinfoOf(t, hash, "one", v2))

	c, err := f.store.Content(background(t), v2.hash)
	if err != nil {
		t.Fatalf("Content: %v", err)
	}
	if _, ok := f.cache.Meta(v2.root); !ok {
		t.Error("the pack of a held reference is not cached")
	}
	if _, ok := f.cache.Meta(v1.root); !ok {
		t.Error("the parent of a held reference is not cached")
	}
	var buf bytes.Buffer
	if _, err := c.WriteTo(&buf); err != nil || !bytes.Equal(buf.Bytes(), v2.archive) {
		t.Fatalf("WriteTo: %v, %d bytes", err, buf.Len())
	}
	c.Release()
	c.Release()
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes after the release, the limit is 1", size)
	}
	if _, err := c.WriteTo(io.Discard); err == nil {
		t.Error("WriteTo after Release succeeds")
	}

	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes after the narinfo was read, the limit is 1", size)
	}
}

// failing is a writer that fails after it took limit bytes.
type failing struct {
	limit int
	n     int
}

var errSink = errors.New("the client went away")

func (w *failing) Write(p []byte) (int, error) {
	if w.n+len(p) > w.limit {
		return 0, errSink
	}
	w.n += len(p)
	return len(p), nil
}

func TestWriteTo(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)

	// A destination that fails: its error comes back, and the cache is
	// left alone.
	c, err := f.store.Content(background(t), r.hash)
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.WriteTo(&failing{limit: 1000})
	if !errors.Is(err, errSink) || n > 1000 {
		t.Errorf("WriteTo into a failing destination = %d, %v", n, err)
	}
	c.Release()
	if _, ok := f.cache.Meta(r.root); !ok {
		t.Error("the pack is gone after a client went away")
	}
	if f.log.has("level=ERROR") {
		t.Error("a client that went away is in the log as an error")
	}

	// An archive that would come out longer or shorter than the length
	// that was announced is not written to its end, and no byte beyond
	// the announced length is written.
	for _, delta := range []int{-8, 8} {
		c, err := f.store.Content(background(t), r.hash)
		if err != nil {
			t.Fatal(err)
		}
		c.narSize = uint64(int(c.narSize) + delta)
		var buf bytes.Buffer
		n, err := c.WriteTo(&buf)
		if err == nil {
			t.Errorf("WriteTo of an archive %d bytes off its length succeeds", delta)
		}
		if n != int64(buf.Len()) || uint64(n) > c.narSize {
			t.Errorf("WriteTo returned %d and wrote %d bytes, announced were %d", n, buf.Len(), c.narSize)
		}
		if !bytes.HasPrefix(r.archive, buf.Bytes()) {
			t.Error("what was written is no beginning of the archive")
		}
		c.Release()
	}
	if !f.log.has("level=ERROR", "defect") {
		t.Error("an archive that is not of its length is not in the log as a defect")
	}
}

func TestDamagedCacheIsFetchedAgain(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)
	hash := storeHash("one")
	info := narinfoOf(t, hash, "one", r)
	f.pushNarinfo(info)
	f.serves(r)
	if _, err := f.store.Narinfo(background(t), hash); err != nil {
		t.Fatal(err)
	}

	// The files of both packs lose their bytes, as a disk may lose them.
	packsDir := filepath.Join(filepath.Dir(f.cache.TempDir()), "packs")
	packs, err := os.ReadDir(packsDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range packs {
		if strings.HasSuffix(e.Name(), ".data") {
			path := filepath.Join(packsDir, e.Name())
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, make([]byte, st.Size()), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.bucket.forget()

	// What cannot be read as what it was is not served, and after at
	// most one request that is not answered the packs are there again.
	eventually(t, "the narinfo is served again", func() bool {
		got, err := f.store.Narinfo(background(t), hash)
		if err != nil {
			notFound(t, "Narinfo from a damaged cache", err)
			return false
		}
		if !bytes.Equal(got.Stored(), info.text) {
			t.Fatal("a damaged narinfo was served")
		}
		return true
	})
	eventually(t, "the content is served again", func() bool {
		c, err := f.store.Content(background(t), r.hash)
		if err != nil {
			notFound(t, "Content from a damaged cache", err)
			return false
		}
		defer c.Release()
		var buf bytes.Buffer
		if _, err := c.WriteTo(&buf); err != nil {
			return false
		}
		if !bytes.Equal(buf.Bytes(), r.archive) {
			t.Fatal("a damaged archive was served")
		}
		return true
	})
	if a, b := f.bucket.gets(".data", info.root), f.bucket.gets(".data", r.root); a != 1 || b != 1 {
		t.Errorf("%d downloads of the narinfo and %d of the content, want one each", a, b)
	}
}

func TestBrokenIsJudgedOnFetchedPacks(t *testing.T) {
	// A pack in the cache that lacks an object of its tree, as no
	// download leaves one: the reference on the server is sound. It is
	// not called broken for what the cache holds. The pack is dropped
	// and fetched, and the request that found it so is served.
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)
	w, err := f.cache.NewDownload()
	if err != nil {
		t.Fatal(err)
	}
	for k, data := range r.objs.m {
		if k.Type() == key.Blob && k.Length() > 1024 {
			continue
		}
		if err := w.Add(k, data); err != nil {
			t.Fatal(err)
		}
	}
	sealed, err := w.Seal()
	if err != nil {
		t.Fatal(err)
	}
	hold := f.cache.Reserve(r.root)
	if err := f.cache.Place(sealed, r.root, nil); err != nil {
		t.Fatal(err)
	}
	hold.Release()
	f.bucket.forget()

	f.serves(r)
	if f.log.has("level=ERROR") {
		t.Error("a reference is called broken for what the cache holds")
	}
	if !f.log.has("level=WARN", f.store.ContentName(r.hash)) {
		t.Error("the pack that was dropped is not in the log")
	}
	if n := f.bucket.gets(".data", r.root); n != 1 {
		t.Errorf("%d downloads of the pack, want one", n)
	}

	// A reference that is broken on the server is judged all the same,
	// once the pack that was cached has been fetched: here the cached pack
	// is whole, and the reference's recipe is none.
	broken, brokenRoot := f.withoutRecipe(r, "broken, with its parent cached")
	_, err = f.store.Content(background(t), broken)
	notFound(t, "Content of a broken patch of a cached pack", err)
	if !f.log.has("level=ERROR", f.store.ContentName(broken)) {
		t.Error("the broken reference is not in the log as an error")
	}
	if _, ok := f.cache.Meta(brokenRoot); ok {
		t.Error("the pack of the broken reference is still cached")
	}
	if meta, ok := f.cache.Meta(r.root); !ok || !meta.Complete {
		t.Errorf("the parent of the broken reference is cached as %+v (%v), want it kept", meta, ok)
	}
	if n := f.bucket.gets(".data", r.root); n != 2 {
		t.Errorf("%d downloads of the parent, want one more to pass the verdict on", n)
	}
	f.serves(r)
}

func TestCloseEndsTheWork(t *testing.T) {
	f := newFixture(t, config{})
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)
	f.bucket.forget()

	arrived, _ := f.stall(r.root)
	done := make(chan error, 1)
	go func() {
		_, err := f.store.Content(background(t), r.hash)
		done <- err
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the download never began")
	}

	closed := make(chan struct{})
	go func() {
		f.store.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close does not return while a download is held up")
	}
	select {
	case err := <-done:
		notFound(t, "Content when the store is closed", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not return when the store was closed")
	}
	if left := f.tmpFiles(); len(left) != 0 {
		t.Errorf("left under tmp/: %v", left)
	}
	if f.log.has("level=ERROR") {
		t.Error("the end of the work is in the log as an error")
	}

	// Nothing is begun after it.
	_, err := f.store.Content(background(t), r.hash)
	notFound(t, "Content after Close", err)
	f.store.Close()
}

func TestStoreContextEndsTheWork(t *testing.T) {
	// The store of this test runs under a context of the test's own.
	f := newFixture(t, config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := New(ctx, f.cache, f.rem, Options{Prefix: testPrefix, Log: quiet})
	defer store.Close()
	r := ingested(t, archive(t, smallTree(1)))
	f.pushContent(r)

	arrived, _ := f.stall(r.root)
	done := make(chan error, 1)
	go func() {
		_, err := store.Content(background(t), r.hash)
		done <- err
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the download never began")
	}
	cancel()
	select {
	case err := <-done:
		notFound(t, "Content when the store's context ends", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not return when the store's context ended")
	}
}

// corruptPatch pushes r, a patch, under the name of its hash with one
// object of its own pack that is not what its key says.
func (f *fixture) corruptPatch(r, parent *reference) {
	f.t.Helper()
	var victim key.Key
	for k := range r.objs.m {
		if _, shared := parent.objs.m[k]; shared || k.Type() != key.Blob {
			continue
		}
		if k.Length() > victim.Length() || (k.Length() == victim.Length() && bytes.Compare(k[:], victim[:]) > 0) {
			victim = k
		}
	}
	if victim.Length() < 1024 {
		f.t.Fatalf("the largest blob of the patch has %d bytes", victim.Length())
	}
	if res := f.push(f.store.ContentName(r.hash), r.root, corrupting{r.objs, victim}, 0.5); res.Parent == nil || *res.Parent != parent.root {
		f.t.Fatalf("the corrupt reference was pushed with parent %v, want a patch of %s", res.Parent, parent.root)
	}
}

// withoutRecipe pushes a reference with the tree of base and a recipe that
// is none, a patch of base that is found broken only when it is checked,
// and returns the hash it is named by and its root.
func (f *fixture) withoutRecipe(base *reference, seed string) (hash string, root key.Key) {
	f.t.Helper()
	broken := base.wrapped(f.t, []byte("not a recipe"))
	sum := sha256.Sum256([]byte(seed))
	hash = nix32.Encode(sum[:])
	if res := f.push(f.store.ContentName(hash), broken.root, broken.objs, 0.5); res.Parent == nil || *res.Parent != base.root {
		f.t.Fatalf("the reference without a recipe was pushed with parent %v, want a patch of %s", res.Parent, base.root)
	}
	return hash, broken.root
}

// checksBusy takes every place of a check, as eight checks under way do,
// and returns how to give n of them back.
func (f *fixture) checksBusy() (free func(n int)) {
	f.t.Helper()
	for range cap(f.store.checkPlaces) {
		f.store.checkPlaces <- struct{}{}
	}
	left := cap(f.store.checkPlaces)
	free = func(n int) {
		for ; n > 0 && left > 0; n, left = n-1, left-1 {
			<-f.store.checkPlaces
		}
	}
	f.t.Cleanup(func() { free(left) })
	return free
}

// asked asks for the content of hash on a goroutine of its own and gives
// the archive that was served.
type answer struct {
	archive []byte
	err     error
}

func (f *fixture) asked(hash string) <-chan answer {
	out := make(chan answer, 1)
	go func() {
		c, err := f.store.Content(background(f.t), hash)
		if err != nil {
			out <- answer{err: err}
			return
		}
		defer c.Release()
		var buf bytes.Buffer
		_, err = c.WriteTo(&buf)
		out <- answer{archive: buf.Bytes(), err: err}
	}()
	return out
}

// served fails the test unless a is the archive of r.
func (f *fixture) served(what string, r *reference, a answer) {
	f.t.Helper()
	if a.err != nil {
		f.t.Fatalf("%s is not served: %v", what, a.err)
	}
	if !bytes.Equal(a.archive, r.archive) {
		f.t.Fatalf("%s: %d bytes were served that are not the archive of %d bytes", what, len(a.archive), len(r.archive))
	}
}

// A sound reference and a broken one that are patches of one parent, asked
// for at once, as Nix asks for the paths of a closure. The sound one has
// its packs and waits for its check while the broken one is judged.
func TestBrokenSiblingDoesNotCondemnASoundReference(t *testing.T) {
	f := newFixture(t, config{server: testserver.Options{NoVerify: true}})
	v1, x := patched(t, 1, 4)
	_, y := patched(t, 1, 5)
	f.pushContent(v1)
	if res := f.pushContent(x); res.Parent == nil || *res.Parent != v1.root {
		t.Fatal("x is no patch of v1")
	}
	f.corruptPatch(y, v1)
	f.bucket.forget()

	free := f.checksBusy()
	sound := f.asked(x.hash)
	eventually(t, "x has both packs local and waits for its check", func() bool {
		_, own := f.cache.Meta(x.root)
		_, parent := f.cache.Meta(v1.root)
		return own && parent
	})

	_, err := f.store.Content(background(t), y.hash)
	notFound(t, "Content of the broken sibling", err)
	if !f.log.has("level=ERROR", f.store.ContentName(y.hash)) {
		t.Error("the broken sibling is not in the log as an error")
	}
	if _, ok := f.cache.Meta(v1.root); !ok {
		t.Error("the parent, which the sound reference reads, was dropped with the broken one")
	}

	free(cap(f.store.checkPlaces))
	f.served("the sound reference, asked for while its sibling was judged", x, <-sound)
	if f.log.has("level=ERROR", f.store.ContentName(x.hash)) {
		t.Error("the sound reference is in the log as broken")
	}
	f.serves(x)
	if n := f.bucket.gets(".data", v1.root); n != 1 {
		t.Errorf("%d downloads of the parent, want one", n)
	}
}

// A broken patch of a parent that is cached is not judged on the cached
// pack: the parent is dropped and fetched again, under whoever reads it,
// and then the verdict is passed, all within the one request. A sound
// patch of the same parent that is being made local meanwhile is served.
func TestDeferredVerdictIsPassedInTheSameWork(t *testing.T) {
	f := newFixture(t, config{})
	v1, x := patched(t, 1, 4)
	f.pushContent(v1)
	if res := f.pushContent(x); res.Parent == nil || *res.Parent != v1.root {
		t.Fatal("x is no patch of v1")
	}
	broken, brokenRoot := f.withoutRecipe(v1, "the broken sibling")
	f.serves(v1)
	f.bucket.forget()

	// Both have their packs and wait for a check, the broken one first.
	free := f.checksBusy()
	judged := f.asked(broken)
	eventually(t, "the broken reference has its pack", func() bool { _, ok := f.cache.Meta(brokenRoot); return ok })
	sound := f.asked(x.hash)
	eventually(t, "x has its pack", func() bool { _, ok := f.cache.Meta(x.root); return ok })
	free(1)

	notFound(t, "Content of the broken reference", (<-judged).err)
	if !f.log.has("level=ERROR", f.store.ContentName(broken)) {
		t.Error("the broken reference was not judged in the request that found it: it is not in the log as an error")
	}
	f.served("the sound sibling, asked for while the parent was dropped and fetched again", x, <-sound)
	if f.log.has("level=ERROR", f.store.ContentName(x.hash)) {
		t.Error("the sound reference is in the log as broken")
	}
	if meta, ok := f.cache.Meta(v1.root); !ok || !meta.Complete {
		t.Errorf("the parent is cached as %+v (%v), want it there", meta, ok)
	}
	if n := f.bucket.gets(".data", v1.root); n != 1 {
		t.Errorf("%d downloads of the parent to pass the verdict, want one", n)
	}

	// It is remembered, and the parent is not dropped for it a second time.
	downloads := f.bucket.gets("")
	_, err := f.store.Content(background(t), broken)
	notFound(t, "Content of the broken reference, again", err)
	f.serves(v1)
	f.serves(x)
	if n := f.bucket.gets(""); n != downloads {
		t.Errorf("%d downloads after the verdict, want the %d before", n, downloads)
	}
}

// A pack that is dropped under a reference that has it and is not served
// yet is fetched again by the work on that reference; the request is not
// told that there is nothing.
func TestPackDroppedUnderAReferenceIsFetchedAgain(t *testing.T) {
	t.Run("the parent of a patch", func(t *testing.T) {
		f := newFixture(t, config{})
		v1, x := patched(t, 1, 4)
		f.pushContent(v1)
		f.pushContent(x)
		f.bucket.forget()

		free := f.checksBusy()
		sound := f.asked(x.hash)
		eventually(t, "x has both packs local and waits for its check", func() bool {
			_, own := f.cache.Meta(x.root)
			_, parent := f.cache.Meta(v1.root)
			return own && parent
		})
		f.cache.Drop(v1.root)
		free(1)

		f.served("the patch whose parent was dropped", x, <-sound)
		if f.log.has("level=ERROR") {
			t.Error("a reference whose parent was dropped is in the log as an error")
		}
		if n := f.bucket.gets(".data", v1.root); n != 2 {
			t.Errorf("%d downloads of the parent, want the one that was dropped and one more", n)
		}
	})
	t.Run("its own pack", func(t *testing.T) {
		f := newFixture(t, config{})
		v1, x := patched(t, 1, 4)
		f.pushContent(v1)
		f.pushContent(x)
		// The parent is cached for the patch; its own reference has not
		// been checked.
		f.serves(x)
		f.bucket.forget()

		free := f.checksBusy()
		base := f.asked(v1.hash)
		eventually(t, "the work on the base's own reference has begun", func() bool {
			f.store.mu.Lock()
			defer f.store.mu.Unlock()
			return len(f.store.jobs) == 1
		})
		// It has its pack, which was cached, and waits for its check.
		time.Sleep(100 * time.Millisecond)
		f.cache.Drop(v1.root)
		free(1)

		f.served("the reference whose pack was dropped", v1, <-base)
		if f.log.has("level=ERROR") {
			t.Error("a reference whose pack was dropped is in the log as an error")
		}
		if n := f.bucket.gets(".data", v1.root); n != 1 {
			t.Errorf("%d downloads of the pack that was dropped, want one", n)
		}
	})
}

// altered changes the cached pack of root on disk without changing its
// size: change is given the bytes of the data file.
func (f *fixture) altered(root key.Key, change func(data []byte)) {
	f.t.Helper()
	path := filepath.Join(filepath.Dir(f.cache.TempDir()), "packs", root.String()+".data")
	data, err := os.ReadFile(path)
	if err != nil {
		f.t.Fatal(err)
	}
	change(data)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

// A pack whose bytes changed on disk under an unchanged size, as after a
// crash of the system, is not served: no NAR that is complete and wrong,
// and no narinfo with another text, which narrenhaus would sign.
func TestAlteredCacheIsNotServed(t *testing.T) {
	body := noise(1, 100<<10)
	for name, change := range map[string]func(t *testing.T, data []byte){
		"a byte of a file": func(t *testing.T, data []byte) {
			at := bytes.Index(data, body[:4096])
			if at < 0 {
				t.Fatal("the file is not in the pack")
			}
			data[at+2000] ^= 0xff
		},
		"a block of a file that reads as zeroes": func(t *testing.T, data []byte) {
			at := bytes.Index(data, body[:4096])
			if at < 0 {
				t.Fatal("the file is not in the pack")
			}
			start := (at + 8192) &^ 4095
			clear(data[start : start+4096])
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, config{})
			r := ingested(t, archive(t, smallTree(1)))
			f.pushContent(r)
			f.serves(r)
			f.altered(r.root, func(data []byte) { change(t, data) })
			f.bucket.forget()

			// The first request finds it out while it writes: what went
			// out is a beginning of the archive, cut short.
			c, err := f.store.Content(background(t), r.hash)
			if err == nil {
				var buf bytes.Buffer
				n, err := c.WriteTo(&buf)
				c.Release()
				if err == nil {
					t.Fatalf("WriteTo wrote %d bytes of an altered pack without an error; they are the archive: %v", n, bytes.Equal(buf.Bytes(), r.archive))
				}
				if buf.Len() >= len(r.archive) || !bytes.HasPrefix(r.archive, buf.Bytes()) {
					t.Errorf("%d bytes were written that are no beginning of the archive cut short", buf.Len())
				}
				if !f.log.has("level=ERROR", f.store.ContentName(r.hash)) {
					t.Error("the altered pack is not in the log")
				}
			} else {
				notFound(t, "Content from an altered pack", err)
			}
			// The next one has it fetched again.
			f.serves(r)
			if n := f.bucket.gets(".data", r.root); n != 1 {
				t.Errorf("%d downloads of the pack, want one", n)
			}
		})
	}

	t.Run("a byte of the parent of a patch", func(t *testing.T) {
		// The file that changed is one the patch reads from its parent.
		// What cuts the NAR drops the patch; the work that makes it
		// local again finds which pack it was.
		f := newFixture(t, config{})
		v1, v2 := patched(t, 1, 4)
		f.pushContent(v1)
		if res := f.pushContent(v2); res.Parent == nil || *res.Parent != v1.root {
			t.Fatal("the second version is no patch of the first")
		}
		f.serves(v2)
		f.altered(v1.root, func(data []byte) {
			at := bytes.Index(data, noise(1, 128<<10)[:4096])
			if at < 0 {
				t.Fatal("the file is not in the parent's pack")
			}
			data[at+2000] ^= 0xff
		})
		f.bucket.forget()

		c, err := f.store.Content(background(t), v2.hash)
		if err != nil {
			t.Fatalf("Content: %v", err)
		}
		var buf bytes.Buffer
		_, err = c.WriteTo(&buf)
		c.Release()
		if err == nil || buf.Len() >= len(v2.archive) || !bytes.HasPrefix(v2.archive, buf.Bytes()) {
			t.Fatalf("WriteTo wrote %d bytes of an altered pack, error %v, want a beginning of the archive and an error", buf.Len(), err)
		}
		f.serves(v2)
		if a, b := f.bucket.gets(".data", v2.root), f.bucket.gets(".data", v1.root); a != 1 || b != 1 {
			t.Errorf("%d downloads of the patch and %d of the parent, want one each", a, b)
		}
		if f.log.has("level=ERROR", "broken reference") {
			t.Error("a reference is called broken for what the cache holds")
		}
		f.serves(v1)
	})

	t.Run("a digit of a narinfo", func(t *testing.T) {
		f := newFixture(t, config{})
		r := ingested(t, archive(t, smallTree(1)))
		hash := storeHash("one")
		info := narinfoOf(t, hash, "one", r)
		f.pushNarinfo(info)
		if _, err := f.store.Narinfo(background(t), hash); err != nil {
			t.Fatal(err)
		}
		f.altered(info.root, func(data []byte) {
			at := bytes.Index(data, []byte("NarSize: "))
			if at < 0 {
				t.Fatal("no NarSize in the pack")
			}
			if digit := &data[at+len("NarSize: ")]; *digit == '9' {
				*digit = '8'
			} else {
				*digit++
			}
		})
		f.bucket.forget()

		eventually(t, "the narinfo is served again", func() bool {
			got, err := f.store.Narinfo(background(t), hash)
			if err != nil {
				notFound(t, "Narinfo from an altered pack", err)
				return false
			}
			if !bytes.Equal(got.Stored(), info.text) {
				t.Fatalf("an altered narinfo was served, with NarSize %d for %d", got.NarSize, r.size)
			}
			return true
		})
		if n := f.bucket.gets(".data", info.root); n != 1 {
			t.Errorf("%d downloads of the narinfo's pack, want one", n)
		}
		if !f.log.has("level=WARN", f.store.NarinfoName(hash)) {
			t.Error("the altered pack is not in the log")
		}
	})
}

// doubled returns the node of a directory that holds the directory below
// it twice, levels deep, with the objects it is made of and the length of
// its archive: a tree of a few dozen objects for an archive of 2^levels
// nodes.
func doubled(t *testing.T, levels int) (node nartree.Node, objs *objects, size uint64) {
	t.Helper()
	objs = newObjects()
	entry := func(name string, k key.Key) fstree.Entry {
		return fstree.Entry{Name: []byte(name), Mode: 0o040555, Mtime: 1_000_000_000, ContentKey: k[:]}
	}
	leaf, err := fstree.EncodeDirLeaf(nil)
	if err != nil {
		t.Fatal(err)
	}
	objs.emit(leaf)
	// The lengths of the first archives are measured, those of the rest
	// follow: an archive is its two halves and what a directory adds.
	var sizes []uint64
	k := leaf.Key
	for level := 0; ; level++ {
		if level <= 3 {
			var n counter
			if err := nartree.WriteNAR(&n, entry("nar", k), objs.Get); err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, uint64(n))
		} else {
			sizes = append(sizes, 2*sizes[level-1]+sizes[1]-2*sizes[0])
		}
		if level == levels {
			break
		}
		dir, err := fstree.EncodeDirLeaf([]fstree.Entry{entry("a", k), entry("b", k)})
		if err != nil {
			t.Fatal(err)
		}
		objs.emit(dir)
		k = dir.Key
	}
	for level := 2; level <= 3; level++ {
		if want := 2*sizes[level-1] + sizes[1] - 2*sizes[0]; sizes[level] != want {
			t.Fatalf("the archive of %d levels has %d bytes, the test reckons %d", level, sizes[level], want)
		}
	}
	return entry("nar", k), objs, sizes[levels]
}

// counter counts the bytes written to it.
type counter uint64

func (c *counter) Write(p []byte) (int, error) {
	*c += counter(len(p))
	return len(p), nil
}

// A reference of a few dozen objects whose archive would take years to
// write is refused before it takes a check's place for longer than a
// moment: by the length its recipe states, or, where the recipe lies
// about that, at the first byte beyond it.
func TestArchiveThatIsTooLargeIsNotRebuilt(t *testing.T) {
	f := newFixture(t, config{})
	node, objs, size := doubled(t, 41)
	if size <= nartree.MaxNARSize {
		t.Fatalf("the archive has %d bytes, the test wants one above the limit", size)
	}
	t.Logf("a reference of %d objects states an archive of %d bytes (%d TiB)", len(objs.m)+2, size, size>>40)

	// More of them than there are places for checks.
	var names []string
	for i := range 2 * cap(f.store.checkPlaces) {
		sum := sha256.Sum256(fmt.Append(nil, "bomb ", i))
		stated := size
		if i%2 == 1 {
			stated = 1 << 20
		}
		o := objs.clone()
		root, err := nartree.Root(node, sum, stated, o.emit)
		if err != nil {
			t.Fatal(err)
		}
		hash := nix32.Encode(sum[:])
		f.push(f.store.ContentName(hash), root, o, 0.5)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = f.store.Content(ctx, hash)
		cancel()
		notFound(t, "Content of an archive bomb", err)
		names = append(names, f.store.ContentName(hash))
	}
	for i, name := range names {
		if !f.log.has("level=ERROR", name) {
			t.Errorf("bomb %d was not refused: it is not in the log as an error", i)
		}
	}
	f.store.mu.Lock()
	running := len(f.store.jobs)
	f.store.mu.Unlock()
	if running != 0 || len(f.store.checkPlaces) != 0 {
		t.Errorf("%d references are still being made local and %d checks run", running, len(f.store.checkPlaces))
	}

	honest := ingested(t, archive(t, smallTree(1)))
	f.pushContent(honest)
	f.serves(honest)
}

// A reference whose packs are larger than the cache's limit, made local
// after its request gave up, is still there for the next request.
func TestReferenceMadeLocalWithoutItsRequestIsKept(t *testing.T) {
	f := newFixture(t, config{limit: 1})
	v1, v2 := patched(t, 1, 4)
	f.pushContent(v1)
	if res := f.pushContent(v2); res.Parent == nil {
		t.Fatal("the second version is no patch")
	}
	f.bucket.forget()
	// The connection is up before the short wait begins.
	if ok, err := f.store.HasNarinfo(background(t), storeHash("nothing")); ok || err != nil {
		t.Fatalf("HasNarinfo = %v, %v", ok, err)
	}
	f.srv.SlowBucket(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := f.store.Content(ctx, v2.hash)
	notFound(t, "Content with a wait of 50 ms", err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the error %v does not say that the wait ended", err)
	}
	eventually(t, "the work has ended", func() bool {
		f.store.mu.Lock()
		defer f.store.mu.Unlock()
		return len(f.store.jobs) == 0
	})
	if rec, ok := f.cache.Record(f.store.ContentName(v2.hash)); !ok || !rec.Checked {
		t.Fatalf("after the work ended the record is %+v (%v): the reference was evicted as soon as it was made local", rec, ok)
	}
	for _, root := range []key.Key{v2.root, v1.root} {
		if _, ok := f.cache.Meta(root); !ok {
			t.Errorf("the pack of %s is not cached after the work ended", root)
		}
	}

	// The next request is served from what the first one began, and when
	// it is over the cache is within its limit again.
	f.srv.SlowBucket(0)
	f.serves(v2)
	if n := f.bucket.gets(".data"); n != 2 {
		t.Errorf("%d data downloads in all, want the two packs once each", n)
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes after the request, the limit is 1", size)
	}
}

// What is kept for a request that never comes is let go of when its time
// is over, and when the store is closed: the cache returns to its limit.
func TestKeptReferenceIsLetGo(t *testing.T) {
	// abandoned asks for r with a wait too short for the download, and
	// returns when the work has ended without a request.
	abandoned := func(t *testing.T, f *fixture, r *reference) {
		t.Helper()
		if ok, err := f.store.HasNarinfo(background(t), storeHash("nothing")); ok || err != nil {
			t.Fatalf("HasNarinfo = %v, %v", ok, err)
		}
		f.srv.SlowBucket(300 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := f.store.Content(ctx, r.hash)
		notFound(t, "Content with a wait of 50 ms", err)
		eventually(t, "the work has ended", func() bool {
			f.store.mu.Lock()
			defer f.store.mu.Unlock()
			return len(f.store.jobs) == 0
		})
		f.srv.SlowBucket(0)
	}

	t.Run("after its time", func(t *testing.T) {
		f := newFixture(t, config{limit: 1})
		f.store.keep = 2 * time.Second
		r := ingested(t, archive(t, smallTree(1)))
		f.pushContent(r)
		f.bucket.forget()
		abandoned(t, f, r)
		if _, ok := f.cache.Meta(r.root); !ok {
			t.Fatal("the pack is not cached after the work ended")
		}
		eventually(t, "the cache is within its limit", func() bool { return f.cache.Size() == 0 })
		f.store.mu.Lock()
		kept := len(f.store.kept)
		f.store.mu.Unlock()
		if kept != 0 {
			t.Errorf("%d references are kept after their time", kept)
		}
		// It is fetched again when it is asked for after all.
		f.serves(r)
		if n := f.bucket.gets(".data", r.root); n != 2 {
			t.Errorf("%d downloads of the pack, want two", n)
		}
	})
	t.Run("when the store is closed", func(t *testing.T) {
		f := newFixture(t, config{limit: 1})
		r := ingested(t, archive(t, smallTree(1)))
		f.pushContent(r)
		abandoned(t, f, r)
		if _, ok := f.cache.Meta(r.root); !ok {
			t.Fatal("the pack is not cached after the work ended")
		}
		f.store.Close()
		if size := f.cache.Size(); size != 0 {
			t.Errorf("the cache holds %d bytes after the store was closed, the limit is 1", size)
		}
	})
	t.Run("by the request that waited for the work", func(t *testing.T) {
		// It holds the packs itself, to its own end.
		f := newFixture(t, config{limit: 1})
		r := ingested(t, archive(t, smallTree(1)))
		f.pushContent(r)
		hash := storeHash("one")
		f.pushNarinfo(narinfoOf(t, hash, "one", r))
		f.serves(r)
		if _, err := f.store.Narinfo(background(t), hash); err != nil {
			t.Fatalf("Narinfo: %v", err)
		}
		f.store.mu.Lock()
		kept := len(f.store.kept)
		f.store.mu.Unlock()
		if kept != 0 || f.cache.Size() != 0 {
			t.Errorf("%d references are kept and the cache holds %d bytes after the requests were served", kept, f.cache.Size())
		}
	})
	t.Run("by the request that comes for a narinfo", func(t *testing.T) {
		f := newFixture(t, config{limit: 1})
		r := ingested(t, archive(t, smallTree(1)))
		hash := storeHash("one")
		info := narinfoOf(t, hash, "one", r)
		f.pushNarinfo(info)
		f.bucket.forget()
		if ok, err := f.store.HasNarinfo(background(t), storeHash("nothing")); ok || err != nil {
			t.Fatalf("HasNarinfo = %v, %v", ok, err)
		}
		f.srv.SlowBucket(300 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := f.store.Narinfo(ctx, hash)
		notFound(t, "Narinfo with a wait of 50 ms", err)
		eventually(t, "the work has ended", func() bool {
			f.store.mu.Lock()
			defer f.store.mu.Unlock()
			return len(f.store.jobs) == 0
		})
		f.srv.SlowBucket(0)
		if _, ok := f.cache.Meta(info.root); !ok {
			t.Fatal("the narinfo's pack is not cached after the work ended")
		}
		if _, err := f.store.Narinfo(background(t), hash); err != nil {
			t.Fatalf("Narinfo: %v", err)
		}
		if n := f.bucket.gets(".data", info.root); n != 1 {
			t.Errorf("%d downloads of the narinfo's pack, want one", n)
		}
		if size := f.cache.Size(); size != 0 {
			t.Errorf("the cache holds %d bytes after the narinfo was read, the limit is 1", size)
		}
	})
}

// The server names one pack, and what is cached under its root is a patch:
// the work of another reference has placed it so in the moment since this
// one saw a complete pack there. That is a failure for now.
func TestPatchWithoutAParentNamed(t *testing.T) {
	f := newFixture(t, config{})
	v1, v2 := patched(t, 1, 4)
	f.pushContent(v1)
	f.pushContent(v2)
	f.serves(v2)
	if meta, ok := f.cache.Meta(v2.root); !ok || meta.Complete {
		t.Fatalf("the pack of the second version is cached as %+v (%v), want a patch", meta, ok)
	}

	beside, err := f.store.parentOf([]client.Pack{{Root: v2.root}})
	if err == nil || beside != nil {
		t.Errorf("a patch for which no parent is named: parent %v, error %v, want an error", beside, err)
	}
	if errors.Is(err, errBroken) || errors.Is(err, errDamaged) || errors.Is(err, errGone) {
		t.Errorf("the error %v is more than a failure for now", err)
	}
	other := ingested(t, archive(t, smallTree(3)))
	if beside, err := f.store.parentOf([]client.Pack{{Root: v2.root}, {Root: other.root}}); err == nil || beside != nil {
		t.Errorf("a patch for which another parent is named: parent %v, error %v, want an error", beside, err)
	}

	named := []client.Pack{{Root: v2.root}, {Root: v1.root}}
	if beside, err := f.store.parentOf(named); err != nil || beside == nil || beside.Root != v1.root {
		t.Errorf("a patch of the parent that is named: parent %v, error %v", beside, err)
	}
	if beside, err := f.store.parentOf([]client.Pack{{Root: v1.root}, {Root: other.root}}); err != nil || beside != nil {
		t.Errorf("a complete pack: parent %v, error %v, want none", beside, err)
	}
	if _, err := f.store.parentOf([]client.Pack{{Root: other.root}}); !errors.Is(err, errGone) {
		t.Errorf("a pack that is not cached: %v, want that it is gone", err)
	}
}

// A sound patch and a broken one of the same parent, asked for at once
// with nothing held back, round after round: the sound one is served every
// time.
func TestSoundReferenceBesideABrokenOne(t *testing.T) {
	const rounds = 12
	for round := range rounds {
		f := newFixture(t, config{server: testserver.Options{NoVerify: true}})
		v1, x := patched(t, 1, 4)
		_, y := patched(t, 1, 5)
		f.pushContent(v1)
		f.pushContent(x)
		broken := y.hash
		if round%2 == 0 {
			f.corruptPatch(y, v1)
		} else {
			broken, _ = f.withoutRecipe(v1, "the broken sibling")
		}
		if round%4 >= 2 {
			// With the parent cached, the broken one's verdict waits for
			// the parent to be fetched again.
			f.serves(v1)
		}

		sound := f.asked(x.hash)
		if round%3 == 1 {
			eventually(t, "x has its own pack", func() bool { _, ok := f.cache.Meta(x.root); return ok })
		}
		_, err := f.store.Content(background(t), broken)
		notFound(t, "Content of the broken sibling", err)
		f.served(fmt.Sprintf("round %d: the sound reference", round), x, <-sound)
		if f.log.has("level=ERROR", f.store.ContentName(x.hash)) {
			t.Fatalf("round %d: the sound reference is in the log as broken", round)
		}
		if !f.log.has("level=ERROR", f.store.ContentName(broken)) {
			t.Fatalf("round %d: the broken reference is not in the log as an error", round)
		}
		f.store.Close()
	}
}

func TestDefaults(t *testing.T) {
	s := New(context.Background(), nil, nil, Options{})
	defer s.Close()
	if s.keep != 15*time.Minute {
		t.Errorf("a reference made local without a request is kept for %s, want 15 minutes", s.keep)
	}
	if n := cap(s.contentPlaces); n != 8 {
		t.Errorf("%d content packs are downloaded at once, want 8", n)
	}
	if n := cap(s.narinfoPlaces); n != 16 {
		t.Errorf("%d narinfo packs are downloaded at once, want 16", n)
	}
	if n := cap(s.checkPlaces); n != 8 {
		t.Errorf("%d content references are checked at once, want 8", n)
	}
}

// The text of a narinfo is its writer's, and so is the index it is read
// through: readText stops at what a narinfo can be.
func TestNarinfoTextIsReadWithinBounds(t *testing.T) {
	blob := func(o *objects, data string) key.Key {
		obj, err := fstree.EncodeBlob([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		o.emit(obj)
		return obj.Key
	}
	// node stores a FileNode over children under a key that says length.
	node := func(o *objects, length uint64, children ...key.Key) key.Key {
		obj, err := fstree.EncodeFileNode(children)
		if err != nil {
			t.Fatal(err)
		}
		k, err := key.New(key.FileNode, length, obj.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		o.emit(fstree.Object{Key: k, Bytes: obj.Bytes})
		return k
	}

	t.Run("an index that names nothing, again and again", func(t *testing.T) {
		// Each level names the one below twice: four million reads for
		// no byte of text.
		o := newObjects()
		k := blob(o, "")
		for range 22 {
			k = node(o, 0, k, k)
		}
		reads := 0
		_, err := readText(k, func(k key.Key) ([]byte, error) {
			reads++
			return o.Get(k)
		})
		if !errors.Is(err, errBroken) {
			t.Errorf("readText: %v, want a broken reference", err)
		}
		if reads > maxTextObjects+1 {
			t.Errorf("%d objects were read, the limit is %d", reads, maxTextObjects)
		}
	})
	for _, c := range []struct {
		name   string
		length uint64
	}{
		{"a key that says one byte more than the text has", 7},
		{"a key that says one byte less than the text has", 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newObjects()
			text, err := readText(node(o, c.length, blob(o, "abc"), blob(o, "def")), o.Get)
			if !errors.Is(err, errBroken) {
				t.Errorf("readText gives %q and %v, want a broken reference", text, err)
			}
		})
	}
	o := newObjects()
	if text, err := readText(node(o, 6, blob(o, "abc"), blob(o, "def")), o.Get); err != nil || string(text) != "abcdef" {
		t.Errorf("readText of a text of its key's length gives %q and %v", text, err)
	}
}
