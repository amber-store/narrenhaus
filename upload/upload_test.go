package upload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"

	"github.com/amber-store/narrenhaus/nar"
	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/refs"
	"github.com/amber-store/narrenhaus/remote"
	"github.com/amber-store/narrenhaus/testserver"
)

const testPrefix = "acme/"

// quiet drops what a Remote logs.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// logBuffer keeps what the uploader and the store log. A test that fails
// prints it, and so does every test when NARRENHAUS_TEST_LOG is set.
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
func (l *logBuffer) has(parts ...string) bool {
lines:
	for line := range strings.SplitSeq(l.String(), "\n") {
		for _, part := range parts {
			if !strings.Contains(line, part) {
				continue lines
			}
		}
		return true
	}
	return false
}

// bucket sees every request of a client to the server's bucket.
type bucket struct {
	mu   sync.Mutex
	seen []string // "PUT /jaccard-test/packs/<root>/<upload>.data"
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

// forget drops what was seen so far.
func (b *bucket) forget() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen = nil
}

// requests returns the roots of the packs whose object with the extension
// ext (".idx" or ".data") was asked for with method, in the order of the
// requests.
func (b *bucket) requests(method, ext string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var roots []string
	for _, s := range b.seen {
		if strings.HasPrefix(s, method+" ") && strings.HasSuffix(s, ext) {
			roots = append(roots, path.Base(path.Dir(s)))
		}
	}
	return roots
}

// uploads counts the packs of root whose data was uploaded.
func (b *bucket) uploads(root key.Key) int {
	n := 0
	for _, r := range b.requests(http.MethodPut, ".data") {
		if r == root.String() {
			n++
		}
	}
	return n
}

// putOf matches the uploads of the pack of root, index and data.
func putOf(root key.Key) func(*http.Request) bool {
	return func(r *http.Request) bool {
		return r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/"+root.String()+"/")
	}
}

// fixture is a server, a cache directory, a store and an uploader on them.
type fixture struct {
	t      *testing.T
	srv    *testserver.Server
	bucket *bucket
	other  *remote.Remote // another client of the server: asks and writes
	rem    *remote.Remote // the uploader's
	dir    string
	cache  *packcache.Cache
	store  *refs.Store
	up     *Uploader
	log    *logBuffer
}

type config struct {
	options Options
	server  testserver.Options
}

func newFixture(t *testing.T, cfg config) *fixture {
	t.Helper()
	f := &fixture{t: t, bucket: &bucket{}, log: &logBuffer{}, dir: t.TempDir()}
	f.srv = testserver.Start(t, cfg.server)
	f.srv.Intercept(f.bucket.serve)

	f.other = f.remote()
	f.rem = f.remote()

	var err error
	f.cache, err = packcache.Open(f.dir, 1<<30, packcache.Origin{Server: "test", Prefix: testPrefix})
	if err != nil {
		t.Fatalf("opening the cache: %v", err)
	}
	t.Cleanup(func() { f.cache.Close() })

	log := slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.store = refs.New(context.Background(), f.cache, f.rem, refs.Options{Prefix: testPrefix, Log: log})
	t.Cleanup(f.store.Close)

	opts := cfg.options
	opts.Log = log
	f.up = New(context.Background(), f.cache, f.rem, f.store, opts)
	t.Cleanup(func() {
		f.up.Close()
		if t.Failed() || os.Getenv("NARRENHAUS_TEST_LOG") != "" {
			t.Logf("the log:\n%s", f.log.String())
		}
	})
	return f
}

// remote returns a connection of its own to the server, with limits short
// enough for tests that stop it.
func (f *fixture) remote() *remote.Remote {
	r := remote.New(remote.Config{
		Dial:          f.srv.Dial(),
		Log:           quiet,
		LookupTimeout: 3 * time.Second,
		DialTimeout:   300 * time.Millisecond,
		FailFast:      500 * time.Millisecond,
	})
	f.t.Cleanup(r.Close)
	return r
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

// smallTree is a store path of a few files, about 100 KiB; what differs
// between two of them follows from seed.
func smallTree(seed byte) node {
	return dir(map[string]node{
		"bin":    dir(map[string]node{"tool": script("#!/bin/sh\necho " + string('a'+rune(seed%26)) + "\n")}),
		"data":   file(noise(seed, 100<<10)),
		"empty":  text(""),
		"latest": link("data"),
	})
}

// tinyTree is a store path of a few bytes.
func tinyTree(seed byte) node {
	return dir(map[string]node{"note": text("tiny " + string('a'+rune(seed%26)) + "\n")})
}

// storePath is a store path as a test uploads it: its archive, and what
// narrenhaus is to make of it.
type storePath struct {
	hash    string // the hash part of the store path
	name    string // the rest of its base name
	archive []byte
	sum     [32]byte
	narHash string // sum in Nix's base 32
	size    uint64
	root    key.Key // of the content reference
	objs    *objects
}

// storeHash returns the hash part of a store path, 32 characters that
// follow from seed.
func storeHash(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return nix32.Encode(sum[:20])
}

// pathOf returns the store path <hash of seed>-<seed> with the contents n.
func pathOf(t *testing.T, seed string, n node) *storePath {
	t.Helper()
	p := &storePath{hash: storeHash(seed), name: seed, archive: archive(t, n), objs: newObjects()}
	node, sum, size, err := nartree.Ingest(bytes.NewReader(p.archive), p.objs.emit)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	p.sum, p.size, p.narHash = sum, size, nix32.Encode(sum[:])
	if p.root, err = nartree.Root(node, sum, size, p.objs.emit); err != nil {
		t.Fatalf("Root: %v", err)
	}
	return p
}

// as returns another store path with the same contents.
func (p *storePath) as(seed string) *storePath {
	out := *p
	out.hash, out.name = storeHash(seed), seed
	return &out
}

// nixText is the narinfo Nix sends for the store path after it uploaded
// the archive compressed with xz: the URL and the file's hash and size are
// those of the compressed file.
func (p *storePath) nixText(references ...string) []byte {
	fileSum := sha256.Sum256([]byte("xz of " + p.narHash))
	fileHash := nix32.Encode(fileSum[:])
	return fmt.Appendf(nil, "StorePath: /nix/store/%s-%s\nURL: nar/%s.nar.xz\nCompression: xz\nFileHash: sha256:%s\nFileSize: %d\n"+
		"NarHash: sha256:%s\nNarSize: %d\nReferences: %s\nDeriver: %s-%s.drv\nSig: cache.example-1:%s\n",
		p.hash, p.name, fileHash, fileHash, p.size/3+1, p.narHash, p.size, strings.Join(references, " "),
		storeHash("drv "+p.name), p.name, base64.StdEncoding.EncodeToString(make([]byte, 64)))
}

// stored is the text narrenhaus keeps of nixText.
func (p *storePath) stored(t *testing.T, references ...string) []byte {
	t.Helper()
	info, err := narinfo.Parse(p.nixText(references...))
	if err != nil {
		t.Fatalf("the narinfo of the test: %v", err)
	}
	return info.Stored()
}

// narinfoRoot returns the root of the reference that holds text, and its
// objects.
func narinfoRoot(t *testing.T, text []byte) (key.Key, *objects) {
	t.Helper()
	objs := newObjects()
	root, err := nartree.File(bytes.NewReader(text), objs.emit)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	return root, objs
}

func background(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

// wantStatus fails the test unless err is an *Error with status, and a
// message fit for the body of an HTTP answer.
func wantStatus(t *testing.T, what string, err error, status int) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("%s: error %v, want an *Error with status %d", what, err, status)
	}
	if e.Status != status {
		t.Fatalf("%s: status %d (%s), want %d", what, e.Status, e.Msg, status)
	}
	if e.Msg == "" || len(e.Msg) >= 1024 || strings.ContainsAny(e.Msg, "\r\n") {
		t.Fatalf("%s: a message of %d bytes that is no body of an answer: %q", what, len(e.Msg), e.Msg)
	}
	if e.Error() != e.Msg {
		t.Errorf("%s: Error() is %q, the message %q", what, e.Error(), e.Msg)
	}
	return e
}

// putNAR uploads the archive of p uncompressed, as Nix does with
// ?compression=none: under its own hash.
func (f *fixture) putNAR(p *storePath) {
	f.t.Helper()
	if err := f.up.PutNAR(background(f.t), p.narHash, bytes.NewReader(p.archive)); err != nil {
		f.t.Fatalf("PutNAR(%s): %v", p.name, err)
	}
}

// putNarinfo uploads the narinfo of p.
func (f *fixture) putNarinfo(p *storePath, references ...string) error {
	f.t.Helper()
	return f.up.PutNarinfo(background(f.t), p.hash, p.nixText(references...))
}

// upload uploads p as Nix does: its archive, then its narinfo.
func (f *fixture) upload(p *storePath) {
	f.t.Helper()
	f.putNAR(p)
	if err := f.putNarinfo(p); err != nil {
		f.t.Fatalf("PutNarinfo(%s): %v", p.name, err)
	}
}

// root asks the server where name points.
func (f *fixture) root(name string) (key.Key, bool) {
	f.t.Helper()
	root, _, err := f.other.Lookup(background(f.t), name)
	switch {
	case err == nil:
		return root, true
	case errors.Is(err, client.ErrNotFound):
		return key.Key{}, false
	}
	f.t.Fatalf("looking up %q: %v", name, err)
	return key.Key{}, false
}

// pointsAt fails the test unless the server has name pointing at root.
func (f *fixture) pointsAt(name string, root key.Key) {
	f.t.Helper()
	got, ok := f.root(name)
	if !ok {
		f.t.Fatalf("the server has no reference %q", name)
	}
	if got != root {
		f.t.Fatalf("%q points at %s, want %s", name, got, root)
	}
}

// push writes the reference name on the server, from another client.
func (f *fixture) push(name string, root key.Key, objs client.ObjectSource, minDedup float64) {
	f.t.Helper()
	_, err := f.other.Push(background(f.t), objs, name, root, client.PushOptions{MinDedup: minDedup, TempDir: f.t.TempDir()})
	if err != nil {
		f.t.Fatalf("pushing %q: %v", name, err)
	}
}

// staged returns the names of what lies under the cache's staging/.
func (f *fixture) staged() []string {
	f.t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.dir, "staging"))
	if err != nil {
		f.t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// nothingStaged fails the test if something lies under staging/.
func (f *fixture) nothingStaged() {
	f.t.Helper()
	if names := f.staged(); len(names) != 0 {
		f.t.Fatalf("staging holds %v, want nothing", names)
	}
}

// settled waits until the uploader has nothing to do.
func (f *fixture) settled() {
	f.t.Helper()
	eventually(f.t, "the uploader is idle", f.up.Idle)
}

// entry returns the state of the content work for the NAR hash, whether a
// narinfo has asked for it, and whether there is an entry at all.
func (f *fixture) entry(narHash string) (st state, asked, ok bool) {
	f.up.mu.Lock()
	defer f.up.mu.Unlock()
	w := f.up.entries[narHash]
	if w == nil {
		return 0, false, false
	}
	return w.state, w.asked, true
}

// inState waits until the content work for p is in the state want.
func (f *fixture) inState(p *storePath, want state) {
	f.t.Helper()
	eventually(f.t, fmt.Sprintf("the work for %s is %s", p.name, want), func() bool {
		st, _, ok := f.entry(p.narHash)
		return ok && st == want
	})
}

// hold makes the bucket keep the requests that match waiting until release
// is called. arrived gets a value for each request that is kept.
func (f *fixture) hold(match func(*http.Request) bool) (arrived <-chan struct{}, release func()) {
	f.t.Helper()
	gate := make(chan struct{})
	came := make(chan struct{}, 256)
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		if !match(r) {
			return false
		}
		select {
		case came <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		return false
	})
	release = sync.OnceFunc(func() { close(gate) })
	f.t.Cleanup(release)
	return came, release
}

// await waits for a value on c.
func await[T any](t *testing.T, what string, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(15 * time.Second):
		t.Fatalf("never happened: %s", what)
	}
	panic("unreachable")
}

// reader is another narrenhaus on the same server: a cache directory and a
// store of its own.
func (f *fixture) reader() *refs.Store {
	f.t.Helper()
	cache, err := packcache.Open(f.t.TempDir(), 1<<30, packcache.Origin{Server: "test", Prefix: testPrefix})
	if err != nil {
		f.t.Fatalf("opening a second cache: %v", err)
	}
	f.t.Cleanup(func() { cache.Close() })
	store := refs.New(context.Background(), cache, f.remote(), refs.Options{Prefix: testPrefix, Log: quiet})
	f.t.Cleanup(store.Close)
	return store
}

// narOf returns the archive store serves for the NAR hash.
func narOf(t *testing.T, store *refs.Store, narHash string) []byte {
	t.Helper()
	c, err := store.Content(background(t), narHash)
	if err != nil {
		t.Fatalf("Content(%s): %v", narHash, err)
	}
	defer c.Release()
	var buf bytes.Buffer
	if _, err := c.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

func xzOf(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := xz.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zstdOf(t *testing.T, data []byte) []byte {
	t.Helper()
	w, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	return w.EncodeAll(data, nil)
}

func gzipOf(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// fileHash is the name Nix gives the upload of a compressed file: the
// file's SHA-256.
func fileHash(body []byte) string {
	sum := sha256.Sum256(body)
	return nix32.Encode(sum[:])
}

func TestQueue(t *testing.T) {
	order := func(q *queue) string {
		var s strings.Builder
		for n := q.next(); n != nil; n = q.next() {
			s.WriteString(n.hash)
		}
		return s.String()
	}
	mk := func(names string) map[rune]*work {
		m := map[rune]*work{}
		for _, r := range names {
			m[r] = &work{hash: string(r)}
		}
		return m
	}

	t.Run("in the order of arrival", func(t *testing.T) {
		var q queue
		w := mk("abc")
		q.push(w['a'])
		q.push(w['b'])
		q.push(w['c'])
		q.push(w['a']) // queued already
		if q.len() != 3 {
			t.Fatalf("len = %d, want 3", q.len())
		}
		if got := order(&q); got != "abc" {
			t.Errorf("order %q, want abc", got)
		}
		if q.len() != 0 || q.next() != nil {
			t.Error("an empty queue gives work")
		}
	})

	t.Run("asked for first, in the order of asking", func(t *testing.T) {
		var q queue
		w := mk("abcde")
		for _, r := range "abcde" {
			q.push(w[r])
		}
		q.promote(w['d'])
		q.promote(w['b'])
		q.promote(w['d']) // asked again: where it was
		if got := order(&q); got != "dbace" {
			t.Errorf("order %q, want dbace", got)
		}
	})

	t.Run("asked for before it is queued", func(t *testing.T) {
		var q queue
		w := mk("abc")
		q.push(w['a'])
		q.promote(w['c']) // not queued: running, or failed for now
		if q.len() != 1 {
			t.Fatalf("promote queued work: len = %d", q.len())
		}
		q.push(w['b'])
		q.push(w['c'])
		if got := order(&q); got != "cab" {
			t.Errorf("order %q, want cab", got)
		}
	})

	t.Run("queued again where it stood", func(t *testing.T) {
		var q queue
		w := mk("abcxy")
		for _, r := range "abc" {
			q.push(w[r])
		}
		q.promote(w['x'])
		q.promote(w['y'])
		q.push(w['x'])
		q.push(w['y'])
		// x and a ran and failed for now; they are started again.
		if n := q.next(); n != w['x'] {
			t.Fatalf("next = %s, want x", n.hash)
		}
		q.push(w['x'])
		if got := order(&q); got != "xyabc" {
			t.Errorf("order %q, want xyabc", got)
		}
		for _, r := range "bca" {
			q.push(w[r])
		}
		if got := order(&q); got != "abc" {
			t.Errorf("order %q, want abc", got)
		}
	})
}

func TestUploadThenBothReferencesExist(t *testing.T) {
	f := newFixture(t, config{})
	dep := pathOf(t, "libdep-1.0", tinyTree(1))
	p := pathOf(t, "hello-2.12", smallTree(1))
	contentName, narinfoName := testPrefix+"nar/"+p.narHash+".nar", testPrefix+p.hash+".narinfo"

	if _, ok := f.root(contentName); ok {
		t.Fatal("the reference is there before the upload")
	}
	f.putNAR(p)
	if err := f.putNarinfo(p, dep.hash+"-"+dep.name, p.hash+"-"+p.name); err != nil {
		t.Fatalf("PutNarinfo: %v", err)
	}

	// A narinfo that was answered 200 has both references on the server.
	stored := p.stored(t, dep.hash+"-"+dep.name, p.hash+"-"+p.name)
	wantRoot, _ := narinfoRoot(t, stored)
	f.pointsAt(contentName, p.root)
	f.pointsAt(narinfoName, wantRoot)
	if blob, err := fstree.EncodeBlob(stored); err != nil || blob.Key != wantRoot {
		t.Fatalf("the narinfo's root is not the Blob of its text (%v)", err)
	}
	if !bytes.Contains(stored, []byte("URL: nar/"+p.narHash+".nar\nCompression: none\n")) {
		t.Fatalf("the stored text of the test is not narrenhaus's:\n%s", stored)
	}

	f.settled()
	f.nothingStaged()
	if _, _, ok := f.entry(p.narHash); ok {
		t.Error("a finished push left an entry")
	}

	// Both packs are in the cache as what was uploaded, with their
	// records, and are served from there: nothing is downloaded.
	for name, root := range map[string]key.Key{contentName: p.root, narinfoName: wantRoot} {
		meta, ok := f.cache.Meta(root)
		if !ok || !meta.Complete {
			t.Errorf("the pack of %s is cached %+v (%v), want a complete pack", name, meta, ok)
		}
		rec, ok := f.cache.Record(name)
		if !ok || rec.Root != root || !slices.Equal(rec.Packs, []key.Key{root}) {
			t.Errorf("the record of %s is %+v (%v), want root and pack %s", name, rec, ok, root)
		}
	}
	f.bucket.forget()
	info, err := f.store.Narinfo(background(t), p.hash)
	if err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if got := info.Stored(); !bytes.Equal(got, stored) {
		t.Errorf("the narinfo served here is\n%s\nwant\n%s", got, stored)
	}
	if got := narOf(t, f.store, p.narHash); !bytes.Equal(got, p.archive) {
		t.Errorf("the archive served here has %d bytes and is not the %d that were uploaded", len(got), len(p.archive))
	}
	if gets := f.bucket.requests(http.MethodGet, ""); len(gets) != 0 {
		t.Errorf("serving what was uploaded here downloaded %v", gets)
	}

	// Another narrenhaus reads from the server what this one wrote.
	there := f.reader()
	info, err = there.Narinfo(background(t), p.hash)
	if err != nil {
		t.Fatalf("Narinfo on another instance: %v", err)
	}
	if got := info.Stored(); !bytes.Equal(got, stored) {
		t.Errorf("the narinfo served there is\n%s\nwant\n%s", got, stored)
	}
	if got := narOf(t, there, p.narHash); !bytes.Equal(got, p.archive) {
		t.Errorf("the archive served there has %d bytes and is not the %d that were uploaded", len(got), len(p.archive))
	}

	// One line per upload in the log.
	if !f.log.has("level=INFO", contentName) || !f.log.has("level=INFO", narinfoName) {
		t.Error("the log has no line for each of the two pushes")
	}
}

func TestCompressedBodies(t *testing.T) {
	f := newFixture(t, config{})
	two := pathOf(t, "frames", smallTree(3))
	for _, c := range []struct {
		name string
		p    *storePath
		body []byte
	}{
		{"xz", pathOf(t, "xz", smallTree(2)), nil},
		{"zstd", two, append(zstdOf(t, two.archive[:len(two.archive)/2]), zstdOf(t, two.archive[len(two.archive)/2:])...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := c.body
			if body == nil {
				body = xzOf(t, c.p.archive)
			}
			// The URL of a compressed upload names the compressed file.
			if err := f.up.PutNAR(background(t), fileHash(body), bytes.NewReader(body)); err != nil {
				t.Fatalf("PutNAR: %v", err)
			}
			if err := f.putNarinfo(c.p); err != nil {
				t.Fatalf("PutNarinfo: %v", err)
			}
			f.pointsAt(testPrefix+"nar/"+c.p.narHash+".nar", c.p.root)
			if got := narOf(t, f.store, c.p.narHash); !bytes.Equal(got, c.p.archive) {
				t.Errorf("the archive served has %d bytes and is not the %d that were uploaded", len(got), len(c.p.archive))
			}
		})
	}
	f.settled()
	f.nothingStaged()
}

func TestNARPutAnswersBeforeThePush(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "slow", smallTree(1))
	name := testPrefix + "nar/" + p.narHash + ".nar"
	arrived, release := f.hold(putOf(p.root))

	// The request is answered while the pack cannot be uploaded.
	f.putNAR(p)
	await(t, "the push reaches the bucket", arrived)
	if _, ok := f.root(name); ok {
		t.Fatal("the reference is on the server before its pack was uploaded")
	}
	if f.up.Idle() {
		t.Error("Idle while a push runs")
	}
	if st, _, ok := f.entry(p.narHash); !ok || st != stateRunning {
		t.Errorf("the work is %v (%v), want running", st, ok)
	}
	if len(f.staged()) == 0 {
		t.Error("nothing is staged while the push runs")
	}

	release()
	f.settled()
	f.pointsAt(name, p.root)
	f.nothingStaged()
	if rec, ok := f.cache.Record(name); !ok || rec.Root != p.root || !rec.Checked {
		t.Errorf("the record is %+v (%v), want the root %s, checked", rec, ok, p.root)
	}
	if got := narOf(t, f.store, p.narHash); !bytes.Equal(got, p.archive) {
		t.Error("the archive served is not the one uploaded")
	}
}

func TestAllNARsBeforeAnyNarinfo(t *testing.T) {
	// Nix 2.35 sends every NAR and then the narinfos, those of the leaves
	// first. The content a narinfo waits for must not stand behind content
	// nobody waits for yet.
	f := newFixture(t, config{options: Options{ContentPushes: 1}})
	var paths []*storePath
	for i := range 5 {
		paths = append(paths, pathOf(t, fmt.Sprintf("path-%d", i), smallTree(byte(i+1))))
	}
	arrived, release := f.hold(putOf(paths[0].root))
	for _, p := range paths {
		f.putNAR(p)
	}
	await(t, "the first push reaches the bucket", arrived)
	for i, p := range paths {
		want := stateQueued
		if i == 0 {
			want = stateRunning
		}
		if st, asked, ok := f.entry(p.narHash); !ok || st != want || asked {
			t.Fatalf("the work for path %d is %v, asked %v (%v), want %v and not asked", i, st, asked, ok, want)
		}
	}

	last := paths[4]
	done := make(chan error, 1)
	go func() { done <- f.putNarinfo(last) }()
	eventually(t, "the narinfo has asked for its content", func() bool {
		_, asked, _ := f.entry(last.narHash)
		return asked
	})
	select {
	case err := <-done:
		t.Fatalf("PutNarinfo returned (%v) before its content was pushed", err)
	default:
	}
	release()
	if err := await(t, "PutNarinfo returns", done); err != nil {
		t.Fatalf("PutNarinfo: %v", err)
	}
	f.settled()

	var want, got []string
	for _, i := range []int{0, 4, 1, 2, 3} {
		want = append(want, paths[i].root.String())
	}
	for _, root := range f.bucket.requests(http.MethodPut, ".data") {
		if slices.Contains(want, root) {
			got = append(got, root)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("the content was pushed in the order\n%v\nwant\n%v", got, want)
	}
	for _, p := range paths {
		f.pointsAt(testPrefix+"nar/"+p.narHash+".nar", p.root)
	}
	f.nothingStaged()
}

func TestTwoStorePathsOneContent(t *testing.T) {
	f := newFixture(t, config{})
	one := pathOf(t, "one", smallTree(1))
	two := one.as("two")

	// Nix sends the archive of each path; the second finds the work of the
	// first, or the reference it made.
	f.upload(one)
	f.upload(two)
	f.settled()

	f.pointsAt(testPrefix+"nar/"+one.narHash+".nar", one.root)
	rootOne, _ := narinfoRoot(t, one.stored(t))
	rootTwo, _ := narinfoRoot(t, two.stored(t))
	if rootOne == rootTwo {
		t.Fatal("the two narinfos of the test are one")
	}
	f.pointsAt(testPrefix+one.hash+".narinfo", rootOne)
	f.pointsAt(testPrefix+two.hash+".narinfo", rootTwo)
	if n := f.bucket.uploads(one.root); n != 1 {
		t.Errorf("the content was uploaded %d times, want once", n)
	}
	f.nothingStaged()
}

func TestNarinfoJoinsARunningPush(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "joined", smallTree(1))
	arrived, release := f.hold(putOf(p.root))
	f.putNAR(p)
	await(t, "the content push reaches the bucket", arrived)

	// Nix repeats a narinfo that was silent for too long: both requests
	// are answered by the one piece of work.
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- f.putNarinfo(p) }()
	}
	eventually(t, "both requests wait for the one piece of narinfo work", func() bool {
		f.up.mu.Lock()
		defer f.up.mu.Unlock()
		return f.up.requests == 2 && len(f.up.narinfos) == 1
	})
	if _, asked, _ := f.entry(p.narHash); !asked {
		eventually(t, "the narinfo has asked for its content", func() bool {
			_, asked, _ := f.entry(p.narHash)
			return asked
		})
	}
	select {
	case err := <-done:
		t.Fatalf("PutNarinfo returned (%v) while its content was being pushed", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	for range 2 {
		if err := await(t, "PutNarinfo returns", done); err != nil {
			t.Fatalf("PutNarinfo: %v", err)
		}
	}
	f.settled()

	infoRoot, _ := narinfoRoot(t, p.stored(t))
	f.pointsAt(testPrefix+p.hash+".narinfo", infoRoot)
	if n := f.bucket.uploads(p.root); n != 1 {
		t.Errorf("the content was uploaded %d times, want once", n)
	}
	if n := f.bucket.uploads(infoRoot); n != 1 {
		t.Errorf("the narinfo was uploaded %d times, want once", n)
	}
}

func TestFirstWriterWins(t *testing.T) {
	f := newFixture(t, config{})
	mine := pathOf(t, "contested", smallTree(1))
	// Somebody else has built the same store path with other contents and
	// uploaded it first.
	theirs := pathOf(t, "contested", smallTree(2))
	if mine.hash != theirs.hash || mine.narHash == theirs.narHash {
		t.Fatal("the test wants one store path with two contents")
	}
	theirRoot, theirObjs := narinfoRoot(t, theirs.stored(t))
	narinfoName := testPrefix + mine.hash + ".narinfo"
	f.push(testPrefix+"nar/"+theirs.narHash+".nar", theirs.root, theirs.objs, 0.5)
	f.push(narinfoName, theirRoot, theirObjs, 2)
	f.bucket.forget()

	f.upload(mine)
	f.settled()
	f.pointsAt(narinfoName, theirRoot)
	// The content was pushed all the same: it is named by what it holds.
	f.pointsAt(testPrefix+"nar/"+mine.narHash+".nar", mine.root)
	myRoot, _ := narinfoRoot(t, mine.stored(t))
	if n := f.bucket.uploads(myRoot); n != 0 {
		t.Errorf("the narinfo that lost was uploaded %d times", n)
	}
	f.nothingStaged()
}

func TestContentThatIsOnTheServer(t *testing.T) {
	f := newFixture(t, config{})

	// The same content, written by another narrenhaus: nothing is pushed.
	p := pathOf(t, "there", smallTree(1))
	name := testPrefix + "nar/" + p.narHash + ".nar"
	f.push(name, p.root, p.objs, 0.5)
	f.bucket.forget()
	f.upload(p)
	f.settled()
	if n := f.bucket.uploads(p.root); n != 0 {
		t.Errorf("content the server had was uploaded %d times", n)
	}
	f.nothingStaged()
	if _, _, ok := f.entry(p.narHash); ok {
		t.Error("the work left an entry")
	}

	// The name with another root, written by something else: it is left as
	// it is, and the log names both roots.
	q := pathOf(t, "other", smallTree(2))
	name = testPrefix + "nar/" + q.narHash + ".nar"
	f.push(name, p.root, p.objs, 0.5)
	f.putNAR(q)
	f.settled()
	f.pointsAt(name, p.root)
	if !f.log.has("level=WARN", name, p.root.String(), q.root.String()) {
		t.Error("no warning that names the reference and both roots")
	}
	f.nothingStaged()
}

func TestSecondVersionIsAPatch(t *testing.T) {
	f := newFixture(t, config{})
	version := func(seed string, changed byte) *storePath {
		entries := map[string]node{}
		for i := range byte(8) {
			s := 10 + i
			if i == 0 {
				s = changed
			}
			entries[fmt.Sprintf("file-%d", i)] = file(noise(s, 128<<10))
		}
		return pathOf(t, seed, dir(entries))
	}
	v1, v2 := version("tool-1.0", 100), version("tool-1.1", 101)
	f.upload(v1)
	f.upload(v2)
	f.settled()

	// On the server the second version leans on the first.
	_, packs, err := f.other.Lookup(background(t), testPrefix+"nar/"+v2.narHash+".nar")
	if err != nil {
		t.Fatal(err)
	}
	if len(packs) != 2 || packs[0].Root != v2.root || packs[1].Root != v1.root {
		t.Fatalf("the second version is held by %d packs, want its own as a patch of the first's", len(packs))
	}
	// Here it is whole, as it was staged.
	if meta, ok := f.cache.Meta(v2.root); !ok || !meta.Complete {
		t.Errorf("the second version is cached %+v (%v), want a complete pack", meta, ok)
	}
	f.bucket.forget()
	if got := narOf(t, f.store, v2.narHash); !bytes.Equal(got, v2.archive) {
		t.Error("the archive served is not the one uploaded")
	}
	if gets := f.bucket.requests(http.MethodGet, ""); len(gets) != 0 {
		t.Errorf("serving what was uploaded here downloaded %v", gets)
	}
	if got := narOf(t, f.reader(), v2.narHash); !bytes.Equal(got, v2.archive) {
		t.Error("the archive served by another instance is not the one uploaded")
	}
}

func TestNarinfoWithoutNAR(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "no-nar", tinyTree(1))
	e := wantStatus(t, "PutNarinfo without its NAR", f.putNarinfo(p), http.StatusBadRequest)
	if !strings.Contains(e.Msg, p.narHash) {
		t.Errorf("the message does not name the NAR hash %s: %s", p.narHash, e.Msg)
	}
	if _, ok := f.root(testPrefix + p.hash + ".narinfo"); ok {
		t.Error("the narinfo reference was written")
	}
	f.settled()
	f.nothingStaged()
}

func TestMalformedNarinfo(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "malformed", tinyTree(1))
	f.putNAR(p)
	f.settled()
	good := p.nixText()
	for _, c := range []struct {
		name string
		hash string
		body []byte
	}{
		{"no text", p.hash, nil},
		{"no narinfo", p.hash, []byte("<html>\n")},
		{"another path's", storeHash("somebody else"), good},
		{"no NarHash", p.hash, bytes.Replace(good, []byte("NarHash: "), []byte("NarHush: "), 1)},
		{"a hash that is no SHA-256", p.hash, bytes.Replace(good, []byte("NarHash: sha256:"+p.narHash), []byte("NarHash: sha1:"+strings.Repeat("0", 32)), 1)},
		{"another store", p.hash, bytes.Replace(good, []byte("/nix/store/"), []byte("/gnu/store/"), 1)},
	} {
		t.Run(c.name, func(t *testing.T) {
			wantStatus(t, "PutNarinfo", f.up.PutNarinfo(background(t), c.hash, c.body), http.StatusBadRequest)
		})
	}
	if _, ok := f.root(testPrefix + p.hash + ".narinfo"); ok {
		t.Error("a narinfo reference was written")
	}
}

func TestNarSizeMismatch(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "sized", smallTree(1))
	_, release := f.hold(putOf(p.root))
	f.putNAR(p)

	// While the NAR is here, a narinfo must state its size.
	wrong := *p
	wrong.size++
	e := wantStatus(t, "PutNarinfo with another NarSize", f.putNarinfo(&wrong), http.StatusBadRequest)
	if !strings.Contains(e.Msg, fmt.Sprint(p.size)) || !strings.Contains(e.Msg, fmt.Sprint(wrong.size)) {
		t.Errorf("the message does not name both sizes: %s", e.Msg)
	}
	release()
	if err := f.putNarinfo(p); err != nil {
		t.Fatalf("PutNarinfo with the size of the NAR: %v", err)
	}
}

func TestUncompressedURLHashMismatch(t *testing.T) {
	f := newFixture(t, config{})
	p, q := pathOf(t, "mine", tinyTree(1)), pathOf(t, "other", tinyTree(2))
	err := f.up.PutNAR(background(t), q.narHash, bytes.NewReader(p.archive))
	e := wantStatus(t, "PutNAR under another hash", err, http.StatusBadRequest)
	if !strings.Contains(e.Msg, p.narHash) || !strings.Contains(e.Msg, q.narHash) {
		t.Errorf("the message does not name both hashes: %s", e.Msg)
	}
	f.settled()
	f.nothingStaged()
	for _, x := range []*storePath{p, q} {
		if _, ok := f.root(testPrefix + "nar/" + x.narHash + ".nar"); ok {
			t.Error("a reference was written")
		}
	}
}

func TestGzipToAnExtensionlessURL(t *testing.T) {
	// Nix gives a gzip upload no extension: the URL looks like that of an
	// uncompressed NAR and names the gzip file's hash.
	f := newFixture(t, config{})
	p := pathOf(t, "gzipped", smallTree(1))
	body := gzipOf(t, p.archive)
	urlHash := fileHash(body)
	head := func(hash string) bool {
		t.Helper()
		ok, err := f.up.HeadNAR(background(t), hash)
		if err != nil {
			t.Fatalf("HeadNAR: %v", err)
		}
		return ok
	}

	if head(urlHash) {
		t.Fatal("HeadNAR of the URL's hash is true before the upload")
	}
	if err := f.up.PutNAR(background(t), urlHash, bytes.NewReader(body)); err != nil {
		t.Fatalf("PutNAR: %v", err)
	}
	if err := f.putNarinfo(p); err != nil {
		t.Fatalf("PutNarinfo: %v", err)
	}
	f.settled()
	f.pointsAt(testPrefix+"nar/"+p.narHash+".nar", p.root)
	if _, ok := f.root(testPrefix + "nar/" + urlHash + ".nar"); ok {
		t.Error("a reference is named by the gzip file's hash")
	}
	if head(urlHash) {
		t.Error("HeadNAR of the URL's hash is true after the upload")
	}
	if !head(p.narHash) {
		t.Error("HeadNAR of the NAR's hash is false after the upload")
	}
}

func TestMalformedNAR(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "broken", smallTree(1))
	trailing := append(bytes.Clone(p.archive), 0)
	unsorted := bytes.Replace(p.archive, []byte("latest"), []byte("aatest"), 1)
	for _, c := range []struct {
		name string
		hash string
		body []byte
	}{
		{"a byte after the archive", p.narHash, trailing},
		{"an archive that ends early", p.narHash, p.archive[:len(p.archive)-8]},
		{"names out of order", p.narHash, unsorted},
		{"xz of a byte after the archive", fileHash(xzOf(t, trailing)), xzOf(t, trailing)},
		{"gzip that ends early", "", gzipOf(t, p.archive)[:len(p.archive)/2]},
		{"zstd of no archive", "", zstdOf(t, []byte("no archive at all"))},
		{"zstd followed by bytes", "", append(zstdOf(t, p.archive), "and more"...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			hash := c.hash
			if hash == "" {
				hash = fileHash(c.body)
			}
			wantStatus(t, "PutNAR", f.up.PutNAR(background(t), hash, bytes.NewReader(c.body)), http.StatusBadRequest)
			f.settled()
			f.nothingStaged()
		})
	}
	if _, ok := f.root(testPrefix + "nar/" + p.narHash + ".nar"); ok {
		t.Error("a reference was written")
	}
	f.up.mu.Lock()
	defer f.up.mu.Unlock()
	if len(f.up.entries) != 0 {
		t.Errorf("%d entries after refused uploads", len(f.up.entries))
	}
}

func TestUnsupportedCompression(t *testing.T) {
	f := newFixture(t, config{})
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"brotli", []byte{0x8b, 0x02, 0x80, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a, 0x03}},
		{"lzip", append([]byte("LZIP\x01\x0c"), noise(4, 64)...)},
		{"empty", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := f.up.PutNAR(background(t), fileHash(c.body), bytes.NewReader(c.body))
			wantStatus(t, "PutNAR", err, http.StatusUnsupportedMediaType)
			f.nothingStaged()
		})
	}
}

// counting counts the calls of Read.
type counting struct {
	r     io.Reader
	reads int
}

func (c *counting) Read(p []byte) (int, error) {
	c.reads++
	return c.r.Read(p)
}

func TestServerDown(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "offline", smallTree(1))
	f.srv.Stop()

	// A copy fails at its first NAR, before the body is read.
	body := &counting{r: bytes.NewReader(p.archive)}
	started := time.Now()
	wantStatus(t, "PutNAR with the server down", f.up.PutNAR(background(t), p.narHash, body), http.StatusBadGateway)
	if body.reads != 0 {
		t.Errorf("the body was read %d times", body.reads)
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Errorf("the answer took %s", took)
	}
	_, err := f.up.HeadNAR(background(t), p.narHash)
	wantStatus(t, "HeadNAR with the server down", err, http.StatusBadGateway)
	wantStatus(t, "PutNarinfo with the server down", f.putNarinfo(p), http.StatusBadGateway)
	f.settled()
	f.nothingStaged()
}

func TestPushFailsForNowThenARepeatSucceeds(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "retried", smallTree(1))
	arrived, release := f.hold(putOf(p.root))
	f.putNAR(p)
	await(t, "the push reaches the bucket", arrived)

	// The server goes away under the push.
	f.srv.Stop()
	release()
	f.inState(p, stateFailedForNow)
	staged := f.staged()
	if len(staged) == 0 {
		t.Fatal("the NAR of a push that failed for now is not staged any more")
	}
	if ok, err := f.up.HeadNAR(background(t), p.narHash); err == nil {
		t.Errorf("HeadNAR = %v with the server down, want an error", ok)
	}

	wantStatus(t, "PutNarinfo with the server down", f.putNarinfo(p), http.StatusBadGateway)
	if st, asked, ok := f.entry(p.narHash); !ok || st != stateFailedForNow || !asked {
		t.Fatalf("the work is %v, asked %v (%v), want failed for now and asked for", st, asked, ok)
	}
	if got := f.staged(); !slices.Equal(got, staged) {
		t.Fatalf("staging holds %v after the 502, want %v as before", got, staged)
	}

	// Nix repeats a request that got 502: the repeat finds the NAR.
	f.srv.Restart()
	eventually(t, "a repeat of the narinfo succeeds", func() bool {
		err := f.putNarinfo(p)
		if err != nil {
			wantStatus(t, "PutNarinfo while the server comes back", err, http.StatusBadGateway)
			time.Sleep(50 * time.Millisecond)
		}
		return err == nil
	})
	f.settled()
	f.pointsAt(testPrefix+"nar/"+p.narHash+".nar", p.root)
	infoRoot, _ := narinfoRoot(t, p.stored(t))
	f.pointsAt(testPrefix+p.hash+".narinfo", infoRoot)
	f.nothingStaged()
	if _, _, ok := f.entry(p.narHash); ok {
		t.Error("the finished push left an entry")
	}
}

func TestANARPutStartsFailedWorkAgain(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "again", smallTree(1))
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		http.Error(w, "the bucket is broken", http.StatusInternalServerError)
		return true
	})
	f.putNAR(p)
	f.inState(p, stateFailedForNow)
	f.settled()
	staged := f.staged()

	// The same NAR again: its copy is dropped, the staged one is pushed.
	f.bucket.answer(nil)
	f.putNAR(p)
	f.settled()
	f.pointsAt(testPrefix+"nar/"+p.narHash+".nar", p.root)
	f.nothingStaged()
	if len(staged) == 0 {
		t.Error("nothing was staged after the push had failed for now")
	}
	if got := narOf(t, f.store, p.narHash); !bytes.Equal(got, p.archive) {
		t.Error("the archive served is not the one uploaded")
	}
}

func TestPushFailsForGood(t *testing.T) {
	// A server that takes no pack above 4 KiB of objects.
	f := newFixture(t, config{options: Options{ContentPushes: 1}, server: testserver.Options{MaxPackBytes: 4 << 10}})
	small, large := pathOf(t, "small", tinyTree(1)), pathOf(t, "large", smallTree(1))
	head := func(p *storePath) bool {
		t.Helper()
		ok, err := f.up.HeadNAR(background(t), p.narHash)
		if err != nil {
			t.Fatalf("HeadNAR: %v", err)
		}
		return ok
	}

	// The large one waits behind the small one, whose upload is held up.
	arrived, release := f.hold(putOf(small.root))
	f.putNAR(small)
	f.putNAR(large)
	await(t, "the first push reaches the bucket", arrived)
	if !head(large) {
		t.Error("HeadNAR is false for work that is queued")
	}
	release()

	e := wantStatus(t, "PutNarinfo for content the server refuses", f.putNarinfo(large), http.StatusUnprocessableEntity)
	if !strings.Contains(e.Msg, "above this server's limit") {
		t.Errorf("the message does not carry the server's reason: %s", e.Msg)
	}
	if st, _, ok := f.entry(large.narHash); !ok || st != stateFailedForGood {
		t.Fatalf("the work is %v (%v), want failed for good", st, ok)
	}
	if head(large) {
		t.Error("HeadNAR is true after the push failed for good")
	}
	// The refusal of one push left the other running.
	if err := f.putNarinfo(small); err != nil {
		t.Fatalf("PutNarinfo for the content the server took: %v", err)
	}
	f.settled()
	f.pointsAt(testPrefix+"nar/"+small.narHash+".nar", small.root)
	f.nothingStaged()

	// It stays refused, with its reason, while the server cannot be asked
	// whether the content has come another way.
	f.srv.Stop()
	e = wantStatus(t, "PutNarinfo again", f.putNarinfo(large), http.StatusUnprocessableEntity)
	if !strings.Contains(e.Msg, "above this server's limit") {
		t.Errorf("the message does not carry the server's reason: %s", e.Msg)
	}
	f.srv.Restart()

	// The next NAR PUT replaces the entry, and the work starts on the new
	// copy.
	eventually(t, "the server is back", func() bool {
		return f.up.PutNAR(background(t), large.narHash, bytes.NewReader(large.archive)) == nil
	})
	wantStatus(t, "PutNarinfo after the NAR was sent again", f.putNarinfo(large), http.StatusUnprocessableEntity)
	f.settled()
	f.nothingStaged()
	if _, ok := f.root(testPrefix + "nar/" + large.narHash + ".nar"); ok {
		t.Error("the refused reference is on the server")
	}
}

func TestHeadForQueuedWork(t *testing.T) {
	f := newFixture(t, config{options: Options{ContentPushes: 1}})
	first, second := pathOf(t, "first", smallTree(1)), pathOf(t, "second", smallTree(2))
	never := pathOf(t, "never", tinyTree(3))
	head := func(p *storePath) bool {
		t.Helper()
		ok, err := f.up.HeadNAR(background(t), p.narHash)
		if err != nil {
			t.Fatalf("HeadNAR: %v", err)
		}
		return ok
	}
	arrived, release := f.hold(putOf(first.root))
	f.putNAR(first)
	f.putNAR(second)
	await(t, "the first push reaches the bucket", arrived)
	if st, _, _ := f.entry(second.narHash); st != stateQueued {
		t.Fatalf("the second work is %v, want queued", st)
	}

	f.bucket.forget()
	if !head(first) {
		t.Error("HeadNAR is false for work that runs")
	}
	if !head(second) {
		t.Error("HeadNAR is false for work that is queued")
	}
	if head(never) {
		t.Error("HeadNAR is true for a NAR nobody sent")
	}
	if _, ok := f.root(testPrefix + "nar/" + second.narHash + ".nar"); ok {
		t.Fatal("the queued content is on the server already")
	}
	release()
	f.settled()
	if !head(second) {
		t.Error("HeadNAR is false after the push")
	}
}

func TestHeadForAReferenceOnTheServer(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "pushed elsewhere", smallTree(1))
	if ok, err := f.up.HeadNAR(background(t), p.narHash); err != nil || ok {
		t.Fatalf("HeadNAR = %v, %v before the reference exists", ok, err)
	}
	f.push(testPrefix+"nar/"+p.narHash+".nar", p.root, p.objs, 0.5)
	f.bucket.forget()
	if ok, err := f.up.HeadNAR(background(t), p.narHash); err != nil || !ok {
		t.Fatalf("HeadNAR = %v, %v for a reference on the server", ok, err)
	}
	// The question downloads nothing and caches nothing.
	if seen := f.bucket.requests(http.MethodGet, ""); len(seen) != 0 {
		t.Errorf("HeadNAR downloaded %v", seen)
	}
	if f.cache.Size() != 0 {
		t.Errorf("HeadNAR left %d bytes in the cache", f.cache.Size())
	}
	// A hash that is none names nothing, and the server is not asked.
	f.srv.Stop()
	for _, hash := range []string{"", "abc", strings.Repeat("e", 52), p.narHash + "0"} {
		if ok, err := f.up.HeadNAR(background(t), hash); err != nil || ok {
			t.Errorf("HeadNAR(%q) = %v, %v", hash, ok, err)
		}
	}
}

func TestNarinfoOverOneMiB(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "many-references", tinyTree(1))
	f.putNAR(p)
	f.settled()

	// 21,000 references of 52 bytes: the stored text is above 1 MiB.
	var references []string
	for i := range 21_000 {
		references = append(references, storeHash(fmt.Sprint("reference ", i))+"-dependency-"+fmt.Sprintf("%07d", i))
	}
	body := p.nixText(references...)
	if info, err := narinfo.Parse(body); err != nil || len(info.Stored()) <= narinfo.MaxStored {
		t.Fatalf("the narinfo of the test is not one above 1 MiB (%v)", err)
	}
	wantStatus(t, "PutNarinfo of a text above 1 MiB", f.up.PutNarinfo(background(t), p.hash, body), http.StatusBadRequest)

	// A body above the limit is refused whatever it holds.
	padded := append(p.nixText(), bytes.Repeat([]byte("Ignored: "+strings.Repeat("x", 90)+"\n"), 11_000)...)
	if info, err := narinfo.Parse(padded); err != nil || len(info.Stored()) > 1<<10 || len(padded) <= narinfo.MaxStored {
		t.Fatalf("the padded narinfo of the test is not a short one in a long body (%v)", err)
	}
	wantStatus(t, "PutNarinfo of a body above 1 MiB", f.up.PutNarinfo(background(t), p.hash, padded), http.StatusBadRequest)

	// A body within the limit whose stored text is above it: Nix's lines
	// about the compressed file are not needed, and narrenhaus writes its
	// own.
	bare := func(n int) []byte {
		return fmt.Appendf(nil, "StorePath: /nix/store/%s-%s\nNarHash: sha256:%s\nNarSize: %d\nReferences: %s\n",
			p.hash, p.name, p.narHash, p.size, strings.Join(references[:n], " "))
	}
	n := (narinfo.MaxStored - len(bare(0))) / (len(references[0]) + 1)
	if len(bare(n)) > narinfo.MaxStored || len(bare(n+2)) <= narinfo.MaxStored {
		t.Fatalf("a body of %d bytes is not one just within the limit", len(bare(n)))
	}
	if info, err := narinfo.Parse(bare(n)); err != nil || len(info.Stored()) <= narinfo.MaxStored {
		t.Fatalf("the bare narinfo of the test is not stored as a text above 1 MiB (%v)", err)
	}
	wantStatus(t, "PutNarinfo of a body whose stored text is above 1 MiB", f.up.PutNarinfo(background(t), p.hash, bare(n)), http.StatusBadRequest)

	if _, ok := f.root(testPrefix + p.hash + ".narinfo"); ok {
		t.Error("a narinfo reference was written")
	}
	// Just below the limit it is taken.
	references = references[:19_000]
	if err := f.putNarinfo(p, references...); err != nil {
		t.Fatalf("PutNarinfo of a text below 1 MiB: %v", err)
	}
	wantRoot, _ := narinfoRoot(t, p.stored(t, references...))
	f.pointsAt(testPrefix+p.hash+".narinfo", wantRoot)
}

// The frozen narinfo: a fixed store path gives this stored text, and the
// text this root. A change of either is a change of what every narrenhaus
// already wrote and reads; it is not answered by pasting new values.
const (
	frozenStoreHash = "0c6kzph7l0dcbfmjap64f0czdafn3b7x"
	frozenStored    = "StorePath: /nix/store/0c6kzph7l0dcbfmjap64f0czdafn3b7x-hello-2.12\n" +
		"URL: nar/13y35a6a03n7gv19dvnpycn16s46rvli7vjzicwsmmd6xdxbmcbl.nar\n" +
		"Compression: none\n" +
		"FileHash: sha256:13y35a6a03n7gv19dvnpycn16s46rvli7vjzicwsmmd6xdxbmcbl\n" +
		"FileSize: 1064\n" +
		"NarHash: sha256:13y35a6a03n7gv19dvnpycn16s46rvli7vjzicwsmmd6xdxbmcbl\n" +
		"NarSize: 1064\n" +
		"References: 0c6kzph7l0dcbfmjap64f0czdafn3b7x-hello-2.12 a04cyf2srw7jyhxyrcxvzds0kbqrywzw-glibc-2.40-66\n" +
		"Deriver: 0jjm4sxdhaq45ph0nyk3mi9ip2yzykq0-hello-2.12.drv\n" +
		"Sig: another.example-1:AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg==\n" +
		"Sig: cache.example-1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ==\n"
	frozenNarinfo = "1cf5ce5ba484839aae10c3de614ba0e388c4ad3c30b1ed674941c42d59bc0201"
	frozenContent = "2e7535ccebdf8ff3cd9b6f0fa18dfede4d121500e3db5a989a2421fb13210221"
)

func TestFrozenNarinfoRoot(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", dir(map[string]node{
		"bin":    dir(map[string]node{"hello": script("#!/bin/sh\necho hello\n")}),
		"latest": link("bin/hello"),
		"share":  dir(map[string]node{"doc": text("frozen\n")}),
	}))
	p.hash = frozenStoreHash
	body := fmt.Appendf(nil, "StorePath: /nix/store/%s-hello-2.12\nURL: nar/1w1fff338fvdw53sqgamddn1b2xgds473pv6y13gizdbqjv4i5p3.nar.xz\nCompression: xz\n"+
		"FileHash: sha256:1w1fff338fvdw53sqgamddn1b2xgds473pv6y13gizdbqjv4i5p3\nFileSize: 4029176\n"+
		"NarHash: sha256:%s\nNarSize: %d\n"+
		"References: 0c6kzph7l0dcbfmjap64f0czdafn3b7x-hello-2.12 a04cyf2srw7jyhxyrcxvzds0kbqrywzw-glibc-2.40-66\n"+
		"Deriver: 0jjm4sxdhaq45ph0nyk3mi9ip2yzykq0-hello-2.12.drv\n"+
		"Sig: cache.example-1:%s\nSig: another.example-1:%s\n",
		frozenStoreHash, p.narHash, p.size,
		base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 64)), base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 64)))

	f.putNAR(p)
	if err := f.up.PutNarinfo(background(t), frozenStoreHash, body); err != nil {
		t.Fatalf("PutNarinfo: %v", err)
	}
	root, ok := f.root(testPrefix + frozenStoreHash + ".narinfo")
	if !ok {
		t.Fatal("the server has no narinfo reference")
	}
	if got := root.String(); got != frozenNarinfo {
		t.Errorf("the narinfo's root is\n%s\nwant\n%s", got, frozenNarinfo)
	}
	if root.Type() != key.Blob {
		t.Errorf("the narinfo's root is a %s, want a Blob", root.Type())
	}
	info, err := f.reader().Narinfo(background(t), frozenStoreHash)
	if err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if got := string(info.Stored()); got != frozenStored {
		t.Errorf("the stored narinfo is\n%s\nwant\n%s", got, frozenStored)
	}
	if got := p.root.String(); got != frozenContent {
		t.Errorf("the content's root is\n%s\nwant\n%s", got, frozenContent)
	}
	f.pointsAt(testPrefix+"nar/"+p.narHash+".nar", p.root)
}

func TestSweep(t *testing.T) {
	f := newFixture(t, config{options: Options{IdleExpiry: time.Minute}})
	p, q := pathOf(t, "left", smallTree(1)), pathOf(t, "held", smallTree(2))

	// The bucket takes no upload; that of q it keeps waiting first.
	gate := make(chan struct{})
	arrived := make(chan struct{}, 16)
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		if putOf(q.root)(r) {
			select {
			case arrived <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-r.Context().Done():
			}
		}
		http.Error(w, "the bucket is broken", http.StatusInternalServerError)
		return true
	})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)

	f.putNAR(p)
	f.inState(p, stateFailedForNow)
	f.settled()
	staged := f.staged()
	if len(staged) == 0 {
		t.Fatal("the NAR of a push that failed for now is not staged")
	}

	// Not before the uploader has been idle for the whole time.
	f.up.Sweep(time.Now().Add(59 * time.Second))
	if got := f.staged(); !slices.Equal(got, staged) {
		t.Fatalf("staging holds %v after 59 idle seconds, want %v", got, staged)
	}

	// Not while a push runs, however long ago the NAR was staged.
	f.putNAR(q)
	await(t, "the second push reaches the bucket", arrived)
	f.up.Sweep(time.Now().Add(24 * time.Hour))
	if _, _, ok := f.entry(p.narHash); !ok {
		t.Fatal("the entry was removed while a push ran")
	}
	if got := f.staged(); len(got) <= len(staged) {
		t.Fatalf("staging holds %v while a push runs", got)
	}
	release()
	f.inState(q, stateFailedForNow)
	f.settled()

	// The time counts from the end of the last work.
	f.up.Sweep(time.Now().Add(59 * time.Second))
	if len(f.staged()) == 0 {
		t.Fatal("staging was emptied 59 seconds after the last push")
	}
	f.up.Sweep(time.Now().Add(61 * time.Second))
	f.nothingStaged()
	for _, x := range []*storePath{p, q} {
		if _, _, ok := f.entry(x.narHash); ok {
			t.Errorf("the entry of %s outlived its staged NAR", x.name)
		}
	}

	// The NAR is gone as if it had never been sent.
	f.bucket.answer(nil)
	e := wantStatus(t, "PutNarinfo after the sweep", f.putNarinfo(p), http.StatusBadRequest)
	if !strings.Contains(e.Msg, p.narHash) {
		t.Errorf("the message does not name the NAR hash: %s", e.Msg)
	}
	// And can be sent again.
	f.upload(p)
	f.settled()
	f.nothingStaged()
}

func TestStagingWriteFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes to a directory whatever its mode")
	}
	f := newFixture(t, config{})
	p := pathOf(t, "unwritable", smallTree(1))
	there := pathOf(t, "there", tinyTree(2))
	f.push(testPrefix+"nar/"+there.narHash+".nar", there.root, there.objs, 0.5)

	staging := filepath.Join(f.dir, "staging")
	if err := os.Chmod(staging, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(staging, 0o755) })

	wantStatus(t, "PutNAR into a staging directory that takes nothing", f.up.PutNAR(background(t), p.narHash, bytes.NewReader(p.archive)), http.StatusInsufficientStorage)
	// The narinfo of content the server has needs a file of its own.
	wantStatus(t, "PutNarinfo into a staging directory that takes nothing", f.putNarinfo(there), http.StatusInsufficientStorage)
	f.settled()
	f.nothingStaged()
	if _, _, ok := f.entry(p.narHash); ok {
		t.Error("a refused upload left an entry")
	}

	// Writable again, the same requests are served.
	if err := os.Chmod(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	f.upload(p)
	if err := f.putNarinfo(there); err != nil {
		t.Fatalf("PutNarinfo: %v", err)
	}
}

func TestARequestThatGivesUp(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "patient", smallTree(1))
	arrived, release := f.hold(putOf(p.root))
	f.putNAR(p)
	await(t, "the push reaches the bucket", arrived)

	// The client goes away; the work is narrenhaus's and goes on.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := f.up.PutNarinfo(ctx, p.hash, p.nixText())
	wantStatus(t, "PutNarinfo of a client that left", err, http.StatusBadGateway)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v, want one that wraps the context's", err)
	}
	if f.up.Idle() {
		t.Error("Idle while the work of a request that left goes on")
	}
	release()
	f.settled()
	infoRoot, _ := narinfoRoot(t, p.stored(t))
	f.pointsAt(testPrefix+p.hash+".narinfo", infoRoot)
	if err := f.putNarinfo(p); err != nil {
		t.Fatalf("PutNarinfo again: %v", err)
	}
	if n := f.bucket.uploads(infoRoot); n != 1 {
		t.Errorf("the narinfo was uploaded %d times, want once", n)
	}
}

func TestClose(t *testing.T) {
	f := newFixture(t, config{options: Options{ContentPushes: 1}})
	p, q := pathOf(t, "running", smallTree(1)), pathOf(t, "queued", smallTree(2))
	arrived, _ := f.hold(putOf(p.root))
	f.putNAR(p)
	f.putNAR(q)
	await(t, "the push reaches the bucket", arrived)
	waiting := make(chan error, 1)
	go func() { waiting <- f.putNarinfo(q) }()
	eventually(t, "the narinfo waits for its content", func() bool {
		_, asked, _ := f.entry(q.narHash)
		return asked
	})

	// Close ends the push that runs, starts none, and lets go of whoever
	// waits.
	closed := make(chan struct{})
	go func() {
		f.up.Close()
		close(closed)
	}()
	await(t, "Close returns", closed)
	wantStatus(t, "PutNarinfo that waited when the uploader was closed", await(t, "PutNarinfo returns", waiting), http.StatusBadGateway)
	if _, ok := f.root(testPrefix + "nar/" + q.narHash + ".nar"); ok {
		t.Error("the queued content was pushed after Close")
	}

	wantStatus(t, "PutNAR after Close", f.up.PutNAR(background(t), p.narHash, bytes.NewReader(p.archive)), http.StatusBadGateway)
	wantStatus(t, "PutNarinfo after Close", f.putNarinfo(p), http.StatusBadGateway)
	f.up.Sweep(time.Now().Add(24 * time.Hour))
	f.up.Close()
}

func TestErrorsFitAnAnswer(t *testing.T) {
	long := strings.Repeat("a reason that goes on and on, ", 200)
	e := failf(http.StatusBadGateway, nil, "the server could not be asked: %s", long)
	if len(e.Msg) >= 1024 {
		t.Errorf("a message of %d bytes", len(e.Msg))
	}
	if !strings.HasPrefix(e.Msg, "the server could not be asked: a reason") {
		t.Errorf("the message does not begin with what was said: %q", e.Msg[:40])
	}
	// What is no text of one line is made one.
	e = failf(http.StatusBadRequest, nil, "malformed: %s", "line one\nline two\x00\xff\u00e9")
	if strings.ContainsAny(e.Msg, "\n\x00") || !strings.Contains(e.Msg, "line one") || !strings.Contains(e.Msg, "line two") {
		t.Errorf("message %q", e.Msg)
	}
	cause := errors.New("the cause")
	if e := failf(http.StatusBadGateway, cause, "failed"); !errors.Is(e, cause) {
		t.Error("an error does not wrap its cause")
	}
}

// sealed returns a staged file of objs without the keys in skip.
func (f *fixture) sealed(objs *objects, skip ...key.Key) *packcache.Sealed {
	f.t.Helper()
	w, err := f.cache.NewStaged()
	if err != nil {
		f.t.Fatal(err)
	}
	for k, data := range objs.m {
		if slices.Contains(skip, k) {
			continue
		}
		if err := w.Add(k, data); err != nil {
			f.t.Fatal(err)
		}
	}
	file, err := w.Seal()
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(file.Discard)
	return file
}

func TestReproduce(t *testing.T) {
	// What does not rebuild to the NAR that was received is not taken: the
	// step that guards against a defect of narrenhaus, shown on staged
	// files that such a defect might leave.
	f := newFixture(t, config{})
	p := pathOf(t, "sound", smallTree(1))
	if err := f.up.reproduce(f.sealed(p.objs), p.root, p.sum, p.size); err != nil {
		t.Fatalf("a sound staged NAR: %v", err)
	}

	other := sha256.Sum256([]byte("another NAR"))
	defect := func(what string, err error) {
		t.Helper()
		e := wantStatus(t, what, err, http.StatusUnprocessableEntity)
		if !strings.Contains(e.Msg, "cannot reproduce this NAR") {
			t.Errorf("%s: the message does not say what is wrong: %s", what, e.Msg)
		}
	}

	// The recipe is of another NAR than the one received.
	defect("another hash received", f.up.reproduce(f.sealed(p.objs), p.root, other, p.size))
	defect("another size received", f.up.reproduce(f.sealed(p.objs), p.root, p.sum, p.size+8))

	// The tree is not the one of the NAR its recipe states.
	node, _, _, err := nartree.Ingest(bytes.NewReader(p.archive), func(fstree.Object) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	stating := func(sum [32]byte, size uint64) (key.Key, *objects) {
		t.Helper()
		objs := &objects{m: maps.Clone(p.objs.m)}
		root, err := nartree.Root(node, sum, size, objs.emit)
		if err != nil {
			t.Fatal(err)
		}
		return root, objs
	}
	root, objs := stating(other, p.size)
	defect("a tree with another hash", f.up.reproduce(f.sealed(objs), root, other, p.size))
	root, objs = stating(p.sum, p.size-8)
	defect("a tree that is longer", f.up.reproduce(f.sealed(objs), root, p.sum, p.size-8))
	root, objs = stating(p.sum, p.size+8)
	defect("a tree that is shorter", f.up.reproduce(f.sealed(objs), root, p.sum, p.size+8))

	// An object is missing from the staged file.
	for k := range p.objs.m {
		if k == p.root {
			continue
		}
		defect("without the object "+k.String(), f.up.reproduce(f.sealed(p.objs, k), p.root, p.sum, p.size))
	}
	if !f.log.has("level=ERROR", "defect") {
		t.Error("a defect is not logged as one")
	}
}

func TestANARThatDoesNotReproduce(t *testing.T) {
	// Spec 5.1 step 6 as an upload meets it: a staged copy that lacks an
	// object of the NAR, which is what a defect of narrenhaus would leave,
	// is found by rebuilding it before the request is answered.
	f := newFixture(t, config{})
	p := pathOf(t, "defect", smallTree(1))
	for _, lost := range slices.SortedFunc(maps.Keys(p.objs.m), func(a, b key.Key) int { return bytes.Compare(a[:], b[:]) }) {
		f.up.tamper = func(o fstree.Object) (fstree.Object, bool) { return o, o.Key != lost }
		err := f.up.PutNAR(background(t), p.narHash, bytes.NewReader(p.archive))
		e := wantStatus(t, "PutNAR of a NAR staged without "+lost.String(), err, http.StatusUnprocessableEntity)
		if !strings.Contains(e.Msg, "cannot reproduce this NAR") {
			t.Errorf("the message does not say what is wrong: %s", e.Msg)
		}
		f.nothingStaged()
		if _, _, ok := f.entry(p.narHash); ok {
			t.Fatal("a refused upload left an entry")
		}
	}
	if !f.log.has("level=ERROR", "defect") {
		t.Error("a defect is not logged as one")
	}
	f.settled()
	if _, ok := f.root(testPrefix + "nar/" + p.narHash + ".nar"); ok {
		t.Error("the content of a refused upload is on the server")
	}

	// Staged whole, the same NAR is taken.
	f.up.tamper = nil
	f.upload(p)
}

func TestALimitOfStagingIsNoFailedWrite(t *testing.T) {
	// What a staged file takes no object for is not the disk's doing: a
	// repeat cures nothing, so the answer is not the 5xx that Nix repeats.
	f := newFixture(t, config{})
	p := pathOf(t, "beyond", smallTree(1))
	f.up.tamper = func(o fstree.Object) (fstree.Object, bool) {
		if o.Key == p.root {
			o.Key[key.Size-1] |= 0x08 // a bit no key has set
		}
		return o, true
	}
	err := f.up.PutNAR(background(t), p.narHash, bytes.NewReader(p.archive))
	e := wantStatus(t, "PutNAR of a NAR with an object the staged file refuses", err, http.StatusUnprocessableEntity)
	if !strings.Contains(e.Msg, "not canonical") {
		t.Errorf("the message does not name what was refused: %s", e.Msg)
	}
	f.nothingStaged()
	if _, _, ok := f.entry(p.narHash); ok {
		t.Error("a refused upload left an entry")
	}
	f.up.tamper = nil

	// The three limits, and what is none of them.
	small := fstree.Object{Key: p.root, Bytes: []byte("an object")}
	refusal := errors.New("packcache: the file takes no more")
	diskFull := fmt.Errorf("packcache: %w", &fs.PathError{Op: "write", Path: "staging/1.data", Err: syscall.ENOSPC})
	huge := fstree.Object{Key: p.root, Bytes: make([]byte, amberpack.MaxPayload+1)}
	bad := small
	bad.Key[key.Size-1] |= 0x08
	for _, c := range []struct {
		name    string
		err     error
		o       fstree.Object
		objects int
		want    string // part of the limit's name, "" for a failed write
	}{
		{"an object above the payload limit", refusal, huge, 3, fmt.Sprint(amberpack.MaxPayload)},
		{"a key that is not canonical", refusal, bad, 3, "not canonical"},
		{"one object more than a pack holds", refusal, small, packfile.MaxEntries, fmt.Sprint(packfile.MaxEntries)},
		{"a full disk", diskFull, small, 3, ""},
		{"a full disk after as many objects as a pack holds", diskFull, small, packfile.MaxEntries + 5, ""},
		{"another refusal before the pack is full", refusal, small, packfile.MaxEntries - 1, ""},
	} {
		got := beyond(c.err, c.o, c.objects)
		if (got == "") != (c.want == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: the limit is %q, want one that names %q", c.name, got, c.want)
		}
		e := f.up.notStaged(c.err, c.o, c.objects)
		want := http.StatusUnprocessableEntity
		if c.want == "" {
			want = http.StatusInsufficientStorage
		}
		wantStatus(t, c.name, e, want)
		if !errors.Is(e, c.err) {
			t.Errorf("%s: the answer does not wrap the file's error", c.name)
		}
	}
}

func TestCloseStopsStaging(t *testing.T) {
	// A NAR that is still arriving when the uploader is closed is not
	// read to its end and staged for nothing.
	f := newFixture(t, config{})
	p := pathOf(t, "half sent", dir(map[string]node{"a": file(noise(1, 16<<20)), "b": file(noise(2, 16<<20))}))
	half := len(p.archive) / 2

	in, out := io.Pipe()
	defer in.Close()
	var read atomic.Int64
	body := readerFunc(func(b []byte) (int, error) {
		n, err := in.Read(b)
		read.Add(int64(n))
		return n, err
	})
	answer := make(chan error, 1)
	go func() { answer <- f.up.PutNAR(background(t), p.narHash, body) }()
	// The pipe takes the first half when all of it has been read.
	if _, err := out.Write(p.archive[:half]); err != nil {
		t.Fatal(err)
	}
	if len(f.staged()) == 0 {
		t.Fatal("nothing is staged of a NAR that is half read")
	}

	f.up.Close()
	go func() {
		out.Write(p.archive[half:])
		out.Close()
	}()
	e := wantStatus(t, "PutNAR of a NAR that was arriving at Close", await(t, "PutNAR returns", answer), http.StatusBadGateway)
	if !strings.Contains(e.Msg, "shutting down") {
		t.Errorf("the message does not say why: %s", e.Msg)
	}
	// Up to the end of the chunk that was being read, and no further.
	if after := read.Load() - int64(half); after > 4<<20 {
		t.Errorf("%d bytes of the body were read after Close, of %d that came", after, len(p.archive)-half)
	}
	f.nothingStaged()
}

// readerFunc is a function as an io.Reader.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// damaging makes the bucket take the first upload of the content pack of
// each of paths with one byte changed: a server that verifies refuses that
// push for good.
func (f *fixture) damaging(paths ...*storePath) {
	var mu sync.Mutex
	seen := map[string]bool{}
	f.bucket.answer(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, ".data") {
			return false
		}
		root := path.Base(path.Dir(r.URL.Path))
		mu.Lock()
		first := !seen[root]
		seen[root] = true
		mu.Unlock()
		if first && slices.ContainsFunc(paths, func(p *storePath) bool { return p.root.String() == root }) {
			body, _ := io.ReadAll(r.Body)
			body[len(body)/2] ^= 0xff
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		return false
	})
}

func TestRefusedForGoodThenOnTheServer(t *testing.T) {
	// A refusal for good is of one push, not of the NAR: its pack may have
	// been damaged on the way. When the content reaches the server another
	// way afterwards, HEAD says so, Nix sends no NAR again, and the
	// narinfo has to be taken: the refusal is not kept against the server.
	f := newFixture(t, config{})
	asked, alone, again := pathOf(t, "asked-first", smallTree(1)), pathOf(t, "narinfo-alone", smallTree(2)), pathOf(t, "sent-again", smallTree(3))
	paths := []*storePath{asked, alone, again}
	contentName := func(p *storePath) string { return testPrefix + "nar/" + p.narHash + ".nar" }
	head := func(p *storePath) bool {
		t.Helper()
		ok, err := f.up.HeadNAR(background(t), p.narHash)
		if err != nil {
			t.Fatalf("HeadNAR: %v", err)
		}
		return ok
	}
	f.damaging(paths...)
	for _, p := range paths {
		f.putNAR(p)
		f.inState(p, stateFailedForGood)
	}
	f.settled()

	// While the server has none of it, the refusal stands.
	for _, p := range paths {
		e := wantStatus(t, "PutNarinfo for content the server refused", f.putNarinfo(p), http.StatusUnprocessableEntity)
		if !strings.Contains(e.Msg, "malformed_pack") {
			t.Errorf("the message does not carry the server's reason: %s", e.Msg)
		}
		if head(p) {
			t.Errorf("HeadNAR is true for %s, which the server has not", p.name)
		}
		if st, _, ok := f.entry(p.narHash); !ok || st != stateFailedForGood {
			t.Fatalf("the work for %s is %v (%v), want failed for good", p.name, st, ok)
		}
	}

	// A colleague uploads two of the NARs.
	for _, p := range []*storePath{asked, alone} {
		f.push(contentName(p), p.root, p.objs, 0.5)
	}

	// With ?compression=none Nix asks first, is told that the NAR is
	// there, and sends the narinfo alone.
	if !head(asked) {
		t.Fatal("HeadNAR is false for content the server has")
	}
	if _, _, ok := f.entry(asked.narHash); ok {
		t.Error("the refusal outlived HeadNAR finding the content on the server")
	}
	if err := f.putNarinfo(asked); err != nil {
		t.Fatalf("PutNarinfo after HeadNAR found the content: %v", err)
	}
	// The narinfo without the question before it.
	if err := f.putNarinfo(alone); err != nil {
		t.Fatalf("PutNarinfo for content that reached the server another way: %v", err)
	}
	if _, _, ok := f.entry(alone.narHash); ok {
		t.Error("the refusal outlived PutNarinfo finding the content on the server")
	}
	for _, p := range []*storePath{asked, alone} {
		infoRoot, _ := narinfoRoot(t, p.stored(t))
		f.pointsAt(testPrefix+p.hash+".narinfo", infoRoot)
	}

	// The refusal that is dropped is the one that was looked at. An entry
	// whose NAR was sent again meanwhile has work of its own, which is not
	// forgotten with it.
	f.up.mu.Lock()
	w := f.up.entries[again.narHash]
	refused := w.attempt
	f.up.mu.Unlock()
	arrived, release := f.hold(putOf(again.root))
	f.putNAR(again)
	await(t, "the second push reaches the bucket", arrived)
	f.up.forget(w, refused)
	if st, _, ok := f.entry(again.narHash); !ok || st != stateRunning {
		t.Fatalf("the work that runs again is %v (%v), want running", st, ok)
	}
	release()
	if err := f.putNarinfo(again); err != nil {
		t.Fatalf("PutNarinfo after the NAR was sent again: %v", err)
	}
	f.settled()
	f.pointsAt(contentName(again), again.root)
	f.nothingStaged()
}

func TestABodyThatAsksForTooMuchMemory(t *testing.T) {
	// 84 bytes as `xz --lzma2=dict=1536MiB` wrote them: the body states
	// the dictionary its reader is to hold. It is refused as a body that
	// cannot be read, before the memory is asked for.
	body, err := base64.StdEncoding.DecodeString(
		"/Td6WFoAAATm1rRGBMAXEyEBJQAAAAAAAAAAAG9QKl0BABJhIE5BUiB3b3VsZCBiZSBoZXJlAACPKcuinFM2xgABMxPFkVNQH7bzfQEAAAAABFla")
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, config{})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = f.up.PutNAR(background(t), fileHash(body), bytes.NewReader(body))
	runtime.ReadMemStats(&after)
	wantStatus(t, "PutNAR of an xz body with a dictionary of 1536 MiB", err, http.StatusBadRequest)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 256<<20 {
		t.Errorf("a body of %d bytes made narrenhaus allocate %d MiB", len(body), grown>>20)
	}
	f.nothingStaged()
}

func TestDefaults(t *testing.T) {
	// The limits the design fixes (spec 5 and 5.1). The command passes
	// none of its own, so these are the ones it runs with.
	u := New(context.Background(), nil, nil, nil, Options{})
	defer u.Close()
	if u.contentPushes != 4 {
		t.Errorf("%d content pushes at once, want 4", u.contentPushes)
	}
	if n := cap(u.narinfoPlaces); n != 8 {
		t.Errorf("%d narinfo pushes at once, want 8", n)
	}
	if u.minDedup != 0.5 {
		t.Errorf("MinDedup is %v, want 0.5", u.minDedup)
	}
	if u.idleExpiry != 15*time.Minute {
		t.Errorf("staged NARs are deleted after %v idle, want 15 minutes", u.idleExpiry)
	}
}
