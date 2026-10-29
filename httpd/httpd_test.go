package httpd

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/ulikunitz/xz"

	"github.com/amber-store/narrenhaus/nar"
	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/refs"
	"github.com/amber-store/narrenhaus/remote"
	"github.com/amber-store/narrenhaus/testserver"
	"github.com/amber-store/narrenhaus/upload"
)

const testPrefix = "acme/"

// quiet drops what a Remote logs.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// logBuffer keeps what the handler, the uploader and the store log. A test
// that fails prints it, and so does every test when NARRENHAUS_TEST_LOG is
// set.
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
	seen []string // "GET /jaccard-test/packs/<root>/<upload>.data"
}

func (b *bucket) serve(w http.ResponseWriter, r *http.Request) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen = append(b.seen, r.Method+" "+r.URL.Path)
	return false
}

// forget drops what was seen so far.
func (b *bucket) forget() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seen = nil
}

// gets returns the roots of the packs of which an object with the
// extension ext (".idx", ".data", or "" for either) was downloaded, in the
// order of the requests.
func (b *bucket) gets(ext string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var roots []string
	for _, s := range b.seen {
		if strings.HasPrefix(s, "GET ") && strings.HasSuffix(s, ext) {
			roots = append(roots, path.Base(path.Dir(s)))
		}
	}
	return roots
}

// fixture is one narrenhaus without its command: a server, a cache
// directory, a store, an uploader, a signing key, and the handler on them
// behind a listener.
type fixture struct {
	t      *testing.T
	srv    *testserver.Server
	bucket *bucket
	other  *remote.Remote // another client of the server: asks and writes
	dir    string
	cache  *packcache.Cache
	store  *refs.Store
	up     *upload.Uploader
	key    narinfo.SigningKey
	log    *logBuffer
	ts     *httptest.Server
	http   *http.Client
}

type config struct {
	limit     uint64 // of the cache; 0 means 1 GiB
	priority  int
	serveWait time.Duration
	host      string // the name the cache is told it listens at
	server    testserver.Options
}

func newFixture(t *testing.T, cfg config) *fixture {
	t.Helper()
	f := &fixture{t: t, bucket: &bucket{}, log: &logBuffer{}, dir: t.TempDir()}
	f.srv = testserver.Start(t, cfg.server)
	f.srv.Intercept(f.bucket.serve)

	f.other = f.remote()
	rem := f.remote()

	limit := cfg.limit
	if limit == 0 {
		limit = 1 << 30
	}
	var err error
	f.cache, err = packcache.Open(f.dir, limit, packcache.Origin{Server: "test", Prefix: testPrefix})
	if err != nil {
		t.Fatalf("opening the cache: %v", err)
	}
	t.Cleanup(func() { f.cache.Close() })

	log := slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.store = refs.New(context.Background(), f.cache, rem, refs.Options{Prefix: testPrefix, Log: log})
	t.Cleanup(f.store.Close)
	f.up = upload.New(context.Background(), f.cache, rem, f.store, upload.Options{Log: log})
	t.Cleanup(func() {
		f.up.Close()
		if t.Failed() || os.Getenv("NARRENHAUS_TEST_LOG") != "" {
			t.Logf("the log:\n%s", f.log.String())
		}
	})

	f.key, err = narinfo.LoadOrCreateSigningKey(filepath.Join(t.TempDir(), "signing-key"))
	if err != nil {
		t.Fatalf("making a signing key: %v", err)
	}
	f.ts = httptest.NewServer(New(Config{
		Store:     f.store,
		Uploader:  f.up,
		Key:       f.key,
		Priority:  cfg.priority,
		ServeWait: cfg.serveWait,
		Host:      cfg.host,
		Log:       log,
	}))
	t.Cleanup(f.ts.Close)
	// A redirect is an answer like any other: the handler gives none.
	f.http = &http.Client{
		Transport:     f.ts.Client().Transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
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

// answer is what a request was answered with.
type answer struct {
	status int
	header http.Header
	length int64 // the Content-Length as the client understood it, -1 for none
	body   []byte
}

// do sends a request to the handler and reads its answer. header is pairs
// of a name and a value; Host is what the request calls the cache.
func (f *fixture) do(method, target string, body []byte, header ...string) answer {
	f.t.Helper()
	var in io.Reader
	if body != nil {
		in = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(background(f.t), method, f.ts.URL+target, in)
	if err != nil {
		f.t.Fatalf("%s %s: %v", method, target, err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		if header[i] == "Host" {
			req.Host = header[i+1]
			continue
		}
		req.Header.Set(header[i], header[i+1])
	}
	res, err := f.http.Do(req)
	if err != nil {
		f.t.Fatalf("%s %s: %v", method, target, err)
	}
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	if err != nil {
		f.t.Fatalf("%s %s: reading the answer: %v", method, target, err)
	}
	return answer{status: res.StatusCode, header: res.Header, length: res.ContentLength, body: got}
}

// want fails the test unless the request is answered with status.
func (f *fixture) want(status int, method, target string, body []byte, header ...string) answer {
	f.t.Helper()
	got := f.do(method, target, body, header...)
	if got.status != status {
		f.t.Fatalf("%s %s: status %d (%q), want %d", method, target, got.status, got.body, status)
	}
	return got
}

// wantError fails the test unless the request is answered with status and
// a body Nix shows: plain text, one line, under 1024 bytes.
func (f *fixture) wantError(status int, method, target string, body []byte, header ...string) answer {
	f.t.Helper()
	got := f.want(status, method, target, body, header...)
	if ct := got.header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		f.t.Errorf("%s %s: Content-Type %q of an error, want plain text", method, target, ct)
	}
	text := string(got.body)
	if len(text) < 2 || len(text) >= 1024 || !strings.HasSuffix(text, "\n") || strings.ContainsAny(strings.TrimSuffix(text, "\n"), "\r\n") {
		f.t.Errorf("%s %s: an error body of %d bytes that is no single line under 1024 bytes: %q", method, target, len(text), text)
	}
	return got
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

// largeTree is a store path of 24 MiB: more than a connection on loopback
// takes before its reader has read.
func largeTree(seed byte) node {
	return dir(map[string]node{
		"a": file(noise(seed, 8<<20)),
		"b": file(noise(seed+1, 8<<20)),
		"c": file(noise(seed+2, 8<<20)),
	})
}

// uploadedSig is a signature that came with an upload, by a key nobody
// has: 64 bytes that verify under none.
var uploadedSig = "cache.example-1:" + base64.StdEncoding.EncodeToString(make([]byte, 64))

// storePath is a store path as a test uploads it: its archive, and what
// narrenhaus is to make of it.
type storePath struct {
	hash    string // the hash part of the store path
	name    string // the rest of its base name
	archive []byte
	narHash string // the archive's SHA-256 in Nix's base 32
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
	p.size, p.narHash = size, nix32.Encode(sum[:])
	if p.root, err = nartree.Root(node, sum, size, p.objs.emit); err != nil {
		t.Fatalf("Root: %v", err)
	}
	return p
}

// base is the store path's name under /nix/store.
func (p *storePath) base() string { return p.hash + "-" + p.name }

// narinfo is the text Nix sends for the store path after it uploaded the
// bytes of sent to url; references are base names.
func (p *storePath) narinfo(url, compression string, sent []byte, references ...string) []byte {
	return fmt.Appendf(nil, "StorePath: /nix/store/%s\nURL: %s\nCompression: %s\nFileHash: sha256:%s\nFileSize: %d\n"+
		"NarHash: sha256:%s\nNarSize: %d\nReferences: %s\nDeriver: %s-%s.drv\nSig: %s\n",
		p.base(), url, compression, fileHash(sent), len(sent), p.narHash, p.size, strings.Join(references, " "),
		storeHash("drv "+p.name), p.name, uploadedSig)
}

// plain is the narinfo Nix sends after an upload with ?compression=none.
func (p *storePath) plain(references ...string) []byte {
	return p.narinfo("nar/"+p.narHash+".nar", "none", p.archive, references...)
}

// stored is the text narrenhaus keeps of the narinfo.
func (p *storePath) stored(t *testing.T, references ...string) []byte {
	t.Helper()
	info, err := narinfo.Parse(p.plain(references...))
	if err != nil {
		t.Fatalf("the narinfo of the test: %v", err)
	}
	return info.Stored()
}

// fileHash is the name Nix gives the upload of a file: its SHA-256.
func fileHash(body []byte) string {
	sum := sha256.Sum256(body)
	return nix32.Encode(sum[:])
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

func background(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

// upload sends p through the handler as Nix does with ?compression=none,
// its archive and then its narinfo, and waits for the pushes to end.
func (f *fixture) upload(p *storePath, references ...string) {
	f.t.Helper()
	f.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	f.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.plain(references...))
	f.settled()
}

// settled waits until the uploader has nothing to do.
func (f *fixture) settled() {
	f.t.Helper()
	eventually(f.t, "the uploader is idle", f.up.Idle)
}

// push writes the reference name on the server from another client, so
// that nothing of it is cached here.
func (f *fixture) push(name string, root key.Key, objs client.ObjectSource) {
	f.t.Helper()
	_, err := f.other.Push(background(f.t), objs, name, root, client.PushOptions{MinDedup: 2, TempDir: f.t.TempDir()})
	if err != nil {
		f.t.Fatalf("pushing %q: %v", name, err)
	}
}

// pushPath writes both references of p on the server from another client.
func (f *fixture) pushPath(p *storePath, references ...string) {
	f.t.Helper()
	f.push(f.store.ContentName(p.narHash), p.root, p.objs)
	objs := newObjects()
	root, err := nartree.File(bytes.NewReader(p.stored(f.t, references...)), objs.emit)
	if err != nil {
		f.t.Fatalf("File: %v", err)
	}
	f.push(f.store.NarinfoName(p.hash), root, objs)
}

// onServer reports whether the server has the reference name.
func (f *fixture) onServer(name string) bool {
	f.t.Helper()
	_, _, err := f.other.Lookup(background(f.t), name)
	switch {
	case err == nil:
		return true
	case errors.Is(err, client.ErrNotFound):
		return false
	}
	f.t.Fatalf("looking up %q: %v", name, err)
	return false
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

// publicKey takes a key as Nix's trusted-public-keys has it apart.
func publicKey(t *testing.T, k narinfo.SigningKey) (name string, public ed25519.PublicKey) {
	t.Helper()
	name, text, ok := strings.Cut(k.Public(), ":")
	if !ok {
		t.Fatalf("the public key %q has no name", k.Public())
	}
	raw, err := base64.StdEncoding.DecodeString(text)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		t.Fatalf("the public key %q is no Ed25519 key: %v", k.Public(), err)
	}
	return name, raw
}

func TestCacheInfo(t *testing.T) {
	f := newFixture(t, config{priority: 50})
	const want = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 50\n"

	got := f.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil)
	if string(got.body) != want {
		t.Errorf("the body is %q, want %q", got.body, want)
	}
	if ct := got.header.Get("Content-Type"); ct != "text/x-nix-cache-info" {
		t.Errorf("Content-Type %q, want text/x-nix-cache-info", ct)
	}
	if got.length != int64(len(want)) {
		t.Errorf("Content-Length %d, want %d", got.length, len(want))
	}

	head := f.want(http.StatusOK, http.MethodHead, "/nix-cache-info", nil)
	if len(head.body) != 0 || head.length != int64(len(want)) {
		t.Errorf("HEAD: a body of %d bytes and Content-Length %d, want none and %d", len(head.body), head.length, len(want))
	}
	if ct := head.header.Get("Content-Type"); ct != "text/x-nix-cache-info" {
		t.Errorf("HEAD: Content-Type %q, want text/x-nix-cache-info", ct)
	}
}

func TestNarinfoIsSignedAtServeTime(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	dep := storeHash("glibc-2.40") + "-glibc-2.40"
	f.upload(p, dep, p.base())

	got := f.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	if ct := got.header.Get("Content-Type"); ct != "text/x-nix-narinfo" {
		t.Errorf("Content-Type %q, want text/x-nix-narinfo", ct)
	}
	if got.length != int64(len(got.body)) {
		t.Errorf("Content-Length %d for a body of %d bytes", got.length, len(got.body))
	}
	info, err := narinfo.Parse(got.body)
	if err != nil {
		t.Fatalf("the narinfo served does not parse: %v\n%s", err, got.body)
	}
	if info.URL != "nar/"+p.narHash+".nar" || info.Compression != "none" {
		t.Errorf("the narinfo names the NAR as %q with compression %q", info.URL, info.Compression)
	}

	// The signature that came with the upload, and one by this key.
	name, public := publicKey(t, f.key)
	var ours string
	for _, sig := range info.Sigs {
		if strings.HasPrefix(sig, name+":") {
			ours = sig
		}
	}
	if len(info.Sigs) != 2 || !slices.Contains(info.Sigs, uploadedSig) || ours == "" {
		t.Fatalf("the signatures are %q, want the uploaded one and one by %s", info.Sigs, name)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(ours, name+":"))
	if err != nil {
		t.Fatalf("the signature %q: %v", ours, err)
	}
	// What Nix signs, written out here and not taken from the package:
	// the references as full paths, sorted.
	refs := []string{"/nix/store/" + dep, "/nix/store/" + p.base()}
	slices.Sort(refs)
	fingerprint := fmt.Sprintf("1;/nix/store/%s;sha256:%s;%d;%s", p.base(), p.narHash, p.size, strings.Join(refs, ","))
	if !ed25519.Verify(public, []byte(fingerprint), raw) {
		t.Errorf("the signature of %s does not verify over %q", name, fingerprint)
	}

	// Without that line it is the text that was stored, and the stored
	// text has no signature of this key: it is made when serving.
	stored := p.stored(t, dep, p.base())
	if without := strings.Replace(string(got.body), "Sig: "+ours+"\n", "", 1); without != string(stored) {
		t.Errorf("the narinfo served is\n%s\nwant the stored text and one more Sig line:\n%s", got.body, stored)
	}
	kept, err := f.store.Narinfo(background(t), p.hash)
	if err != nil {
		t.Fatalf("Narinfo: %v", err)
	}
	if !slices.Equal(kept.Sigs, []string{uploadedSig}) {
		t.Errorf("the stored narinfo has the signatures %q, want only the uploaded one", kept.Sigs)
	}

	// Ed25519 signs the same text the same way each time.
	again := f.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	if !bytes.Equal(again.body, got.body) {
		t.Error("a second answer differs from the first")
	}
}

func TestNarinfoHead(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.pushPath(p)
	f.bucket.forget()

	got := f.want(http.StatusOK, http.MethodHead, "/"+p.hash+".narinfo", nil)
	if len(got.body) != 0 {
		t.Errorf("HEAD has a body of %d bytes", len(got.body))
	}
	if gets := f.bucket.gets(""); len(gets) != 0 {
		t.Errorf("HEAD downloaded %v, want nothing", gets)
	}
	// GET says the same and then has the text.
	f.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	f.want(http.StatusOK, http.MethodHead, "/"+p.hash+".narinfo", nil)

	missing := f.want(http.StatusNotFound, http.MethodHead, "/"+storeHash("nothing")+".narinfo", nil)
	if len(missing.body) != 0 {
		t.Errorf("HEAD of a path the server lacks has a body of %d bytes", len(missing.body))
	}
	f.wantError(http.StatusNotFound, http.MethodGet, "/"+storeHash("nothing")+".narinfo", nil)
}

func TestNarinfoPutWithContentEncoding(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)

	// Nix 2.32 and later can send a narinfo compressed; narrenhaus reads
	// it as text only.
	got := f.wantError(http.StatusUnsupportedMediaType, http.MethodPut, "/"+p.hash+".narinfo", p.plain(), "Content-Encoding", "zstd")
	if !strings.Contains(string(got.body), "Content-Encoding") {
		t.Errorf("the body %q does not name the header", got.body)
	}
	f.settled()
	if f.onServer(f.store.NarinfoName(p.hash)) {
		t.Error("the server has the narinfo of a refused request")
	}
	// The same text without the header is taken.
	f.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.plain())
	if !f.onServer(f.store.NarinfoName(p.hash)) {
		t.Error("the server lacks the narinfo of an accepted request")
	}
}

func TestUnknownPaths(t *testing.T) {
	f := newFixture(t, config{})
	hash := storeHash("x")
	narHash := fileHash([]byte("x"))
	for _, c := range []struct {
		method, target string
		status         int
	}{
		{http.MethodGet, "/log/x", 404},
		{http.MethodGet, "/abc.ls", 404},
		{http.MethodGet, "/" + hash + ".ls", 404},
		{http.MethodGet, "/realisations/x.doi", 404},
		{http.MethodGet, "/debuginfo/x", 404},
		{http.MethodHead, "/debuginfo/x", 404},
		{http.MethodGet, "/", 404},
		{http.MethodGet, "/nar", 404},
		{http.MethodGet, "/nar/", 404},
		{http.MethodGet, "/nix-cache-info/", 404},
		{http.MethodGet, "//nix-cache-info", 404},
		{http.MethodGet, "/nar/../nix-cache-info", 404},
		{http.MethodGet, "/x/nix-cache-info", 404},
		{http.MethodGet, "/" + hash + ".narinfo/x", 404},
		{http.MethodGet, "/" + hash[1:] + ".narinfo", 404},
		{http.MethodGet, "/" + hash + "0.narinfo", 404},
		{http.MethodGet, "/" + strings.ToUpper(hash) + ".narinfo", 404},
		{http.MethodGet, "/e" + hash[1:] + ".narinfo", 404}, // no letter of Nix's base 32
		{http.MethodGet, "/" + hash + ".NARINFO", 404},
		{http.MethodGet, "/" + hash, 404},
		{http.MethodGet, "/nar/" + hash + ".narinfo", 404},
		{http.MethodGet, "/nar/" + narHash + ".nar/x", 404},
		{http.MethodGet, "/nar/" + narHash, 404},
		{http.MethodPost, "/nix-cache-info", 404},
		{http.MethodPost, "/" + hash + ".narinfo", 404},
		{http.MethodDelete, "/" + hash + ".narinfo", 404},
		{http.MethodDelete, "/nar/" + narHash + ".nar", 404},

		{http.MethodPut, "/abc.ls", 405},
		{http.MethodPut, "/" + hash + ".ls", 405},
		{http.MethodPut, "/nix-cache-info", 405},
		{http.MethodPut, "/", 405},
		{http.MethodPut, "/log/x", 405},
		{http.MethodPut, "/debuginfo/x", 405},
		{http.MethodPut, "/realisations/x.doi", 405},
		{http.MethodPut, "/" + hash[1:] + ".narinfo", 405},
		{http.MethodPut, "//" + hash + ".narinfo", 405},
		{http.MethodPut, "/nar/abc.nar", 405},
		{http.MethodPut, "/nar/" + narHash, 405},
		{http.MethodPut, "/nar/" + narHash + ".nar.", 405},
		{http.MethodPut, "/nar/" + narHash + ".nar.x.z", 405},
		{http.MethodPut, "/nar/" + narHash + ".nar/x", 405},
		{http.MethodPut, "/nar/2" + narHash[1:] + ".nar", 405}, // more than 256 bits
	} {
		var body []byte
		if c.method == http.MethodPut || c.method == http.MethodPost {
			body = []byte("x")
		}
		if c.method == http.MethodHead {
			f.want(c.status, c.method, c.target, body)
			continue
		}
		f.wantError(c.status, c.method, c.target, body)
	}
	if names := f.staged(); len(names) != 0 {
		t.Errorf("staging holds %v after requests that were all refused", names)
	}
}

func TestNARGet(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.pushPath(p)

	got := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if got.length != int64(p.size) || got.header.Get("Content-Length") != strconv.FormatUint(p.size, 10) {
		t.Errorf("Content-Length %q, want %d", got.header.Get("Content-Length"), p.size)
	}
	if ct := got.header.Get("Content-Type"); ct != "application/x-nix-nar" {
		t.Errorf("Content-Type %q, want application/x-nix-nar", ct)
	}
	if ar := got.header.Get("Accept-Ranges"); ar != "" {
		t.Errorf("Accept-Ranges %q, want none: a part of a NAR is never served", ar)
	}
	if hash := fileHash(got.body); hash != p.narHash {
		t.Errorf("the body hashes to %s, the name says %s", hash, p.narHash)
	}
	if !bytes.Equal(got.body, p.archive) {
		t.Error("the body is not the archive")
	}
	// A request for a part gets the whole.
	part := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil, "Range", "bytes=100-")
	if !bytes.Equal(part.body, p.archive) {
		t.Errorf("a request with Range got %d bytes, want the whole archive", len(part.body))
	}
}

func TestNARHeadHasNoContentLength(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.pushPath(p)
	f.bucket.forget()

	for _, c := range []struct {
		hash   string
		status int
	}{
		{p.narHash, http.StatusOK},
		{fileHash([]byte("nothing")), http.StatusNotFound},
	} {
		got := f.want(c.status, http.MethodHead, "/nar/"+c.hash+".nar", nil)
		if got.length != -1 || len(got.header.Values("Content-Length")) != 0 {
			t.Errorf("HEAD answered %d with Content-Length %q, want none", c.status, got.header.Values("Content-Length"))
		}
		if len(got.body) != 0 {
			t.Errorf("HEAD answered %d with a body of %d bytes", c.status, len(got.body))
		}
	}
	// It is the question whether the NAR must be sent, and downloads
	// nothing.
	if gets := f.bucket.gets(""); len(gets) != 0 {
		t.Errorf("HEAD downloaded %v, want nothing", gets)
	}

	f.srv.Stop()
	got := f.want(http.StatusBadGateway, http.MethodHead, "/nar/"+p.narHash+".nar", nil)
	if got.length != -1 || len(got.body) != 0 {
		t.Errorf("HEAD with the server down has Content-Length %d and a body of %d bytes", got.length, len(got.body))
	}
}

func TestNARGetNotFound(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.pushPath(p)
	f.bucket.forget()

	for _, hash := range []string{
		fileHash([]byte("nothing")),      // a hash the server has nothing for
		p.narHash[1:],                    // 51 characters
		p.narHash + "0",                  // 53
		"e" + p.narHash[1:],              // no letter of Nix's base 32
		"z" + p.narHash[1:],              // more than 256 bits
		strings.ToUpper(p.narHash),       // Nix writes lowercase
		p.hash,                           // a store path's hash
		p.narHash[:26] + "/" + p.narHash, // a path below
	} {
		got := f.wantError(http.StatusNotFound, http.MethodGet, "/nar/"+hash+".nar", nil)
		if cl := got.header.Get("Content-Length"); cl == strconv.FormatUint(p.size, 10) {
			t.Errorf("a 404 with the Content-Length of a NAR")
		}
	}
	if gets := f.bucket.gets(""); len(gets) != 0 {
		t.Errorf("requests that were all answered 404 downloaded %v", gets)
	}
}

func TestCompressedNARURLIsNeverServed(t *testing.T) {
	f := newFixture(t, config{})
	p := pathOf(t, "hello-2.12", smallTree(1))
	sent := xzOf(t, p.archive)
	url := "/nar/" + fileHash(sent) + ".nar.xz"

	// Nix asks before it sends, sends, and then names that URL in the
	// narinfo.
	f.want(http.StatusNotFound, http.MethodHead, url, nil)
	f.want(http.StatusOK, http.MethodPut, url, sent, "Content-Type", "application/x-nix-nar")
	f.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo(url[1:], "xz", sent))
	f.settled()

	for _, target := range []string{
		url,
		"/nar/" + fileHash(sent) + ".nar",    // the compressed file's hash names no NAR
		"/nar/" + p.narHash + ".nar.xz",      // no compressed form of the NAR either
		"/nar/" + p.narHash + ".nar.zst",     //
		"/nar/" + p.narHash + ".nar.bz2",     //
		"/nar/" + p.narHash + ".nar.narinfo", //
	} {
		f.wantError(http.StatusNotFound, http.MethodGet, target, nil)
		f.want(http.StatusNotFound, http.MethodHead, target, nil)
	}

	// The NAR is served under its own hash, and the narinfo says so.
	got := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if !bytes.Equal(got.body, p.archive) {
		t.Error("the NAR served is not the archive that was sent compressed")
	}
	f.want(http.StatusOK, http.MethodHead, "/nar/"+p.narHash+".nar", nil)
	info, err := narinfo.Parse(f.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil).body)
	if err != nil {
		t.Fatalf("the narinfo served: %v", err)
	}
	if info.URL != "nar/"+p.narHash+".nar" || info.Compression != "none" {
		t.Errorf("the narinfo names the NAR as %q with compression %q", info.URL, info.Compression)
	}
}

func TestUploadErrorsCarryTheirStatus(t *testing.T) {
	t.Run("refused requests", func(t *testing.T) {
		f := newFixture(t, config{})
		p := pathOf(t, "hello-2.12", smallTree(1))
		other := pathOf(t, "other-1.0", smallTree(2))

		// A narinfo whose NAR never came.
		got := f.wantError(http.StatusBadRequest, http.MethodPut, "/"+p.hash+".narinfo", p.plain())
		if !strings.Contains(string(got.body), p.narHash) {
			t.Errorf("the body %q does not name the NAR that is missing", got.body)
		}
		// A narinfo sent under another path's hash, one that is no narinfo,
		// and one that is too long to be one.
		f.wantError(http.StatusBadRequest, http.MethodPut, "/"+other.hash+".narinfo", p.plain())
		f.wantError(http.StatusBadRequest, http.MethodPut, "/"+p.hash+".narinfo", []byte("StorePath /nix/store\n"))
		long := append(p.plain(), bytes.Repeat([]byte("X-Padding: "+strings.Repeat("x", 100)+"\n"), 10000)...)
		if len(long) <= narinfo.MaxStored {
			t.Fatalf("the long narinfo has %d bytes", len(long))
		}
		f.wantError(http.StatusBadRequest, http.MethodPut, "/"+p.hash+".narinfo", long)

		// A body that is no NAR in any compression; a NAR that ends early;
		// a NAR under the hash of another.
		f.wantError(http.StatusUnsupportedMediaType, http.MethodPut, "/nar/"+p.narHash+".nar", []byte("<html>not a NAR</html>"))
		f.wantError(http.StatusUnsupportedMediaType, http.MethodPut, "/nar/"+p.narHash+".nar", []byte{})
		f.wantError(http.StatusBadRequest, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive[:len(p.archive)-8])
		f.wantError(http.StatusBadRequest, http.MethodPut, "/nar/"+other.narHash+".nar", p.archive)
		cut := xzOf(t, p.archive)
		f.wantError(http.StatusBadRequest, http.MethodPut, "/nar/"+fileHash(cut)+".nar.xz", cut[:len(cut)/2])

		f.settled()
		if names := f.staged(); len(names) != 0 {
			t.Errorf("staging holds %v after requests that were all refused", names)
		}
		if f.onServer(f.store.ContentName(p.narHash)) || f.onServer(f.store.NarinfoName(p.hash)) {
			t.Error("the server has a reference of a refused request")
		}
	})

	t.Run("a pack the server refuses", func(t *testing.T) {
		f := newFixture(t, config{server: testserver.Options{MaxPackBytes: 4 << 10}})
		p := pathOf(t, "hello-2.12", smallTree(1))
		// The NAR is answered before its push, the narinfo after it.
		f.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
		got := f.wantError(http.StatusUnprocessableEntity, http.MethodPut, "/"+p.hash+".narinfo", p.plain())
		if !strings.Contains(string(got.body), "limit") {
			t.Errorf("the body %q does not carry the server's reason", got.body)
		}
	})

	t.Run("the server is down", func(t *testing.T) {
		f := newFixture(t, config{})
		p := pathOf(t, "hello-2.12", smallTree(1))
		f.srv.Stop()
		f.wantError(http.StatusBadGateway, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
		f.wantError(http.StatusBadGateway, http.MethodPut, "/"+p.hash+".narinfo", p.plain())
		f.want(http.StatusBadGateway, http.MethodHead, "/nar/"+p.narHash+".nar", nil)
		if names := f.staged(); len(names) != 0 {
			t.Errorf("staging holds %v with the server down", names)
		}
	})

	t.Run("an error that is none of the uploader's", func(t *testing.T) {
		log := &logBuffer{}
		s := &server{log: slog.New(slog.NewTextHandler(log, nil))}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/nar/x.nar", nil)
		s.uploadFailed(rec, req, fmt.Errorf("staging: %w", errors.New("an inode went missing")))
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status %d, want 500", rec.Code)
		}
		if body := rec.Body.String(); strings.Contains(body, "inode") || len(body) >= 1024 || !strings.HasSuffix(body, "\n") {
			t.Errorf("the body is %q, want a line that does not pass the error on", body)
		}
		if !log.has("level=ERROR", "an inode went missing", "/nar/x.nar") {
			t.Errorf("the error is not in the log:\n%s", log)
		}

		// An error of the uploader inside another is still answered as
		// the uploader says.
		rec = httptest.NewRecorder()
		s.uploadFailed(rec, req, fmt.Errorf("wrapped: %w", &upload.Error{Status: http.StatusInsufficientStorage, Msg: "the disk is full"}))
		if rec.Code != http.StatusInsufficientStorage || rec.Body.String() != "the disk is full\n" {
			t.Errorf("status %d and body %q, want 507 and the uploader's message", rec.Code, rec.Body.String())
		}
	})
}

// slowBody hands out size bytes of zeros and counts them.
type slowBody struct {
	size int64
	read atomic.Int64
}

func (b *slowBody) Read(p []byte) (int, error) {
	left := b.size - b.read.Load()
	if left <= 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), left))
	clear(p[:n])
	b.read.Add(int64(n))
	return n, nil
}

func TestNARPutIsNotReadWhenTheServerIsDown(t *testing.T) {
	// A copy fails at its first NAR and says why: the answer comes
	// although the body was not read, and without the body being sent in
	// full first.
	f := newFixture(t, config{})
	f.srv.Stop()

	body := &slowBody{size: 256 << 20}
	req, err := http.NewRequestWithContext(background(t), http.MethodPut, f.ts.URL+"/nar/"+fileHash([]byte("x"))+".nar", body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = body.size
	res, err := f.http.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer res.Body.Close()
	text, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d (%q), want 502", res.StatusCode, text)
	}
	if len(text) < 2 || len(text) >= 1024 {
		t.Errorf("an error body of %d bytes: %q", len(text), text)
	}
	if sent := body.read.Load(); sent >= body.size/2 {
		t.Errorf("%d of %d bytes were taken from the body before the answer", sent, body.size)
	}
	if names := f.staged(); len(names) != 0 {
		t.Errorf("staging holds %v with the server down", names)
	}
}

func TestAbandonedNARDownloadReleasesItsPacks(t *testing.T) {
	// A cache whose limit is below any pack keeps a pack only while a
	// request reads it.
	f := newFixture(t, config{limit: 1})
	p := pathOf(t, "big-1.0", largeTree(1))
	f.pushPath(p)
	if size := f.cache.Size(); size != 0 {
		t.Fatalf("the cache holds %d bytes before the download", size)
	}

	req, err := http.NewRequestWithContext(background(t), http.MethodGet, f.ts.URL+"/nar/"+p.narHash+".nar", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.http.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if res.StatusCode != http.StatusOK || res.ContentLength != int64(p.size) {
		res.Body.Close()
		t.Fatalf("status %d with Content-Length %d, want 200 and %d", res.StatusCode, res.ContentLength, p.size)
	}
	start := make([]byte, 1<<10)
	if _, err := io.ReadFull(res.Body, start); err != nil {
		res.Body.Close()
		t.Fatalf("reading the first KiB: %v", err)
	}
	if !bytes.Equal(start, p.archive[:len(start)]) {
		t.Error("the first KiB is not the archive's")
	}
	// The handler is in the middle of the NAR and holds its pack.
	held := f.cache.Size()
	if held < p.size {
		t.Errorf("the cache holds %d bytes while a NAR of %d is being served", held, p.size)
	}

	// The client goes away.
	res.Body.Close()
	eventually(t, "the pack of the abandoned download is evicted", func() bool { return f.cache.Size() < held })
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes after the client left, the limit is 1", size)
	}
	eventually(t, "the log says the download was abandoned", func() bool {
		return f.log.has("a NAR was not sent to its end", p.narHash)
	})

	// Nothing is left of it but the reference: the next client gets it all.
	got := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if !bytes.Equal(got.body, p.archive) {
		t.Error("the NAR served after an abandoned download is not the archive")
	}
	if size := f.cache.Size(); size != 0 {
		t.Errorf("the cache holds %d bytes after a download that ended, the limit is 1", size)
	}
}

func TestNARThatCannotBeWrittenCutsTheConnection(t *testing.T) {
	// After the header there is one way to say that what follows is not
	// the NAR: no end. A pack that loses its data under a download is
	// such a case.
	f := newFixture(t, config{})
	p := pathOf(t, "big-1.0", largeTree(1))
	f.pushPath(p)

	req, err := http.NewRequestWithContext(background(t), http.MethodGet, f.ts.URL+"/nar/"+p.narHash+".nar", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.http.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || res.ContentLength != int64(p.size) {
		t.Fatalf("status %d with Content-Length %d, want 200 and %d", res.StatusCode, res.ContentLength, p.size)
	}
	start := make([]byte, 1<<10)
	if _, err := io.ReadFull(res.Body, start); err != nil {
		t.Fatalf("reading the first KiB: %v", err)
	}
	// The handler waits for the client with most of the NAR unread.
	if err := os.Truncate(filepath.Join(f.dir, "packs", p.root.String()+".data"), 0); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(res.Body)
	if err == nil {
		t.Fatalf("the answer ended well after %d bytes of %d", len(start)+len(rest), p.size)
	}
	if got := uint64(len(start) + len(rest)); got >= p.size {
		t.Errorf("%d bytes came, the NAR has %d", got, p.size)
	}
	if !bytes.Equal(rest, p.archive[len(start):len(start)+len(rest)]) {
		t.Error("what came before the cut is not the archive's beginning")
	}
	eventually(t, "the log says the NAR could not be written", func() bool {
		return f.log.has("level=ERROR", "a NAR was not sent to its end", p.narHash)
	})

	// The pack is fetched again for the next request.
	got := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if !bytes.Equal(got.body, p.archive) {
		t.Error("the NAR served after the cut is not the archive")
	}
}

func TestServeWaitAnswers404(t *testing.T) {
	f := newFixture(t, config{serveWait: 100 * time.Millisecond})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.pushPath(p)
	// The connection is up before the clock runs.
	f.want(http.StatusNotFound, http.MethodHead, "/"+storeHash("nothing")+".narinfo", nil)
	f.bucket.forget()
	f.srv.SlowBucket(time.Second)

	// The pack cannot be here within the wait: Nix is told in time that
	// there is nothing, and not kept waiting.
	began := time.Now()
	f.wantError(http.StatusNotFound, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if took := time.Since(began); took < 100*time.Millisecond || took > 900*time.Millisecond {
		t.Errorf("the 404 came after %s, want it after the wait of 100ms and before the bucket's first byte", took)
	}

	// The work went on without the request: once it is done, and that is
	// a second or two later, the next request is served at once.
	name := f.store.ContentName(p.narHash)
	eventually(t, "the reference is local and checked", func() bool {
		rec, ok := f.cache.Record(name)
		return ok && rec.Checked
	})
	if took := time.Since(began); took < time.Second {
		t.Errorf("the reference was local after %s although the bucket waits a second", took)
	}
	began = time.Now()
	got := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if !bytes.Equal(got.body, p.archive) {
		t.Error("the NAR served is not the archive")
	}
	if took := time.Since(began); took > 900*time.Millisecond {
		t.Errorf("the second request took %s: it did not find the work done", took)
	}
	if gets := f.bucket.gets(".data"); !slices.Equal(gets, []string{p.root.String()}) {
		t.Errorf("the data downloads were of %v, want one of %s", gets, p.root)
	}

	// A narinfo waits in the same way.
	f.wantError(http.StatusNotFound, http.MethodGet, "/"+p.hash+".narinfo", nil)
	eventually(t, "the narinfo is local", func() bool {
		_, ok := f.cache.Record(f.store.NarinfoName(p.hash))
		return ok
	})
	f.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
}

func TestNamed(t *testing.T) {
	s := &server{host: "build.example"}
	for _, host := range []string{
		"127.0.0.1:8484", "127.0.0.1", "127.0.0.1:", "127.8.9.10:1",
		"[::1]:8484", "[::1]", "::1",
		"192.168.1.20:8484", "10.0.0.7", "[fe80::1%en0]:8484", "[2001:db8::1]:80", "0.0.0.0:8484", "[::]:8484",
		"localhost:8484", "localhost", "LocalHost:8484",
		"build.example:8484", "build.example", "BUILD.example:1",
	} {
		if !s.named(host) {
			t.Errorf("a request with the Host %q is not taken for Nix's", host)
		}
	}
	for _, host := range []string{
		"", ":8484", "[]:8484",
		"attacker.example:8484", "attacker.example", "[attacker.example]:8484",
		"localhost.attacker.example:8484", "127.0.0.1.attacker.example", "build.example.attacker.example:8484",
		"attacker.example:8484:127.0.0.1", "127.0.0.1 attacker.example",
		"localhost.", "sub.localhost:8484", "other.example:8484", "build:8484",
		"127.1:8484", "2130706433:8484", "0x7f.1",
	} {
		if s.named(host) {
			t.Errorf("a request with the Host %q is taken for Nix's", host)
		}
	}
	// A cache that was given an address and no name answers to no name
	// but localhost.
	if s := (&server{}); s.named("build.example:8484") || s.named("") || !s.named("localhost:8484") {
		t.Error("a cache without a name of its own does not answer to localhost alone")
	}
	if s := (&server{host: "0.0.0.0"}); s.named("build.example:8484") || !s.named("192.168.1.20:8484") {
		t.Error("a cache that listens at every address does not answer to addresses alone")
	}
}

// A page in a browser reaches a cache on loopback under a name of its own
// that it lets resolve to 127.0.0.1. Its requests carry that name as their
// Host, and they are neither answered nor taken.
func TestRequestsOfAWebPageAreRefused(t *testing.T) {
	f := newFixture(t, config{host: "build.example"})
	p := pathOf(t, "hello-2.12", smallTree(1))
	f.upload(p)
	planted := pathOf(t, "evil-1.0", smallTree(2))
	const page = "attacker.example:8484"

	// The uploads, as a page sends them: without a preflight, the name
	// being the page's own.
	for _, c := range []struct {
		target string
		body   []byte
	}{
		{"/nar/" + planted.narHash + ".nar", planted.archive},
		{"/" + planted.hash + ".narinfo", planted.plain()},
		{"/nar/" + p.narHash + ".nar", p.archive},
		{"/nix-cache-info", []byte("x")},
	} {
		got := f.wantError(http.StatusForbidden, http.MethodPut, c.target, c.body, "Host", page, "Origin", "http://"+page)
		if !strings.Contains(string(got.body), "attacker.example") {
			t.Errorf("PUT %s: the body %q does not name the host that was refused", c.target, got.body)
		}
	}
	// What it could read: nothing is there for it, and nothing is fetched.
	f.bucket.forget()
	for _, target := range []string{
		"/nix-cache-info",
		"/" + p.hash + ".narinfo",
		"/nar/" + p.narHash + ".nar",
		"/",
	} {
		f.wantError(http.StatusNotFound, http.MethodGet, target, nil, "Host", page)
		f.want(http.StatusNotFound, http.MethodHead, target, nil, "Host", page)
	}
	f.wantError(http.StatusNotFound, http.MethodOptions, "/nar/"+planted.narHash+".nar", nil,
		"Host", page, "Origin", "http://"+page, "Access-Control-Request-Method", "PUT")
	f.settled()
	if names := f.staged(); len(names) != 0 {
		t.Errorf("staging holds %v after uploads that were all refused", names)
	}
	if f.onServer(f.store.ContentName(planted.narHash)) || f.onServer(f.store.NarinfoName(planted.hash)) {
		t.Error("the server has a reference a web page sent")
	}
	if !f.log.has("level=WARN", "attacker.example", "/nar/"+planted.narHash+".nar") {
		t.Errorf("the log does not say that a request was refused:\n%s", f.log)
	}

	// Under an address, under localhost and under the name the cache was
	// given, Nix is answered and its uploads are taken.
	for _, host := range []string{"127.0.0.1:8484", "[::1]:8484", "192.168.1.20:8484", "localhost:8484", "build.example:8484", "build.example"} {
		f.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil, "Host", host)
		f.want(http.StatusOK, http.MethodHead, "/"+p.hash+".narinfo", nil, "Host", host)
		got := f.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil, "Host", host)
		if !bytes.Equal(got.body, p.archive) {
			t.Errorf("under the Host %s the NAR is not the archive", host)
		}
	}
	lan := pathOf(t, "lan-1.0", smallTree(3))
	f.want(http.StatusOK, http.MethodPut, "/nar/"+lan.narHash+".nar", lan.archive, "Host", "192.168.1.20:8484")
	f.want(http.StatusOK, http.MethodPut, "/"+lan.hash+".narinfo", lan.plain(), "Host", "build.example:8484")

	// A page of another site that names the cache by its address is
	// cross-origin: the browser says where the request comes from, which
	// Nix never does.
	other := pathOf(t, "other-1.0", smallTree(4))
	f.wantError(http.StatusForbidden, http.MethodPut, "/nar/"+other.narHash+".nar", other.archive, "Origin", "http://attacker.example")
	f.wantError(http.StatusForbidden, http.MethodPut, "/"+other.hash+".narinfo", other.plain(), "Origin", "null")
	f.bucket.forget()
	for _, site := range []string{"cross-site", "same-site"} {
		f.wantError(http.StatusNotFound, http.MethodGet, "/nar/"+p.narHash+".nar", nil, "Sec-Fetch-Site", site)
		f.wantError(http.StatusNotFound, http.MethodGet, "/"+p.hash+".narinfo", nil, "Sec-Fetch-Site", site)
		f.wantError(http.StatusForbidden, http.MethodPut, "/nar/"+other.narHash+".nar", other.archive, "Sec-Fetch-Site", site)
	}
	// Someone who types the address into a browser is no page, and reading
	// is not refused for an Origin: the browser keeps the answer from a
	// page of another origin by itself.
	f.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil, "Sec-Fetch-Site", "none")
	f.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil, "Origin", "http://attacker.example")
	f.settled()
	if names := f.staged(); len(names) != 0 {
		t.Errorf("staging holds %v after uploads that were all refused", names)
	}
	if f.onServer(f.store.ContentName(other.narHash)) {
		t.Error("the server has a reference a web page sent")
	}
}

// A refused upload is answered before its body is read.
func TestRefusedUploadIsNotRead(t *testing.T) {
	f := newFixture(t, config{})
	body := &slowBody{size: 256 << 20}
	req, err := http.NewRequestWithContext(background(t), http.MethodPut, f.ts.URL+"/nar/"+fileHash([]byte("x"))+".nar", body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = body.size
	req.Host = "attacker.example:8484"
	res, err := f.http.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer res.Body.Close()
	text, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d (%q), want 403", res.StatusCode, text)
	}
	if sent := body.read.Load(); sent >= body.size/2 {
		t.Errorf("%d of %d bytes were taken from the body before the answer", sent, body.size)
	}
	if names := f.staged(); len(names) != 0 {
		t.Errorf("staging holds %v after an upload that was refused", names)
	}
}
