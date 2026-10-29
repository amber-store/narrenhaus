// Package e2e runs two whole narrenhaus instances, each as the command
// puts one together, against one jaccard-store server in this process, and
// talks to them over HTTP as Nix does.
package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/ulikunitz/xz"

	"github.com/amber-store/narrenhaus/httpd"
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

// testPrefix is the one prefix both instances use.
const testPrefix = "acme/"

// repeatAfter is how long an instance waits for the answer to a lookup on
// a connection that was up before it repeats the lookup on a fresh one.
// The command's is three seconds; the test of the silent connection waits
// for it.
const repeatAfter = 300 * time.Millisecond

// logBuffer keeps what an instance logs. A test that fails prints it, and
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

// bucket sees every request of a client to the server's bucket, whichever
// instance it comes from.
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

// harness is a jaccard-store server and two narrenhaus instances, a and b,
// that know nothing of each other but the server and the prefix.
type harness struct {
	t      *testing.T
	srv    *testserver.Server
	bucket *bucket
	a, b   *instance
}

// instance is one narrenhaus: what the command builds from its options,
// with the handler behind a listener on loopback.
type instance struct {
	t     *testing.T
	name  string
	dir   string // the cache directory
	cache *packcache.Cache
	store *refs.Store
	up    *upload.Uploader
	key   narinfo.SigningKey
	log   *logBuffer
	url   string
	http  *http.Client
}

// start brings up the server and both instances; the test's end takes them
// down.
func start(t *testing.T, opts testserver.Options) *harness {
	t.Helper()
	h := &harness{t: t, bucket: &bucket{}}
	h.srv = testserver.Start(t, opts)
	h.srv.Intercept(h.bucket.serve)
	h.a = h.instance("a")
	h.b = h.instance("b")
	return h
}

// instance starts a narrenhaus with a cache directory, a signing key and a
// connection of its own, and the time limits the command runs with, but
// for repeatAfter.
func (h *harness) instance(name string) *instance {
	h.t.Helper()
	t := h.t
	i := &instance{t: t, name: name, dir: t.TempDir(), log: &logBuffer{}}
	log := slog.New(slog.NewTextHandler(i.log, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var err error
	i.key, err = narinfo.LoadOrCreateSigningKey(filepath.Join(t.TempDir(), "signing-key"))
	if err != nil {
		t.Fatalf("instance %s: %v", name, err)
	}
	i.cache, err = packcache.Open(i.dir, 1<<30, packcache.Origin{Server: "test", Prefix: testPrefix})
	if err != nil {
		t.Fatalf("instance %s: %v", name, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rem := remote.New(remote.Config{Dial: h.srv.Dial(), RepeatAfter: repeatAfter, Log: log})
	i.store = refs.New(ctx, i.cache, rem, refs.Options{Prefix: testPrefix, Log: log})
	i.up = upload.New(ctx, i.cache, rem, i.store, upload.Options{Log: log})
	ts := httptest.NewServer(httpd.New(httpd.Config{
		Store:    i.store,
		Uploader: i.up,
		Key:      i.key,
		Priority: 30,
		Log:      log,
	}))
	i.url = ts.URL
	// Nix follows redirects; an instance gives none, and the tests see it
	// if one does.
	i.http = &http.Client{
		Transport:     ts.Client().Transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	// The command's way down: no more requests, then the work, then what
	// the work stood on.
	t.Cleanup(func() {
		ts.Close()
		cancel()
		i.up.Close()
		i.store.Close()
		i.cache.Close()
		rem.Close()
		if t.Failed() || os.Getenv("NARRENHAUS_TEST_LOG") != "" {
			t.Logf("the log of instance %s:\n%s", name, i.log.String())
		}
	})
	return i
}

// answer is what a request was answered with.
type answer struct {
	status int
	header http.Header
	length int64 // the Content-Length as the client understood it, -1 for none
	body   []byte
}

// do sends a request to the instance and reads its answer. header is pairs
// of a name and a value. It may be called from any goroutine.
func (i *instance) do(method, target string, body []byte, header ...string) (answer, error) {
	var in io.Reader
	if body != nil {
		in = bytes.NewReader(body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, i.url+target, in)
	if err != nil {
		return answer{}, fmt.Errorf("%s %s on %s: %w", method, target, i.name, err)
	}
	for n := 0; n+1 < len(header); n += 2 {
		req.Header.Set(header[n], header[n+1])
	}
	res, err := i.http.Do(req)
	if err != nil {
		return answer{}, fmt.Errorf("%s %s on %s: %w", method, target, i.name, err)
	}
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	if err != nil {
		return answer{}, fmt.Errorf("%s %s on %s: reading the answer: %w", method, target, i.name, err)
	}
	return answer{status: res.StatusCode, header: res.Header, length: res.ContentLength, body: got}, nil
}

// expect is do with the status the answer must have.
func (i *instance) expect(status int, method, target string, body []byte, header ...string) (answer, error) {
	got, err := i.do(method, target, body, header...)
	if err != nil {
		return got, err
	}
	if got.status != status {
		return got, fmt.Errorf("%s %s on %s: status %d (%q), want %d", method, target, i.name, got.status, got.body, status)
	}
	return got, nil
}

// want is expect for the test's own goroutine: it fails the test.
func (i *instance) want(status int, method, target string, body []byte, header ...string) answer {
	i.t.Helper()
	got, err := i.expect(status, method, target, body, header...)
	if err != nil {
		i.t.Fatal(err)
	}
	return got
}

// settled waits until the instance's uploader has nothing to do.
func (i *instance) settled() {
	i.t.Helper()
	eventually(i.t, "the uploader of "+i.name+" is idle", i.up.Idle)
}

// staged returns the names of what lies under the cache's staging/.
func (i *instance) staged() []string {
	i.t.Helper()
	entries, err := os.ReadDir(filepath.Join(i.dir, "staging"))
	if err != nil {
		i.t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// What Nix does to a cache, as far as the cache sees it.

// opened is the request Nix begins with: it reads nix-cache-info and
// refuses a cache for another store.
func (i *instance) opened() {
	i.t.Helper()
	got := i.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil)
	if !strings.Contains(string(got.body), "StoreDir: /nix/store\n") {
		i.t.Fatalf("nix-cache-info of %s is %q", i.name, got.body)
	}
}

// sendNAR is the NAR part of a copy: Nix asks whether the file is there
// and sends it when it is not. It may be called from any goroutine.
func (i *instance) sendNAR(p *storePath) error {
	if _, err := i.expect(http.StatusNotFound, http.MethodHead, "/"+p.url, nil); err != nil {
		return err
	}
	_, err := i.expect(http.StatusOK, http.MethodPut, "/"+p.url, p.sent, "Content-Type", "application/x-nix-nar")
	return err
}

// sendNarinfo is the narinfo part of a copy. It may be called from any
// goroutine.
func (i *instance) sendNarinfo(p *storePath) error {
	_, err := i.expect(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo(), "Content-Type", "text/x-nix-narinfo")
	return err
}

// copy uploads store paths as Nix up to 2.34 does: for each one it asks
// for the narinfo, and for a path that is missing sends the NAR and then
// the narinfo.
func (i *instance) copy(paths ...*storePath) {
	i.t.Helper()
	i.opened()
	for _, p := range paths {
		i.want(http.StatusNotFound, http.MethodGet, "/"+p.hash+".narinfo", nil)
		if err := i.sendNAR(p); err != nil {
			i.t.Fatal(err)
		}
		if err := i.sendNarinfo(p); err != nil {
			i.t.Fatal(err)
		}
	}
}

// narinfoOf asks the instance for the narinfo of p and checks what Nix
// checks, and that it describes p: it must carry a signature by the
// instance's key over the fingerprint.
func (i *instance) narinfoOf(p *storePath) *narinfo.Info {
	i.t.Helper()
	got := i.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	info, err := narinfo.Parse(got.body)
	if err != nil {
		i.t.Fatalf("the narinfo of %s on %s: %v\n%s", p.name, i.name, err, got.body)
	}
	if info.StorePath != "/nix/store/"+p.base() || info.NarHashNix32() != p.narHash || info.NarSize != p.size {
		i.t.Fatalf("the narinfo of %s on %s describes another path:\n%s", p.name, i.name, got.body)
	}
	if !slices.Equal(info.References, p.references) {
		i.t.Errorf("the narinfo of %s on %s has the references %q, want %q", p.name, i.name, info.References, p.references)
	}
	if !i.signed(info) {
		i.t.Errorf("the narinfo of %s on %s has no signature of that instance's key that verifies:\n%s", p.name, i.name, got.body)
	}
	return info
}

// signed reports whether info carries a signature by the instance's key
// that verifies.
func (i *instance) signed(info *narinfo.Info) bool {
	i.t.Helper()
	name, text, _ := strings.Cut(i.key.Public(), ":")
	public, err := base64.StdEncoding.DecodeString(text)
	if err != nil || len(public) != ed25519.PublicKeySize {
		i.t.Fatalf("the public key of %s is %q", i.name, i.key.Public())
	}
	for _, sig := range info.Sigs {
		by, text, _ := strings.Cut(sig, ":")
		raw, err := base64.StdEncoding.DecodeString(text)
		if by == name && err == nil && ed25519.Verify(public, []byte(info.Fingerprint()), raw) {
			return true
		}
	}
	return false
}

// substitute fetches p from the instance as Nix does: the narinfo, then
// the NAR it names, which must have the hash and size the narinfo states
// and be the archive that was uploaded.
func (i *instance) substitute(p *storePath) *narinfo.Info {
	i.t.Helper()
	info := i.narinfoOf(p)
	got := i.want(http.StatusOK, http.MethodGet, "/"+info.URL, nil)
	if got.length != int64(info.NarSize) || uint64(len(got.body)) != info.NarSize {
		i.t.Errorf("the NAR of %s on %s: Content-Length %d and %d bytes, the narinfo says %d", p.name, i.name, got.length, len(got.body), info.NarSize)
	}
	if sum := sha256.Sum256(got.body); sum != info.NarHash {
		i.t.Errorf("the NAR of %s on %s does not have the hash its narinfo states", p.name, i.name)
	}
	if !bytes.Equal(got.body, p.archive) {
		i.t.Errorf("the NAR of %s on %s is not the archive that was uploaded", p.name, i.name)
	}
	return info
}

// Store paths.

// objects is a set of objects in memory.
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

// tree is a store path of a few files with size bytes of data; what
// differs between two of them follows from seed.
func tree(seed byte, size int) node {
	return dir(map[string]node{
		"bin":    dir(map[string]node{"tool": script("#!/bin/sh\necho " + string('a'+rune(seed%26)) + "\n")}),
		"data":   file(noise(seed, size)),
		"empty":  text(""),
		"latest": link("data"),
	})
}

// uploadedSig is a signature that came with an upload, by a key nobody
// has: 64 bytes that verify under none.
var uploadedSig = "cache.example-1:" + base64.StdEncoding.EncodeToString(make([]byte, 64))

// storePath is a store path as Nix uploads it.
type storePath struct {
	hash       string // the hash part of the store path
	name       string // the rest of its base name
	archive    []byte
	narHash    string // the archive's SHA-256 in Nix's base 32
	size       uint64
	root       key.Key  // of the content reference narrenhaus is to make
	references []string // base names

	// What goes over the wire: the file, where it is sent, and what the
	// narinfo calls its compression.
	sent        []byte
	url         string
	compression string
}

// storeHash returns the hash part of a store path, 32 characters that
// follow from seed.
func storeHash(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return nix32.Encode(sum[:20])
}

// pathOf returns the store path <hash of seed>-<seed> with the contents n,
// to be uploaded compressed with xz, which is what Nix does unless told
// otherwise. references are other store paths.
func pathOf(t *testing.T, seed string, n node, references ...*storePath) *storePath {
	t.Helper()
	p := &storePath{hash: storeHash(seed), name: seed, archive: archive(t, n)}
	objs := newObjects()
	node, sum, size, err := nartree.Ingest(bytes.NewReader(p.archive), objs.emit, nartree.MaxNARSize)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	p.size, p.narHash = size, nix32.Encode(sum[:])
	if p.root, err = nartree.Root(node, sum, size, objs.emit); err != nil {
		t.Fatalf("Root: %v", err)
	}
	for _, r := range references {
		p.references = append(p.references, r.base())
	}

	var buf bytes.Buffer
	w, err := xz.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(p.archive); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p.sent, p.compression = buf.Bytes(), "xz"
	p.url = "nar/" + fileHash(p.sent) + ".nar.xz"
	return p
}

// uncompressed makes p a path that is uploaded with ?compression=none, and
// returns it.
func (p *storePath) uncompressed() *storePath {
	p.sent, p.compression = p.archive, "none"
	p.url = "nar/" + p.narHash + ".nar"
	return p
}

// base is the store path's name under /nix/store.
func (p *storePath) base() string { return p.hash + "-" + p.name }

// narinfo is the text Nix sends for the store path after it uploaded the
// file: the URL, the hash and the size are the file's.
func (p *storePath) narinfo() []byte {
	return fmt.Appendf(nil, "StorePath: /nix/store/%s\nURL: %s\nCompression: %s\nFileHash: sha256:%s\nFileSize: %d\n"+
		"NarHash: sha256:%s\nNarSize: %d\nReferences: %s\nDeriver: %s-%s.drv\nSig: %s\n",
		p.base(), p.url, p.compression, fileHash(p.sent), len(p.sent), p.narHash, p.size, strings.Join(p.references, " "),
		storeHash("drv "+p.name), p.name, uploadedSig)
}

// narinfoRoot is the root of the narinfo reference narrenhaus is to make
// of the store path.
func (p *storePath) narinfoRoot(t *testing.T) key.Key {
	t.Helper()
	info, err := narinfo.Parse(p.narinfo())
	if err != nil {
		t.Fatalf("the narinfo of the test: %v", err)
	}
	root, err := nartree.File(bytes.NewReader(info.Stored()), newObjects().emit)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	return root
}

// fileHash is the name Nix gives the upload of a file: its SHA-256.
func fileHash(body []byte) string {
	sum := sha256.Sum256(body)
	return nix32.Encode(sum[:])
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
