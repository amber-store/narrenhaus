package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/narrenhaus/testserver"
)

// These tests put the real nix program before two narrenhaus instances.
// Each skips where there is no nix.
//
// Nix remembers what a cache URL answered, per user account, under
// XDG_CACHE_HOME. So every test gives nix a directory of its own for that
// and store paths nobody has seen: directories of random bytes, added to
// this machine's store with `nix store add`, and in one test derivations
// of one line that are built there, all deleted from it again at the
// test's end where the store allows it. The user's Nix configuration is
// left out (NIX_USER_CONF_FILES empty, NIX_CONFIG unset); nothing of the
// machine's configuration is changed, every setting a test needs is on
// nix's command line.

// nix runs the nix program for a test.
type nix struct {
	t    *testing.T
	bin  string
	home string // XDG_CACHE_HOME: what this nix remembers
}

// newNix returns nix with nothing remembered, or skips the test where there
// is no nix.
func newNix(t *testing.T) *nix {
	t.Helper()
	bin, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("no nix on PATH")
	}
	return &nix{t: t, bin: bin, home: t.TempDir()}
}

// forgetful returns the same nix as an account that remembers nothing of
// any cache.
func (n *nix) forgetful() *nix {
	return &nix{t: n.t, bin: n.bin, home: n.t.TempDir()}
}

// env is the environment nix runs in: the test's, without the user's Nix
// configuration and with this nix's memory.
func (n *nix) env() []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "NIX_CONFIG", "NIX_USER_CONF_FILES", "XDG_CACHE_HOME":
		default:
			env = append(env, kv)
		}
	}
	return append(env, "NIX_USER_CONF_FILES=", "XDG_CACHE_HOME="+n.home)
}

// program runs one of Nix's programs with args and returns what it wrote.
func (n *nix) program(name string, stdin []byte, args ...string) (stdout, stderr string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(filepath.Dir(n.bin), name), args...)
	if name == "nix" {
		cmd = exec.CommandContext(ctx, n.bin, append([]string{"--extra-experimental-features", "nix-command flakes"}, args...)...)
	}
	cmd.Env = n.env()
	var out, errOut bytes.Buffer
	cmd.Stdin, cmd.Stdout, cmd.Stderr = bytes.NewReader(stdin), &out, &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// run runs nix with args.
func (n *nix) run(args ...string) (stdout, stderr string, err error) {
	return n.program("nix", nil, args...)
}

// must runs nix with args, which must succeed, and returns its standard
// output and standard error.
func (n *nix) must(args ...string) (stdout, stderr string) {
	n.t.Helper()
	stdout, stderr, err := n.run(args...)
	if err != nil {
		n.t.Fatalf("nix %s: %v\n%s", strings.Join(args, " "), err, stderr)
	}
	return stdout, stderr
}

// token is text nobody has seen before.
func token(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// added is a store path of this machine made of a directory of files.
type added struct {
	path  string // /nix/store/<hash>-<name>
	files map[string][]byte
}

// add makes a new store path of this machine: a directory with a file
// data of size random bytes and a small script. The test's end deletes it.
func (n *nix) add(name string, size int) *added {
	n.t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		n.t.Fatal(err)
	}
	a := &added{files: map[string][]byte{
		"data":     data,
		"bin/tool": []byte("#!/bin/sh\necho " + token(n.t) + "\n"),
	}}
	dir := filepath.Join(n.t.TempDir(), name)
	for file, body := range a.files {
		path := filepath.Join(dir, file)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			n.t.Fatal(err)
		}
		mode := fs.FileMode(0o644)
		if strings.HasPrefix(file, "bin/") {
			mode = 0o755
		}
		if err := os.WriteFile(path, body, mode); err != nil {
			n.t.Fatal(err)
		}
	}
	if err := os.Symlink("data", filepath.Join(dir, "latest")); err != nil {
		n.t.Fatal(err)
	}
	// `nix store add` was `nix store add-path` before Nix 2.20.
	out, stderr, err := n.run("store", "add", "--name", name, dir)
	if err != nil {
		first := stderr
		if out, stderr, err = n.run("store", "add-path", "--name", name, dir); err != nil {
			n.t.Fatalf("nix store add: %s\nnix store add-path: %v\n%s", first, err, stderr)
		}
	}
	a.path = strings.TrimSpace(out)
	if !strings.HasPrefix(a.path, "/nix/store/") || !strings.HasSuffix(a.path, "-"+name) {
		n.t.Fatalf("nix store add printed %q", out)
	}
	n.deleteAtEnd(a.path)
	return a
}

// referring makes a new store path of this machine that refers to target:
// one file that names it. `nix store add` records no references, so the
// path is made by evaluating builtins.toFile on a string that carries the
// target, which records it. The test's end deletes the path.
func (n *nix) referring(name, target string) string {
	n.t.Helper()
	expr := fmt.Sprintf("builtins.toFile %q (builtins.storePath %q + %q)", name, target, "\n"+token(n.t)+"\n")
	out, _ := n.must("eval", "--impure", "--raw", "--expr", expr)
	path := strings.TrimSpace(out)
	if !strings.HasPrefix(path, "/nix/store/") || !strings.HasSuffix(path, "-"+name) {
		n.t.Fatalf("nix eval printed %q", out)
	}
	n.deleteAtEnd(path)
	return path
}

// deleteAtEnd takes a path the test made out of this machine's store when
// the test ends. A store that refuses (the user may not be allowed, or
// something still uses the path) keeps it until its next collection.
func (n *nix) deleteAtEnd(path string) {
	n.t.Cleanup(func() {
		if _, stderr, err := n.run("store", "delete", path); err != nil {
			n.t.Logf("%s stays in the store: %v\n%s", path, err, stderr)
		}
	})
}

// base is the name of a store path under /nix/store.
func base(path string) string { return strings.TrimPrefix(path, "/nix/store/") }

// throwaway is a Nix store of the test's own, under a temporary directory:
// store paths lie under <root>/nix/store.
type throwaway struct {
	t    *testing.T
	root string
}

func newThrowaway(t *testing.T) *throwaway {
	t.Helper()
	// Nix wants the real name of the directory, and macOS keeps its
	// temporary directories behind a link.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A store's directories are read-only; the test's end can only remove
	// them once they are not.
	t.Cleanup(func() {
		filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	return &throwaway{t: t, root: root}
}

// url is the store's URL for nix.
func (s *throwaway) url() string { return "local?root=" + s.root }

// file reads a file of a store path in the store.
func (s *throwaway) file(path, file string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.root, path, file))
}

// has reports whether the store path is in the store.
func (s *throwaway) has(path string) bool {
	_, err := os.Lstat(filepath.Join(s.root, path))
	return err == nil
}

// holds fails the test unless a is in the store as it was made.
func (s *throwaway) holds(a *added) {
	s.t.Helper()
	for file, want := range a.files {
		got, err := s.file(a.path, file)
		if err != nil {
			s.t.Errorf("%s in the throwaway store: %v", a.path, err)
			return
		}
		if !bytes.Equal(got, want) {
			s.t.Errorf("%s/%s in the throwaway store is not what was added (%d bytes, want %d)", a.path, file, len(got), len(want))
		}
	}
	if target, err := os.Readlink(filepath.Join(s.root, a.path, "latest")); err != nil || target != "data" {
		s.t.Errorf("%s/latest in the throwaway store: %q, %v, want a link to data", a.path, target, err)
	}
	if info, err := os.Stat(filepath.Join(s.root, a.path, "bin/tool")); err != nil || info.Mode().Perm()&0o111 == 0 {
		s.t.Errorf("%s/bin/tool in the throwaway store is not executable: %v", a.path, err)
	}
}

// requests returns the requests an instance has answered since its log was
// from bytes long, as "METHOD /path status".
func (i *instance) requests(from int) []string {
	var seen []string
	for _, m := range requestLine.FindAllStringSubmatch(i.log.String()[from:], -1) {
		seen = append(seen, m[1]+" "+m[2]+" "+m[3])
	}
	return seen
}

// requestLine is the line the handler logs for a request.
var requestLine = regexp.MustCompile(`(?m) msg=request method=(\S+) path=(\S+) status=(\d+) `)

// count returns how many of the requests begin with prefix and end with
// suffix.
func count(requests []string, prefix, suffix string) int {
	n := 0
	for _, r := range requests {
		if strings.HasPrefix(r, prefix) && strings.HasSuffix(r, suffix) {
			n++
		}
	}
	return n
}

// What the design calls success: a path copied to one narrenhaus with
// `nix copy --to` is substituted from another one, with its references,
// and Nix finds it sound. For Nix's default upload (xz), for the upload
// the advice asks for (uncompressed) and for zstd with a path whose NAR is
// over 16 MiB.
func TestNixCopyThenSubstitute(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
		size   int
		// How the NAR arrives at narrenhaus.
		put string
	}{
		{"xz", "", 100 << 10, ".nar.xz"},
		{"none", "?compression=none", 100 << 10, ".nar"},
		{"zstd", "?compression=zstd", 17 << 20, ".nar.zst"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nix := newNix(t)
			h := start(t, testserver.Options{})

			// A path, and a second one that refers to it.
			dep := nix.add("narrenhaus-test-dep", tc.size)
			top := nix.referring("narrenhaus-test-top", dep.path)

			// Copied to a: the closure, so both.
			_, stderr := nix.must("copy", "--to", h.a.url+tc.params, top)
			seen := h.a.requests(0)
			if n := count(seen, "PUT /nar/", tc.put+" 200"); n != 2 {
				t.Errorf("a took %d NARs as %s, want 2:\n%s\n%s", n, tc.put, strings.Join(seen, "\n"), stderr)
			}
			if n := count(seen, "PUT /", ".narinfo 200"); n != 2 {
				t.Errorf("a took %d narinfos, want 2:\n%s", n, strings.Join(seen, "\n"))
			}
			h.a.settled()
			if staged := h.a.staged(); len(staged) != 0 {
				t.Errorf("a still has %v staged after the copy", staged)
			}

			// Substituted from b into a store that has nothing, by another
			// account, trusting b's key.
			store := newThrowaway(t)
			other := nix.forgetful()
			other.must("copy", "--from", h.b.url, "--to", store.url(),
				"--option", "trusted-public-keys", h.b.key.Public(), top)
			store.holds(dep)
			if got, err := store.file(top, ""); err != nil || !bytes.Contains(got, []byte(dep.path)) {
				t.Errorf("%s in the throwaway store does not name %s: %q, %v", top, dep.path, got, err)
			}
			// Nix finds both paths sound: contents that hash to what the
			// narinfos said, and trusted.
			out, stderr, err := other.run("store", "verify", "--store", store.url(),
				"--option", "trusted-public-keys", h.b.key.Public(), "--recursive", top)
			if err != nil {
				t.Errorf("nix store verify: %v\n%s%s", err, out, stderr)
			}
			// The reference came through the narinfo.
			out, _ = other.must("path-info", "--store", store.url(), "--recursive", top)
			if got := strings.Fields(out); !slices.Contains(got, dep.path) || !slices.Contains(got, top) || len(got) != 2 {
				t.Errorf("the closure of %s in the throwaway store is %q, want it and %s", top, got, dep.path)
			}

			// b served NARs uncompressed under their own hashes, and
			// nothing else.
			served := h.b.requests(0)
			if n := count(served, "GET /nar/", ".nar 200"); n != 2 {
				t.Errorf("b served %d NARs, want 2:\n%s", n, strings.Join(served, "\n"))
			}
			for _, r := range served {
				if !strings.HasSuffix(r, " 200") && !strings.HasSuffix(r, " 404") {
					t.Errorf("b answered %s", r)
				}
			}
		})
	}
}

// A path that is not content-addressed is only taken with a signature Nix
// trusts, so this is where the key of an instance counts: Nix refuses the
// path from b without b's key, and with a's key, and takes it with b's.
//
// What `nix store add` and builtins.toFile make is content-addressed, which
// Nix takes from anybody. The same paths exported and imported into a
// throwaway store are not: the export format has no place for it.
func TestNixTrustsThePathByTheKeyOfTheInstance(t *testing.T) {
	nix := newNix(t)
	h := start(t, testserver.Options{})

	dep := nix.add("narrenhaus-test-dep", 20<<10)
	top := nix.referring("narrenhaus-test-top", dep.path)
	exported, stderr, err := nix.program("nix-store", nil, "--export", dep.path, top)
	if err != nil {
		t.Fatalf("nix-store --export: %v\n%s", err, stderr)
	}
	source := newThrowaway(t)
	if _, stderr, err := nix.program("nix-store", []byte(exported), "--store", source.url(), "--import"); err != nil {
		t.Fatalf("nix-store --import: %v\n%s", err, stderr)
	}
	// A newer Nix wants to be told the format of its JSON, an older one
	// does not know the option. Either prints the content address as a
	// string where there is one.
	info, _, err := nix.run("path-info", "--store", source.url(), "--json", "--json-format", "1", top)
	if err != nil {
		info, _ = nix.must("path-info", "--store", source.url(), "--json", top)
	}
	if strings.Contains(info, `"ca":"`) || !strings.Contains(info, dep.path) {
		t.Fatalf("the imported path is content-addressed or lost its reference:\n%s", info)
	}

	nix.must("copy", "--store", source.url(), "--to", h.a.url+"?compression=none", top)

	for _, tc := range []struct {
		name string
		keys string
		ok   bool
	}{
		{"no key", "", false},
		{"the key of a", h.a.key.Public(), false},
		{"the key of b", h.b.key.Public(), true},
	} {
		store := newThrowaway(t)
		_, stderr, err := nix.forgetful().run("copy", "--from", h.b.url, "--to", store.url(),
			"--option", "trusted-public-keys", tc.keys, top)
		if tc.ok {
			if err != nil {
				t.Fatalf("with %s: nix copy --from b: %v\n%s", tc.name, err, stderr)
			}
			store.holds(dep)
			continue
		}
		if err == nil || store.has(top) || store.has(dep.path) {
			t.Errorf("with %s Nix took the path from b: %v\n%s", tc.name, err, stderr)
		} else if !strings.Contains(stderr, "signature") {
			t.Errorf("with %s Nix refused the path from b for another reason:\n%s", tc.name, stderr)
		}
	}
}

// With ?compression=none the NAR's URL is the one narrenhaus answers HEAD
// for truthfully, so a NAR the server has is not sent again: after the
// narinfo references were deleted on the server, a second copy sends the
// narinfos alone.
func TestSecondCopyWithCompressionNoneSendsNoNAR(t *testing.T) {
	nix := newNix(t)
	h := start(t, testserver.Options{})
	dep := nix.add("narrenhaus-test-dep", 100<<10)
	top := nix.referring("narrenhaus-test-top", dep.path)
	to := h.a.url + "?compression=none"

	nix.must("copy", "--to", to, top)
	first := h.a.requests(0)
	if n := count(first, "PUT /nar/", ".nar 200"); n != 2 {
		t.Fatalf("the first copy sent %d NARs, want 2:\n%s", n, strings.Join(first, "\n"))
	}
	h.a.settled()

	// The narinfo references go; the content stays.
	ctx := context.Background()
	client := h.srv.Client(t)
	refs, err := client.List(ctx, testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	deleted := 0
	for _, r := range refs {
		if strings.HasSuffix(r.Name, ".narinfo") {
			if err := client.Delete(ctx, r.Name); err != nil {
				t.Fatal(err)
			}
			deleted++
		}
	}
	if deleted != 2 || len(refs) != 4 {
		t.Fatalf("the server had %d references, %d of them narinfos, want 4 and 2", len(refs), deleted)
	}
	for _, path := range []string{dep.path, top} {
		h.a.want(http.StatusNotFound, http.MethodGet, "/"+base(path)[:32]+".narinfo", nil)
	}

	// The second copy, by an account that does not remember the first.
	mark := len(h.a.log.String())
	nix.forgetful().must("copy", "--to", to, top)
	second := h.a.requests(mark)
	if n := count(second, "HEAD /nar/", ".nar 200"); n != 2 {
		t.Errorf("the second copy was told %d times that a NAR is there, want 2:\n%s", n, strings.Join(second, "\n"))
	}
	if n := count(second, "PUT /nar/", ""); n != 0 {
		t.Errorf("the second copy sent %d NARs, want none:\n%s", n, strings.Join(second, "\n"))
	}
	if n := count(second, "PUT /", ".narinfo 200"); n != 2 {
		t.Errorf("the second copy sent %d narinfos, want 2:\n%s", n, strings.Join(second, "\n"))
	}

	// And the path can be substituted again.
	store := newThrowaway(t)
	nix.forgetful().must("copy", "--from", h.b.url, "--to", store.url(),
		"--option", "trusted-public-keys", h.b.key.Public(), top)
	store.holds(dep)
}

// stoppable stands before an instance at an address of its own, so that
// the test can stop "narrenhaus" as its user does: after stop, nothing
// listens there.
type stoppable struct {
	url  string
	stop func()
}

func stoppableFront(t *testing.T, i *instance) *stoppable {
	t.Helper()
	target, err := url.Parse(i.url)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: httputil.NewSingleHostReverseProxy(target)}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return &stoppable{url: "http://" + ln.Addr().String(), stop: func() { srv.Close() }}
}

// The case of the design's section 8.1 for a substitution: Nix has a
// narrenhaus among its substituters that it has talked to before and that
// is not running now, and fallback = true as the advice asks. A
// substitution of a path another cache has must still succeed, and Nix
// must have tried the stopped one. Whether Nix fails here without fallback
// depends on its version and on the order it asks the caches in, and is
// only noted; TestBuildWithStoppedNarrenhausNeedsFallback is the case in
// which it must.
//
// The other cache is instance b; the narrenhaus that is stopped is a front
// before instance a. The store is a throwaway one, so the substituters are
// asked by this nix process and not by the machine's daemon.
func TestStoppedNarrenhausWithFallback(t *testing.T) {
	nix := newNix(t)
	h := start(t, testserver.Options{})
	p := nix.add("narrenhaus-test-path", 100<<10)
	nix.must("copy", "--to", h.a.url+"?compression=none", p.path)

	// Nix meets the narrenhaus while it runs, and remembers it as a cache
	// (for seven days).
	front := stoppableFront(t, h.a)
	user := nix.forgetful()
	mark := len(h.a.log.String())
	// `nix store info` was `nix store ping` before Nix 2.19.
	if _, first, err := user.run("store", "info", "--store", front.url); err != nil {
		if _, stderr, err := user.run("store", "ping", "--store", front.url); err != nil {
			t.Fatalf("nix store info: %s\nnix store ping: %v\n%s", first, err, stderr)
		}
	}
	if n := count(h.a.requests(mark), "GET /nix-cache-info", " 200"); n != 1 {
		t.Fatalf("Nix did not ask the narrenhaus that is to be stopped what kind of cache it is")
	}

	front.stop()
	if conn, err := net.DialTimeout("tcp", strings.TrimPrefix(front.url, "http://"), time.Second); err == nil {
		conn.Close()
		t.Fatalf("the stopped narrenhaus still listens at %s", front.url)
	}

	substitute := func(store *throwaway, fallback string) (stderr string, err error) {
		_, stderr, err = user.run("build", "--no-link", "--store", store.url(),
			"--option", "substituters", front.url+" "+h.b.url,
			"--option", "trusted-public-keys", h.b.key.Public(),
			"--option", "fallback", fallback,
			p.path)
		return stderr, err
	}

	// What the advice is for. Without fallback this Nix may fail here or
	// not, by its version: that is Nix's business and only noted.
	without := newThrowaway(t)
	stderr, err := substitute(without, "false")
	t.Logf("without fallback: %v, the path arrived: %v\n%s", err, without.has(p.path), stderr)

	store := newThrowaway(t)
	began := time.Now()
	stderr, err = substitute(store, "true")
	if err != nil {
		t.Fatalf("with a stopped narrenhaus among the substituters and fallback = true Nix failed: %v\n%s", err, stderr)
	}
	t.Logf("with fallback Nix took %v and said:\n%s", time.Since(began).Round(time.Millisecond), stderr)
	store.holds(p)
	if !strings.Contains(stderr, front.url) {
		t.Errorf("Nix did not try the stopped narrenhaus at %s:\n%s", front.url, stderr)
	}
}

// building is a derivation nobody has built: its output is a path that
// only a cache could have, and no cache has.
type building struct {
	nix   *nix
	token string
	expr  string
}

// derivation makes a derivation of this machine's store whose build writes
// a text nobody has seen into its output. The test's end deletes it.
func (n *nix) derivation() *building {
	n.t.Helper()
	b := &building{nix: n, token: token(n.t)}
	b.expr = fmt.Sprintf(`derivation { name = "narrenhaus-test-built"; system = builtins.currentSystem; `+
		`builder = "/bin/sh"; args = [ "-c" "echo %s > $out" ]; }`, b.token)
	drv, _ := n.must("eval", "--impure", "--raw", "--expr", "("+b.expr+").drvPath")
	if !strings.HasPrefix(drv, "/nix/store/") || !strings.HasSuffix(drv, ".drv") {
		n.t.Fatalf("nix eval printed %q", drv)
	}
	n.deleteAtEnd(drv)
	return b
}

// build builds the derivation with the machine's store, with substituter
// as the one cache to ask, and returns the output's path.
func (b *building) build(substituter, fallback string) (out, stderr string, err error) {
	out, stderr, err = b.nix.run("build", "--no-link", "--print-out-paths", "--impure",
		"--option", "substituters", substituter,
		"--option", "fallback", fallback,
		"--expr", b.expr)
	if out = strings.TrimSpace(out); err == nil {
		b.nix.deleteAtEnd(out)
	}
	return out, stderr, err
}

// The case of the design's section 8.1 as the design states it: narrenhaus
// is among Nix's substituters, Nix has met it, and it is not running. A
// build that needs a path only narrenhaus could have fails without
// fallback = true and succeeds with it, which is why the advice asks for
// the line.
//
// The builds are the machine's, so they go through its Nix daemon where
// there is one, and a daemon takes substituters only from a user it
// trusts: where it does not ask the narrenhaus of the test, the test
// skips. Nothing of the machine's configuration is changed; what stays is
// what the daemon remembers of a cache at a port of loopback.
func TestBuildWithStoppedNarrenhausNeedsFallback(t *testing.T) {
	nix := newNix(t)
	h := start(t, testserver.Options{})
	front := stoppableFront(t, h.a)
	user := nix.forgetful()

	// Nix meets the narrenhaus while it runs: asked for the output of a
	// build, it has nothing, and Nix builds.
	mark := len(h.a.log.String())
	met := user.derivation()
	out, stderr, err := met.build(front.url, "false")
	if err != nil {
		t.Fatalf("a build with a running narrenhaus as the substituter: %v\n%s", err, stderr)
	}
	asked := h.a.requests(mark)
	if count(asked, "GET /nix-cache-info", " 200") == 0 {
		t.Skipf("this Nix does not ask the substituters its user names (the daemon does not trust the user):\n%s", stderr)
	}
	if want := "GET /" + base(out)[:32] + ".narinfo 404"; !slices.Contains(asked, want) {
		t.Fatalf("Nix did not ask the narrenhaus for the output of its build, want %s:\n%s", want, strings.Join(asked, "\n"))
	}

	front.stop()
	if conn, err := net.DialTimeout("tcp", strings.TrimPrefix(front.url, "http://"), time.Second); err == nil {
		conn.Close()
		t.Fatalf("the stopped narrenhaus still listens at %s", front.url)
	}

	// Without fallback the build fails on the cache that does not answer.
	without := user.derivation()
	out, stderr, err = without.build(front.url, "false")
	if err == nil {
		t.Errorf("with a stopped narrenhaus as the substituter and fallback = false Nix built %s:\n%s", out, stderr)
	} else if !strings.Contains(stderr, front.url) {
		t.Errorf("without fallback Nix failed for another reason than the stopped narrenhaus at %s:\n%s", front.url, stderr)
	}

	// With it Nix says that the cache failed and builds.
	with := user.derivation()
	out, stderr, err = with.build(front.url, "true")
	if err != nil {
		t.Fatalf("with a stopped narrenhaus as the substituter and fallback = true Nix failed: %v\n%s", err, stderr)
	}
	if !strings.Contains(stderr, front.url) {
		t.Errorf("Nix did not try the stopped narrenhaus at %s:\n%s", front.url, stderr)
	}
	if got, err := os.ReadFile(out); err != nil || strings.TrimSpace(string(got)) != with.token {
		t.Errorf("the output %s of the build with fallback holds %q, %v, want %s", out, got, err, with.token)
	}
}
