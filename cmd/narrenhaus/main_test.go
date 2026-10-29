package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-store/bucket/buckettest"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/urfave/cli/v2"

	"github.com/amber-store/narrenhaus/nar"
	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/remote"
	"github.com/amber-store/narrenhaus/testserver"
)

// testServer is the public key of RFC 8032's first test vector, in the hex
// form of an endpoint ID. No server has it.
const testServer = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"

// optionVariables are the environment variables of the options.
var optionVariables = []string{
	"NARRENHAUS_SERVER",
	"NARRENHAUS_PREFIX",
	"NARRENHAUS_LISTEN",
	"NARRENHAUS_CACHE_DIR",
	"NARRENHAUS_CACHE_SIZE",
	"NARRENHAUS_CONFIG_DIR",
	"NARRENHAUS_PRIORITY",
}

// clearEnvironment takes the options' variables out of the environment for
// the test, so that what the developer's shell has set decides nothing.
// The user's directories, which two options have their defaults under, it
// puts under a directory of the test: where the test runs there may be no
// home at all, and those defaults are then empty.
func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range optionVariables {
		t.Setenv(name, "") // registers the restore
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
}

// load runs loadOptions on a command line of options alone.
func load(t *testing.T, args ...string) (options, error) {
	t.Helper()
	var (
		opts    options
		loadErr error
		ran     bool
	)
	app := &cli.App{
		Name:      "narrenhaus",
		Flags:     optionFlags(),
		Writer:    io.Discard,
		ErrWriter: io.Discard,
		Action: func(c *cli.Context) error {
			ran = true
			opts, loadErr = loadOptions(c)
			return nil
		},
	}
	if err := app.Run(append([]string{"narrenhaus"}, args...)); err != nil {
		t.Fatalf("parsing %q: %v", args, err)
	}
	if !ran {
		t.Fatalf("the action did not run for %q", args)
	}
	return opts, loadErr
}

func TestParseSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want uint64
	}{
		{"10GiB", 10 << 30},
		{"500MiB", 500 << 20},
		{"123", 123},
		{"1073741824", 1 << 30},
		{"1B", 1},
		{"4KiB", 4 << 10},
		{"2TiB", 2 << 40},
		{"16777215TiB", 16777215 << 40},
		{"18446744073709551615", 1<<64 - 1},
	} {
		got, err := parseSize(tc.in)
		if err != nil {
			t.Errorf("parseSize(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, in := range []string{
		"", "0", "ten", "-1GiB",
		"0GiB", "0B", "-1", "+1", "GiB", "1.5GiB", "10 GiB", " 10GiB", "10GiB ", "10gib", "10GB", "10G",
		"1e3", "0x10", "10GiBB",
		"16777216TiB",          // 2^64 bytes
		"18446744073709551616", // 2^64
		"99999999999GiB",
	} {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, want an error", in, got)
		}
	}
}

func TestLoadOptionsDefaults(t *testing.T) {
	clearEnvironment(t)
	// The user's directories are where this test says, whatever the
	// environment of the test run has (a build sandbox has no home).
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	opts, err := load(t, "--server", testServer, "--prefix", "acme/")
	if err != nil {
		t.Fatal(err)
	}
	if got := opts.Server.String(); got != testServer {
		t.Errorf("Server = %s, want %s", got, testServer)
	}
	if opts.Prefix != "acme/" {
		t.Errorf("Prefix = %q, want %q", opts.Prefix, "acme/")
	}
	if opts.Listen != "127.0.0.1:8484" {
		t.Errorf("Listen = %q, want 127.0.0.1:8484", opts.Listen)
	}
	if opts.CacheSize != 10<<30 {
		t.Errorf("CacheSize = %d, want %d", opts.CacheSize, uint64(10<<30))
	}
	if opts.Priority != 50 {
		t.Errorf("Priority = %d, want 50", opts.Priority)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(cache, "narrenhaus"); opts.CacheDir != want {
		t.Errorf("CacheDir = %q, want %q", opts.CacheDir, want)
	}
	config, err := os.UserConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(config, "narrenhaus"); opts.ConfigDir != want {
		t.Errorf("ConfigDir = %q, want %q", opts.ConfigDir, want)
	}
}

func TestLoadOptionsFromFlags(t *testing.T) {
	clearEnvironment(t)
	opts, err := load(t,
		"--server", testServer,
		"--prefix", "acme/",
		"--listen", "[::1]:9000",
		"--cache-dir", "/var/cache/nh",
		"--cache-size", "500MiB",
		"--config-dir", "/etc/nh",
		"--priority", "30",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := options{
		Server:    opts.Server,
		Prefix:    "acme/",
		Listen:    "[::1]:9000",
		CacheDir:  "/var/cache/nh",
		CacheSize: 500 << 20,
		ConfigDir: "/etc/nh",
		Priority:  30,
	}
	if opts != want {
		t.Errorf("options = %+v, want %+v", opts, want)
	}
	if got := opts.Server.String(); got != testServer {
		t.Errorf("Server = %s, want %s", got, testServer)
	}
}

func TestLoadOptionsFromEnvironment(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("NARRENHAUS_SERVER", testServer)
	t.Setenv("NARRENHAUS_PREFIX", "acme/")
	t.Setenv("NARRENHAUS_LISTEN", "127.0.0.1:9001")
	t.Setenv("NARRENHAUS_CACHE_DIR", "/var/cache/nh")
	t.Setenv("NARRENHAUS_CACHE_SIZE", "2GiB")
	t.Setenv("NARRENHAUS_CONFIG_DIR", "/etc/nh")
	t.Setenv("NARRENHAUS_PRIORITY", "45")
	opts, err := load(t)
	if err != nil {
		t.Fatal(err)
	}
	want := options{
		Server:    opts.Server,
		Prefix:    "acme/",
		Listen:    "127.0.0.1:9001",
		CacheDir:  "/var/cache/nh",
		CacheSize: 2 << 30,
		ConfigDir: "/etc/nh",
		Priority:  45,
	}
	if opts != want {
		t.Errorf("options = %+v, want %+v", opts, want)
	}
	if got := opts.Server.String(); got != testServer {
		t.Errorf("Server = %s, want %s", got, testServer)
	}

	// The flag wins over the variable.
	opts, err = load(t, "--prefix", "other/", "--priority", "7")
	if err != nil {
		t.Fatal(err)
	}
	if opts.Prefix != "other/" || opts.Priority != 7 {
		t.Errorf("Prefix, Priority = %q, %d, want %q, 7", opts.Prefix, opts.Priority, "other/")
	}
}

func TestLoadOptionsRefusesABadPrefix(t *testing.T) {
	clearEnvironment(t)
	for _, prefix := range []string{
		"a@b/",      // '@' separates a reference from a path in core
		"acme\n",    // a control character
		"acme\xff/", // not UTF-8
		strings.Repeat("a", reference.MaxNameLen-59), // the content reference's name is one byte too long, the narinfo's is not
	} {
		_, err := load(t, "--server", testServer, "--prefix", prefix)
		if err == nil {
			t.Errorf("prefix %q: no error", prefix)
			continue
		}
		if !strings.Contains(err.Error(), "--prefix") {
			t.Errorf("prefix %q: error %q does not name --prefix", prefix, err)
		}
	}
	// The error says which prefix it was.
	_, err := load(t, "--server", testServer, "--prefix", "a@b/")
	if err == nil || !strings.Contains(err.Error(), "a@b/") {
		t.Errorf("prefix a@b/: error %v does not mention the prefix", err)
	}
	// The longest prefix that still fits.
	if _, err := load(t, "--server", testServer, "--prefix", strings.Repeat("a", reference.MaxNameLen-60)); err != nil {
		t.Errorf("a prefix that leaves room for the content reference's name: %v", err)
	}
	// A prefix is required.
	for _, args := range [][]string{
		{"--server", testServer},
		{"--server", testServer, "--prefix", ""},
	} {
		_, err := load(t, args...)
		if err == nil || !strings.Contains(err.Error(), "--prefix") {
			t.Errorf("%q: error %v, want one naming --prefix", args, err)
		}
	}
}

func TestLoadOptionsRefusesABadServer(t *testing.T) {
	clearEnvironment(t)
	id, err := irohkey.ParseEndpointID(testServer)
	if err != nil {
		t.Fatal(err)
	}
	for _, server := range []string{
		"nonsense",
		testServer[:63],                 // short
		testServer + "00",               // long
		"x" + testServer[1:],            // not hex
		"02" + strings.Repeat("00", 31), // 32 bytes that are no point of the curve
		id.Z32(),                        // z-base-32, which iroh's own tools do not print
	} {
		_, err := load(t, "--server", server, "--prefix", "acme/")
		if err == nil {
			t.Errorf("server %q: no error", server)
			continue
		}
		if !strings.Contains(err.Error(), "--server") {
			t.Errorf("server %q: error %q does not name --server", server, err)
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("server %q: error %q is more than one line", server, err)
		}
	}
	// A server is required.
	for _, args := range [][]string{
		{"--prefix", "acme/"},
		{"--prefix", "acme/", "--server", ""},
	} {
		_, err := load(t, args...)
		if err == nil || !strings.Contains(err.Error(), "--server") {
			t.Errorf("%q: error %v, want one naming --server", args, err)
		}
	}
	// The other form of an endpoint ID, RFC 4648's base 32 without padding
	// in either case, is taken.
	raw := id.Bytes()
	b32 := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:])
	for _, server := range []string{b32, strings.ToLower(b32)} {
		opts, err := load(t, "--server", server, "--prefix", "acme/")
		if err != nil {
			t.Errorf("server %q: %v", server, err)
			continue
		}
		if opts.Server != id {
			t.Errorf("server %q: got %s, want %s", server, opts.Server, id)
		}
	}
}

func TestLoadOptionsRefusesOtherBadOptions(t *testing.T) {
	clearEnvironment(t)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"--cache-size", []string{"--cache-size", "0"}},
		{"--cache-size", []string{"--cache-size", "lots"}},
		{"--cache-size", []string{"--cache-size", ""}},
		{"--listen", []string{"--listen", ""}},
		{"--listen", []string{"--listen", "localhost"}},
		{"--listen", []string{"--listen", "127.0.0.1:http:8484"}},
		{"--listen", []string{"--listen", "127.0.0.1:99999"}},
		{"--listen", []string{"--listen", "127.0.0.1:-1"}},
		{"--cache-dir", []string{"--cache-dir", ""}},
		{"--config-dir", []string{"--config-dir", ""}},
	} {
		args := append([]string{"--server", testServer, "--prefix", "acme/"}, tc.args...)
		_, err := load(t, args...)
		if err == nil {
			t.Errorf("%q: no error", tc.args)
			continue
		}
		if !strings.Contains(err.Error(), tc.name) {
			t.Errorf("%q: error %q does not name %s", tc.args, err, tc.name)
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("%q: error %q is more than one line", tc.args, err)
		}
	}
}

// run runs the whole command in-process.
func run(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err = newApp(&out, &errOut).Run(append([]string{"narrenhaus"}, args...))
	return out.String(), errOut.String(), err
}

func TestHelpListsTheCommands(t *testing.T) {
	clearEnvironment(t)
	stdout, _, err := run(t, "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"narrenhaus", "serve", "check"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("help does not mention %q:\n%s", want, stdout)
		}
	}
}

// The flags, variables and defaults of the design's section 10, verbatim.
func TestServeHelpListsTheOptions(t *testing.T) {
	clearEnvironment(t)
	stdout, _, err := run(t, "serve", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range append([]string{
		"--server", "--prefix", "--listen", "--cache-dir", "--cache-size", "--config-dir", "--priority",
		"127.0.0.1:8484", "10GiB", "50",
	}, optionVariables...) {
		if !strings.Contains(stdout, want) {
			t.Errorf("help of serve does not mention %q:\n%s", want, stdout)
		}
	}
}

// syncBuffer is a buffer the command writes to while the test reads it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// runContext runs the whole command in-process under ctx, which stands for
// the signals: when it ends, serve stops.
func runContext(t *testing.T, ctx context.Context, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut syncBuffer
	err = newApp(&out, &errOut).RunContext(ctx, append([]string{"narrenhaus"}, args...))
	return out.String(), errOut.String(), err
}

// noNix takes every program off PATH, so that the setup check finds no nix
// and says the same on every machine.
func noNix(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", "")
}

// daemon is a serve that runs in this process.
type daemon struct {
	t       *testing.T
	log     *syncBuffer // what it wrote to standard error
	url     string      // where it serves, from its log
	cancel  context.CancelFunc
	stopped chan struct{}
	err     error // what it returned, once stopped is closed
}

// servingLine is the line serve logs once it serves, with the URL.
var servingLine = regexp.MustCompile(`(?m)^.* msg=serving url=(http://\S+).*$`)

// startDaemon runs a serve and waits until it says that it serves. The
// test's end stops it, and prints what it wrote when the test failed or
// NARRENHAUS_TEST_LOG is set.
func startDaemon(t *testing.T, serve func(ctx context.Context, stderr io.Writer) error) *daemon {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	d := &daemon{t: t, log: &syncBuffer{}, cancel: cancel, stopped: make(chan struct{})}
	go func() {
		defer close(d.stopped)
		d.err = serve(ctx, d.log)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-d.stopped:
		case <-time.After(time.Minute):
			t.Errorf("serve did not stop:\n%s", d.log.String())
		}
		if t.Failed() || os.Getenv("NARRENHAUS_TEST_LOG") != "" {
			t.Logf("what serve wrote to standard error:\n%s", d.log.String())
		}
	})
	for end := time.Now().Add(30 * time.Second); ; {
		if m := servingLine.FindStringSubmatch(d.log.String()); m != nil {
			d.url = m[1]
			return d
		}
		select {
		case <-d.stopped:
			t.Fatalf("serve returned before it served: %v\n%s", d.err, d.log.String())
		case <-time.After(2 * time.Millisecond):
		}
		if time.Now().After(end) {
			t.Fatalf("serve never said that it serves:\n%s", d.log.String())
		}
	}
}

// stop ends the daemon as a signal does and returns what serve returned.
func (d *daemon) stop() error {
	d.t.Helper()
	d.cancel()
	return d.wait(time.Minute)
}

// wait waits for serve to return.
func (d *daemon) wait(limit time.Duration) error {
	d.t.Helper()
	select {
	case <-d.stopped:
		return d.err
	case <-time.After(limit):
		d.t.Fatalf("serve did not return within %v:\n%s", limit, d.log.String())
		return nil
	}
}

// running reports whether serve has not returned yet.
func (d *daemon) running() bool {
	select {
	case <-d.stopped:
		return false
	default:
		return true
	}
}

// do sends a request to the daemon and reads the answer.
func (d *daemon) do(method, target string, body io.Reader) (status int, got []byte, err error) {
	req, err := http.NewRequest(method, d.url+target, body)
	if err != nil {
		return 0, nil, err
	}
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	got, err = io.ReadAll(res.Body)
	return res.StatusCode, got, err
}

// want is do with the status the answer must have.
func (d *daemon) want(status int, method, target string, body []byte) []byte {
	d.t.Helper()
	var in io.Reader
	if body != nil {
		in = bytes.NewReader(body)
	}
	code, got, err := d.do(method, target, in)
	if err != nil {
		d.t.Fatalf("%s %s: %v\n%s", method, target, err, d.log.String())
	}
	if code != status {
		d.t.Fatalf("%s %s: status %d (%q), want %d\n%s", method, target, code, got, status, d.log.String())
	}
	return got
}

// serveArgs is a command line of serve that is good: the test's server ID,
// a port the system chooses, and directories under dir.
func serveArgs(dir string, more ...string) []string {
	return append([]string{
		"serve",
		"--server", testServer,
		"--prefix", "acme/",
		"--listen", "127.0.0.1:0",
		"--cache-dir", filepath.Join(dir, "cache"),
		"--config-dir", filepath.Join(dir, "config"),
	}, more...)
}

// Bad start-up input is one line that names the option and no panic, and
// serve does not start.
func TestServeRefusesBadOptions(t *testing.T) {
	clearEnvironment(t)
	noNix(t)

	// A port something else listens on.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	// serve is given a minute: one that starts instead of refusing runs
	// until then and returns no error, which fails the test.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	refused := func(name string, args ...string) string {
		t.Helper()
		_, stderr, err := runContext(t, ctx, args...)
		if err == nil {
			t.Errorf("serve %q: no error\n%s", args, stderr)
			return ""
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("serve %q: error %q does not name %s", args, err, name)
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("serve %q: error %q is more than one line", args, err)
		}
		if strings.Contains(stderr, "msg=serving") {
			t.Errorf("serve %q served before it refused:\n%s", args, stderr)
		}
		return err.Error()
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"--prefix", []string{"--prefix", "a@b/"}},
		{"--server", []string{"--server", "nonsense"}},
		{"--cache-size", []string{"--cache-size", "0"}},
		{"--cache-size", []string{"--cache-size", "lots"}},
		{"--listen", []string{"--listen", busy.Addr().String()}},
		{"--listen", []string{"--listen", "nowhere"}},
	} {
		dir := t.TempDir()
		msg := refused(tc.name, serveArgs(dir, tc.args...)...)
		if !strings.Contains(msg, tc.args[1]) {
			t.Errorf("serve %q: error %q does not show the value", tc.args, msg)
		}
	}

	// Without options it is the server that is missing.
	refused("--server", "serve")

	// A key file that holds no key is named and left as it is.
	for _, file := range []string{"client.key", "signing.key"} {
		dir := t.TempDir()
		path := filepath.Join(dir, "config", file)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("not a key\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		msg := refused("--config-dir", serveArgs(dir)...)
		if !strings.Contains(msg, path) {
			t.Errorf("a broken %s: error %q does not name the file", file, msg)
		}
		if got, err := os.ReadFile(path); err != nil || string(got) != "not a key\n" {
			t.Errorf("a broken %s was changed: %q, %v", file, got, err)
		}
	}

	// A cache directory that cannot be one.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cache"), []byte("a file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if msg := refused("--cache-dir", serveArgs(dir)...); !strings.Contains(msg, filepath.Join(dir, "cache")) {
		t.Errorf("a cache directory that is a file: error %q does not name it", msg)
	}
}

// check needs the address and the configuration directory and nothing else,
// and no nix either: it then prints the lines to add.
func TestCheckNeedsNoServer(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	dir := filepath.Join(t.TempDir(), "config")

	stdout, stderr, err := run(t, "check", "--listen", "127.0.0.1:1", "--config-dir", dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if stderr != "" {
		t.Errorf("check wrote to standard error: %q", stderr)
	}
	// The key it printed is the one it made and kept.
	key, err := narinfo.LoadOrCreateSigningKey(filepath.Join(dir, "signing.key"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key.Public(), "narrenhaus-") {
		t.Errorf("the signing key is named %q", key.Public())
	}
	for _, want := range []string{
		"    extra-substituters = http://127.0.0.1:1\n",
		"    extra-trusted-public-keys = " + key.Public() + "\n",
		"    fallback = true\n",
		"nix copy --to 'http://127.0.0.1:1?compression=none'",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("check does not print %q:\n%s", want, stdout)
		}
	}
	if !strings.HasSuffix(stdout, "\n") || strings.HasSuffix(stdout, "\n\n") {
		t.Errorf("check's output does not end with one line feed:\n%q", stdout)
	}
	// It made that key and nothing else: the key shown to the server is
	// serve's business.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "signing.key" {
		t.Errorf("check left %v in the configuration directory, want signing.key alone", entries)
	}

	// A second run says the same.
	again, _, err := run(t, "check", "--listen", "127.0.0.1:1", "--config-dir", dir)
	if err != nil || again != stdout {
		t.Errorf("a second check: %v\n%s\nwant what the first printed:\n%s", err, again, stdout)
	}

	// The options of serve are taken and not looked at.
	other, _, err := run(t, "check", "--listen", "127.0.0.1:1", "--config-dir", dir,
		"--server", "nonsense", "--prefix", "a@b/", "--cache-size", "0", "--cache-dir", "")
	if err != nil || other != stdout {
		t.Errorf("check with options of serve that are wrong: %v\n%s", err, other)
	}

	// With the defaults the key is under the user's configuration
	// directory.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	stdout, _, err = run(t, "check")
	if err != nil {
		t.Fatalf("check with the defaults: %v", err)
	}
	config, err := os.UserConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(config, home) {
		t.Fatalf("the user's configuration directory is %s, not under the test's home", config)
	}
	key, err = narinfo.LoadOrCreateSigningKey(filepath.Join(config, "narrenhaus", "signing.key"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout, "extra-substituters = http://127.0.0.1:8484\n") || !strings.Contains(stdout, key.Public()) {
		t.Errorf("check with the defaults:\n%s", stdout)
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"--listen", []string{"--listen", "nowhere"}},
		{"--config-dir", []string{"--config-dir", ""}},
	} {
		_, _, err := run(t, append([]string{"check"}, tc.args...)...)
		if err == nil || !strings.Contains(err.Error(), tc.name) {
			t.Errorf("check %q: error %v, want one naming %s", tc.args, err, tc.name)
		}
	}
	// A signing key that is no key is an error that names the file.
	bad := filepath.Join(t.TempDir(), "signing.key")
	if err := os.WriteFile(bad, []byte("not a key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err = run(t, "check", "--config-dir", filepath.Dir(bad))
	if err == nil || !strings.Contains(err.Error(), bad) || !strings.Contains(err.Error(), "--config-dir") {
		t.Errorf("check with a broken signing key: error %v, want one naming --config-dir and %s", err, bad)
	}
}

// keyFiles reads the two key files under the configuration directory.
func keyFiles(t *testing.T, config string) (client, signing []byte) {
	t.Helper()
	for _, f := range []struct {
		name string
		into *[]byte
	}{{"client.key", &client}, {"signing.key", &signing}} {
		path := filepath.Join(config, f.name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Errorf("%s has mode %o, want 600", f.name, mode)
		}
		if *f.into, err = os.ReadFile(path); err != nil {
			t.Fatal(err)
		}
	}
	return client, signing
}

func TestServeCreatesBothKeys(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	dir := t.TempDir()
	config := filepath.Join(dir, "config")

	d := startDaemon(t, func(ctx context.Context, stderr io.Writer) error {
		return newApp(io.Discard, stderr).RunContext(ctx, append([]string{"narrenhaus"}, serveArgs(dir, "--priority", "45")...))
	})
	client, signing := keyFiles(t, config)
	if info, err := os.Stat(config); err != nil || info.Mode().Perm() != 0o700 {
		t.Errorf("the configuration directory: %v, %v, want mode 700", info, err)
	}
	// Both are keys of their kind.
	sk, err := node.LoadOrCreateKey(filepath.Join(config, "client.key"))
	if err != nil {
		t.Errorf("client.key: %v", err)
	}
	if sk.Public().EndpointID().String() == testServer {
		t.Errorf("client.key is the server's key")
	}
	key, err := narinfo.LoadOrCreateSigningKey(filepath.Join(config, "signing.key"))
	if err != nil {
		t.Fatalf("signing.key: %v", err)
	}

	// Standard error: the check's advice for the address it serves at and
	// for its key, then that it serves.
	log := d.log.String()
	for _, want := range []string{
		"    extra-substituters = " + d.url + "\n",
		"    extra-trusted-public-keys = " + key.Public() + "\n",
		"    fallback = true\n",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("serve does not print %q:\n%s", want, log)
		}
	}
	if advice, line := strings.Index(log, "extra-substituters"), strings.Index(log, "msg=serving"); advice < 0 || line < advice {
		t.Errorf("serve says that it serves before the advice:\n%s", log)
	}
	if strings.HasSuffix(d.url, ":0") {
		t.Errorf("serve says it serves at %s, not at the port it got", d.url)
	}

	// It serves, with the priority it was given, and without the server:
	// nothing has asked for a path.
	if got := string(d.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil)); got != "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 45\n" {
		t.Errorf("nix-cache-info is %q", got)
	}

	if err := d.stop(); err != nil {
		t.Errorf("serve returned %v when it was stopped", err)
	}
	if _, _, err := d.do(http.MethodGet, "/nix-cache-info", nil); err == nil {
		t.Errorf("serve still answers after it returned")
	}
	if !strings.Contains(d.log.String(), "msg=stopped") {
		t.Errorf("serve does not say that it stopped:\n%s", d.log.String())
	}

	// A second start finds the keys and keeps them.
	d = startDaemon(t, func(ctx context.Context, stderr io.Writer) error {
		return newApp(io.Discard, stderr).RunContext(ctx, append([]string{"narrenhaus"}, serveArgs(dir)...))
	})
	clientAgain, signingAgain := keyFiles(t, config)
	if !bytes.Equal(client, clientAgain) || !bytes.Equal(signing, signingAgain) {
		t.Errorf("a second start changed a key")
	}
	if !strings.Contains(d.log.String(), key.Public()) {
		t.Errorf("the second start advises another key:\n%s", d.log.String())
	}
	if err := d.stop(); err != nil {
		t.Errorf("serve returned %v when it was stopped", err)
	}
}

// asCommand is the variable that makes the test binary the command: with
// it set, the binary runs main on its arguments and no test.
const asCommand = "NARRENHAUS_TEST_AS_COMMAND"

func TestMain(m *testing.M) {
	if os.Getenv(asCommand) != "" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// The first signal lets the requests that run finish; a second one ends
// the process at once, however it was started. A shell starts the
// background jobs of a script with SIGINT ignored, and a process that
// gives its signals back to the system after the first then ignores the
// second.
func TestSecondSignalEndsItAtOnce(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh to start the command as a shell does")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, script string
	}{
		{"in the foreground", `exec "$0" "$@"`},
		{"in the background of a script", `trap '' INT; exec "$0" "$@"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &syncBuffer{}
			cmd := exec.Command("/bin/sh", append([]string{"-c", tc.script, self}, serveArgs(t.TempDir())...)...)
			cmd.Env = []string{asCommand + "=1", "PATH="} // and so no nix
			cmd.Stderr = log
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			running := true
			t.Cleanup(func() {
				if running {
					cmd.Process.Kill()
					<-exited
				}
			})
			said := func(what string, re *regexp.Regexp) []string {
				t.Helper()
				for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
					if m := re.FindStringSubmatch(log.String()); m != nil {
						return m
					}
				}
				t.Fatalf("the command never said %s:\n%s", what, log.String())
				return nil
			}
			at, err := url.Parse(said("that it serves", servingLine)[1])
			if err != nil {
				t.Fatal(err)
			}

			// A request that does not end: an upload whose body never
			// comes. The handler has it once it asks for the body.
			conn, err := net.Dial("tcp", at.Host)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(30 * time.Second))
			fmt.Fprintf(conn, "PUT /%s.narinfo HTTP/1.1\r\nHost: %s\r\nContent-Length: 1000\r\nExpect: 100-continue\r\n\r\n",
				strings.Repeat("0", 32), at.Host)
			if line, err := bufio.NewReader(conn).ReadString('\n'); err != nil || !strings.Contains(line, "100 Continue") {
				t.Fatalf("the upload was answered %q, %v before its body, want to be asked for the body", line, err)
			}

			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			said("that it is stopping", regexp.MustCompile(`msg="stopping: `))
			select {
			case err := <-exited:
				running = false
				t.Fatalf("the command ended with %v while a request was running:\n%s", err, log.String())
			case <-time.After(200 * time.Millisecond):
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-exited:
				running = false
				if err == nil {
					t.Errorf("the command ended as if nothing was wrong:\n%s", log.String())
				}
			case <-time.After(3 * time.Second):
				t.Errorf("the command still runs three seconds after the second signal:\n%s", log.String())
			}
			if strings.Contains(log.String(), "msg=stopped") {
				t.Errorf("the command waited for its requests after the second signal:\n%s", log.String())
			}
		})
	}
}

// testPath is a small store path as Nix uploads it with ?compression=none.
type testPath struct {
	hash    string // of the store path
	archive []byte
	narHash string // in Nix's base 32
	narinfo []byte
}

func newTestPath(t *testing.T, body string) testPath {
	t.Helper()
	var buf bytes.Buffer
	w := nar.NewWriter(&buf)
	if err := w.Directory(""); err != nil {
		t.Fatal(err)
	}
	f, err := w.Regular("hello", false, uint64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(f, body); err != nil {
		t.Fatal(err)
	}
	if err := w.Leave(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p := testPath{archive: buf.Bytes()}
	sum := sha256.Sum256(p.archive)
	p.narHash = nix32.Encode(sum[:])
	of := sha256.Sum256([]byte("the store path of " + body))
	p.hash = nix32.Encode(of[:20])
	p.narinfo = fmt.Appendf(nil, "StorePath: /nix/store/%s-hello\nURL: nar/%s.nar\nCompression: none\n"+
		"FileHash: sha256:%s\nFileSize: %d\nNarHash: sha256:%s\nNarSize: %d\nReferences: \n",
		p.hash, p.narHash, p.narHash, len(p.archive), p.narHash, len(p.archive))
	return p
}

// testOptions are options of serve that are good, with directories under
// dir and a port the system chooses.
func testOptions(t *testing.T, dir string) options {
	t.Helper()
	id, err := irohkey.ParseEndpointID(testServer)
	if err != nil {
		t.Fatal(err)
	}
	return options{
		Server:    id,
		Prefix:    "acme/",
		Listen:    "127.0.0.1:0",
		CacheDir:  filepath.Join(dir, "cache"),
		CacheSize: 1 << 30,
		ConfigDir: filepath.Join(dir, "config"),
		Priority:  30,
	}
}

// hostOf is the machine of a test: the server is the one in this process,
// whatever ID the options name, and requests get grace to finish.
func hostOf(srv *testserver.Server, grace time.Duration) host {
	h := thisHost()
	h.connect = func(irohkey.SecretKey, irohkey.EndpointID, *slog.Logger) func(context.Context) (remote.Conn, error) {
		return srv.Dial()
	}
	h.grace = grace
	return h
}

// startServe runs serve itself, past the command line.
func startServe(t *testing.T, opts options, h host) *daemon {
	t.Helper()
	return startDaemon(t, func(ctx context.Context, stderr io.Writer) error {
		return serve(ctx, opts, stderr, h)
	})
}

// logged counts the lines of the daemon's log with the message msg.
func (d *daemon) logged(msg string) int {
	if strings.Contains(msg, " ") {
		msg = strconv.Quote(msg)
	}
	log := d.log.String()
	return strings.Count(log, " msg="+msg+" ") + strings.Count(log, " msg="+msg+"\n")
}

// serve is the whole cache: what is uploaded to it is on the server and
// comes back signed with its key, each with a line in the log; and the
// cache directory is its own while it runs and free afterwards.
func TestServeUploadsAndServes(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	dir := t.TempDir()
	opts := testOptions(t, dir)
	d := startServe(t, opts, hostOf(srv, 10*time.Second))
	p := newTestPath(t, "hello, world\n")

	if got := string(d.want(http.StatusOK, http.MethodGet, "/nix-cache-info", nil)); !strings.Contains(got, "Priority: 30\n") {
		t.Errorf("nix-cache-info is %q", got)
	}
	d.want(http.StatusNotFound, http.MethodGet, "/"+p.hash+".narinfo", nil)
	d.want(http.StatusNotFound, http.MethodHead, "/nar/"+p.narHash+".nar", nil)
	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	d.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo)
	if d.logged("content pushed") != 1 || d.logged("narinfo pushed") != 1 {
		t.Errorf("the log has no one line for each push:\n%s", d.log.String())
	}

	// Both references are on the server under the prefix.
	refs, err := srv.Client(t).List(context.Background(), "acme/")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range refs {
		names = append(names, r.Name)
	}
	slices.Sort(names)
	if want := []string{"acme/" + p.hash + ".narinfo", "acme/nar/" + p.narHash + ".nar"}; !slices.Equal(names, want) {
		t.Errorf("the server has the references %q, want %q", names, want)
	}

	// The narinfo comes back signed with the key of the configuration
	// directory.
	key, err := narinfo.LoadOrCreateSigningKey(filepath.Join(opts.ConfigDir, "signing.key"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := narinfo.Parse(d.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil))
	if err != nil {
		t.Fatal(err)
	}
	if want := key.Sign(info.Fingerprint()); !slices.Equal(info.Sigs, []string{want}) {
		t.Errorf("the narinfo is signed %q, want %q", info.Sigs, want)
	}
	if info.URL != "nar/"+p.narHash+".nar" || info.Compression != "none" {
		t.Errorf("the narinfo names the NAR as %q with compression %q", info.URL, info.Compression)
	}

	// The NAR, with one line in the log; what is not served has none.
	if d.logged("NAR served") != 0 {
		t.Errorf("the log says a NAR was served before one was asked for:\n%s", d.log.String())
	}
	if got := d.want(http.StatusOK, http.MethodGet, "/"+info.URL, nil); !bytes.Equal(got, p.archive) {
		t.Errorf("the NAR that came back is not the one sent")
	}
	d.want(http.StatusNotFound, http.MethodGet, "/nar/"+strings.Repeat("0", 52)+".nar", nil)
	d.want(http.StatusOK, http.MethodHead, "/nar/"+p.narHash+".nar", nil)
	if d.logged("NAR served") != 1 {
		t.Errorf("the log has no one line for the NAR served:\n%s", d.log.String())
	}
	if strings.Contains(d.log.String(), "level=DEBUG") {
		t.Errorf("the log has debug lines:\n%s", d.log.String())
	}

	// A second narrenhaus on the same cache directory is refused, and the
	// first goes on.
	second := opts
	second.ConfigDir = filepath.Join(dir, "config2")
	err = serve(context.Background(), second, io.Discard, hostOf(srv, time.Second))
	if err == nil || !strings.Contains(err.Error(), "--cache-dir") || !strings.Contains(err.Error(), opts.CacheDir) ||
		!errors.Is(err, packcache.ErrInUse) || strings.Contains(err.Error(), "\n") {
		t.Errorf("a second serve on the cache directory: error %v, want one line naming --cache-dir and the directory", err)
	}
	d.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)

	if err := d.stop(); err != nil {
		t.Errorf("serve returned %v when it was stopped", err)
	}

	// The directory belongs to the server and the prefix: another prefix
	// is refused, and so is another server.
	other := opts
	other.Prefix = "other/"
	err = serve(context.Background(), other, io.Discard, hostOf(srv, time.Second))
	if err == nil || !strings.Contains(err.Error(), "--cache-dir") || !strings.Contains(err.Error(), opts.CacheDir) ||
		!errors.Is(err, packcache.ErrOrigin) || strings.Contains(err.Error(), "\n") {
		t.Errorf("serve with another prefix on the cache directory: error %v, want one line naming --cache-dir and the directory", err)
	}
	cache, err := packcache.Open(opts.CacheDir, opts.CacheSize, packcache.Origin{Server: "another", Prefix: opts.Prefix})
	if !errors.Is(err, packcache.ErrOrigin) || !strings.Contains(err.Error(), testServer) {
		if err == nil {
			cache.Close()
		}
		t.Errorf("the cache directory opened for another server: %v, want a refusal that names %s", err, testServer)
	}

	// Started again, it serves what it has in the cache, and does not ask
	// the server for the NAR.
	srv.Stop()
	d = startServe(t, opts, hostOf(srv, time.Second))
	if got := d.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil); !bytes.Equal(got, p.archive) {
		t.Errorf("the NAR after a restart is not the one sent")
	}
}

// uploading waits until the daemon whose cache directory is cache has
// begun to stage an upload, which is when the upload's request has reached
// the handler.
func uploading(t *testing.T, cache string) {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if entries, err := os.ReadDir(filepath.Join(cache, "staging")); err == nil && len(entries) > 0 {
			return
		}
	}
	t.Fatalf("no upload was staged under %s", cache)
}

// A request that runs when the signal comes is let finish.
func TestServeLetsARunningRequestFinish(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	opts := testOptions(t, t.TempDir())
	d := startServe(t, opts, hostOf(srv, 30*time.Second))
	p := newTestPath(t, strings.Repeat("a request that is under way\n", 4000))

	// An upload that has sent its first half.
	body, feed := io.Pipe()
	type answer struct {
		status int
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		status, _, err := d.do(http.MethodPut, "/nar/"+p.narHash+".nar", body)
		answered <- answer{status, err}
	}()
	half := len(p.archive) / 2
	if _, err := feed.Write(p.archive[:half]); err != nil {
		t.Fatal(err)
	}
	uploading(t, opts.CacheDir)

	d.cancel()
	// serve waits for it, and takes no new connection meanwhile.
	time.Sleep(300 * time.Millisecond)
	if !d.running() {
		t.Fatalf("serve returned while a request was running:\n%s", d.log.String())
	}
	if _, _, err := d.do(http.MethodGet, "/nix-cache-info", nil); err == nil {
		t.Errorf("serve took a new connection after the signal")
	}
	select {
	case a := <-answered:
		t.Fatalf("the upload was answered %d, %v before its body ended", a.status, a.err)
	default:
	}

	if _, err := feed.Write(p.archive[half:]); err != nil {
		t.Fatal(err)
	}
	feed.Close()
	if a := <-answered; a.err != nil || a.status != http.StatusOK {
		t.Errorf("the upload that ran when serve was stopped: status %d, %v, want 200\n%s", a.status, a.err, d.log.String())
	}
	if err := d.wait(10 * time.Second); err != nil {
		t.Errorf("serve returned %v", err)
	}
}

// A request that does not finish in the time serve gives it is cut.
func TestServeCutsRequestsThatDoNotFinish(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	dir := t.TempDir()
	opts := testOptions(t, dir)
	d := startServe(t, opts, hostOf(srv, 200*time.Millisecond))
	p := newTestPath(t, strings.Repeat("a request that never ends\n", 4000))

	body, feed := io.Pipe()
	defer feed.Close()
	answered := make(chan error, 1)
	go func() {
		status, _, err := d.do(http.MethodPut, "/nar/"+p.narHash+".nar", body)
		if err == nil && status == http.StatusOK {
			err = errors.New("the upload was answered 200")
		} else {
			err = nil
		}
		answered <- err
	}()
	if _, err := feed.Write(p.archive[:len(p.archive)/2]); err != nil {
		t.Fatal(err)
	}
	uploading(t, opts.CacheDir)

	began := time.Now()
	if err := d.stop(); err != nil {
		t.Errorf("serve returned %v", err)
	}
	if took := time.Since(began); took < 200*time.Millisecond || took > 5*time.Second {
		t.Errorf("serve took %v to stop with a request that does not end, want a little over 200ms", took)
	}
	if d.logged("requests that were still running have been cut") != 1 {
		t.Errorf("serve does not say that it cut a request:\n%s", d.log.String())
	}
	// The request had ended when serve closed what it stood on, and had
	// taken back what it staged.
	if names := files(t, opts.CacheDir, "staging"); len(names) != 0 {
		t.Errorf("staging holds %v when serve has returned: the upload that was cut had not ended", names)
	}
	// Go's client returns only once it has let go of the body it was
	// sending.
	feed.Close()
	select {
	case err := <-answered:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Errorf("the request that was cut is still waiting")
	}

	// Nothing of it is left, and the directory is free.
	cache, err := packcache.Open(opts.CacheDir, opts.CacheSize, packcache.Origin{Server: testServer, Prefix: opts.Prefix})
	if err != nil {
		t.Fatalf("the cache directory after serve returned: %v", err)
	}
	defer cache.Close()
	if cache.Size() != 0 {
		t.Errorf("the cache holds %d bytes after an upload that was cut", cache.Size())
	}
}

// eventually waits until cond holds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

// files returns the names of what lies in the directory sub of a cache
// directory.
func files(t *testing.T, cache, sub string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(cache, sub))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// called sends a request to the cache at base that calls it host, with
// header as pairs of a name and a value, and reads the answer.
func called(t *testing.T, base, host, method, target string, body []byte, header ...string) (status int, got []byte) {
	t.Helper()
	var in io.Reader
	if body != nil {
		in = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, base+target, in)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatalf("%s %s as %s: %v", method, target, host, err)
	}
	defer res.Body.Close()
	if got, err = io.ReadAll(res.Body); err != nil {
		t.Fatalf("%s %s as %s: reading the answer: %v", method, target, host, err)
	}
	return res.StatusCode, got
}

// A page in a browser on the same machine reaches the port, under a name
// of its own that it has pointed at 127.0.0.1. It is not Nix: nothing it
// sends is taken, so nothing of it reaches the server, where a colleague's
// narrenhaus would sign it; and nothing is served to it.
func TestServeRefusesAWebPage(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	opts := testOptions(t, t.TempDir())
	d := startServe(t, opts, hostOf(srv, time.Second))
	planted := newTestPath(t, "#!/bin/sh\necho planted\n")
	const page = "attacker.example:8484"

	for _, put := range []struct {
		target string
		body   []byte
	}{
		{"/nar/" + planted.narHash + ".nar", planted.archive},
		{"/" + planted.hash + ".narinfo", planted.narinfo},
	} {
		status, body := called(t, d.url, page, http.MethodPut, put.target, put.body, "Origin", "http://"+page)
		if status != http.StatusForbidden || !strings.Contains(string(body), "attacker.example") {
			t.Errorf("PUT %s of a web page: status %d (%q), want 403 and the name that was refused", put.target, status, body)
		}
	}
	refs, err := srv.Client(t).List(context.Background(), "acme/")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("the server has %v after uploads of a web page", refs)
	}
	if names := files(t, opts.CacheDir, "staging"); len(names) != 0 {
		t.Errorf("staging holds %v after uploads of a web page", names)
	}

	// What Nix uploaded is not there for the page either.
	p := newTestPath(t, "hello, world\n")
	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	d.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo)
	for _, target := range []string{"/nix-cache-info", "/" + p.hash + ".narinfo", "/nar/" + p.narHash + ".nar"} {
		if status, body := called(t, d.url, page, http.MethodGet, target, nil); status != http.StatusNotFound {
			t.Errorf("GET %s of a web page: status %d (%q), want 404", target, status, body)
		}
	}
	if d.logged("NAR served") != 0 {
		t.Errorf("the log says a NAR was served to a web page:\n%s", d.log.String())
	}
	if got := d.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil); !bytes.Equal(got, p.archive) {
		t.Errorf("the NAR Nix gets is not the one sent")
	}
}

// serve answers to the host of --listen when that is a name, besides the
// addresses and localhost, and to no other name.
func TestServeAnswersToTheNameItListensAt(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	opts := testOptions(t, t.TempDir())
	opts.Listen = "build.example:0"
	h := thisHost()
	h.grace = time.Second
	// The name leads nowhere. The port is opened on loopback in its place.
	h.listen = func(address string) (net.Listener, error) {
		if address != opts.Listen {
			return nil, fmt.Errorf("serve listens at %q, the option says %q", address, opts.Listen)
		}
		return net.Listen("tcp", "127.0.0.1:0")
	}
	d := startServe(t, opts, h)
	at, err := url.Parse(d.url)
	if err != nil || at.Hostname() != "build.example" {
		t.Fatalf("serve says it serves at %s, want the name of --listen with the port it got (%v)", d.url, err)
	}
	base := "http://127.0.0.1:" + at.Port()
	for _, tc := range []struct {
		host   string
		status int
	}{
		{"build.example:" + at.Port(), http.StatusOK},
		{"Build.Example", http.StatusOK},
		{"127.0.0.1:" + at.Port(), http.StatusOK},
		{"localhost:" + at.Port(), http.StatusOK},
		{"other.example:" + at.Port(), http.StatusNotFound},
		{"build.example.attacker.example:" + at.Port(), http.StatusNotFound},
	} {
		if status, body := called(t, base, tc.host, http.MethodGet, "/nix-cache-info", nil); status != tc.status {
			t.Errorf("GET /nix-cache-info of a client that calls the cache %s: status %d (%q), want %d", tc.host, status, body, tc.status)
		}
	}
}

// failingBucket makes every upload to the server's bucket fail in a way
// that may pass, and returns how many files lay under the staging
// directory of cache when the first one failed.
func failingBucket(t *testing.T, srv *testserver.Server, cache string) (staged func() int) {
	t.Helper()
	var (
		mu   sync.Mutex
		seen = -1
	)
	srv.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		mu.Lock()
		if seen < 0 {
			entries, _ := os.ReadDir(filepath.Join(cache, "staging"))
			seen = len(entries)
		}
		mu.Unlock()
		http.Error(w, "the bucket is not well", http.StatusInternalServerError)
		return true
	})
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return seen
	}
}

// A NAR whose push failed for now stays staged for the upload that comes
// back for it, and not for ever: serve looks for such NARs on a timer and
// removes them once nothing has asked for the idle time.
func TestServeRemovesStagedNARsNobodyCameBackFor(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	opts := testOptions(t, t.TempDir())
	staged := failingBucket(t, srv, opts.CacheDir)
	h := hostOf(srv, time.Second)
	h.sweepEvery, h.idleExpiry = 10*time.Millisecond, 300*time.Millisecond
	d := startServe(t, opts, h)
	p := newTestPath(t, "a NAR whose narinfo never comes\n")

	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	eventually(t, "the push has failed for now", func() bool {
		return d.logged("a content push failed for now; its NAR stays staged") == 1
	})
	if staged() <= 0 {
		t.Fatalf("%d files were staged while the NAR was pushed, want some", staged())
	}
	eventually(t, "the staged NAR is removed", func() bool {
		return len(files(t, opts.CacheDir, "staging")) == 0
	})
	if d.logged("staged NARs whose pushes had failed were deleted: no upload came back for them") != 1 {
		t.Errorf("the log does not say that the staged NAR was deleted:\n%s", d.log.String())
	}
}

// The size of --cache-size is the cache's: with one below any pack,
// nothing stays in the cache once no request holds it.
func TestServeKeepsTheCacheUnderItsSize(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	opts := testOptions(t, t.TempDir())
	opts.CacheSize = 1
	d := startServe(t, opts, hostOf(srv, time.Second))
	p := newTestPath(t, strings.Repeat("more than one byte\n", 1000))

	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	d.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo)
	d.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	if got := d.want(http.StatusOK, http.MethodGet, "/nar/"+p.narHash+".nar", nil); !bytes.Equal(got, p.archive) {
		t.Errorf("the NAR that came back is not the one sent")
	}
	// A request lets go of its packs after its answer has gone out.
	eventually(t, "the cache holds no pack", func() bool {
		return len(files(t, opts.CacheDir, "packs")) == 0
	})
}

// A narinfo that comes after a restart finds no NAR: what was staged is
// gone with the process that staged it, and the answer says that the NAR
// has to be sent again.
func TestNarinfoAfterARestart(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	srv := testserver.Start(t, testserver.Options{})
	opts := testOptions(t, t.TempDir())
	staged := failingBucket(t, srv, opts.CacheDir)
	d := startServe(t, opts, hostOf(srv, time.Second))
	p := newTestPath(t, "a NAR that is staged when narrenhaus stops\n")

	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	eventually(t, "the push has failed for now", func() bool {
		return d.logged("a content push failed for now; its NAR stays staged") == 1
	})
	if staged() <= 0 || len(files(t, opts.CacheDir, "staging")) == 0 {
		t.Fatalf("no NAR is staged before the restart: %d files while it was pushed, %v now",
			staged(), files(t, opts.CacheDir, "staging"))
	}
	if err := d.stop(); err != nil {
		t.Fatalf("serve returned %v when it was stopped", err)
	}

	// The bucket is well again, and it is too late for that NAR.
	srv.Intercept(nil)
	d = startServe(t, opts, hostOf(srv, time.Second))
	if names := files(t, opts.CacheDir, "staging"); len(names) != 0 {
		t.Errorf("staging holds %v after a restart", names)
	}
	got := string(d.want(http.StatusBadRequest, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo))
	if !strings.Contains(got, p.narHash) || !strings.Contains(got, "no NAR") {
		t.Errorf("the 400 says %q, want it to name the NAR that was not received", got)
	}
	d.want(http.StatusNotFound, http.MethodHead, "/nar/"+p.narHash+".nar", nil)

	// Sent again, as Nix does when its copy is repeated, the path is taken.
	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	d.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo)
}

// The dial of the command itself, which every other test replaces: the
// server of --server is dialled, for jaccard-store's protocol, as the
// client whose key lies in the configuration directory.
//
// The server here is the test's own and not testserver's, because the
// command's dial needs its endpoint ID and address, and the test the
// server's record of who wrote a reference.
func TestServeDialsTheServerOfItsOptions(t *testing.T) {
	clearEnvironment(t)
	noNix(t)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	database, err := db.Open(filepath.Join(dir, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	js, err := server.New(server.Config{DB: database, Bucket: buckettest.New(t), Scratch: filepath.Join(dir, "scratch"), Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := node.LoadOrCreateKey(filepath.Join(dir, "server.key"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	ep, err := node.Bind(ctx, node.ServerConfig{Key: serverKey, ALPN: wire.ALPN, Local: true, Log: quiet})
	if err != nil {
		stop()
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		js.Serve(ctx, ep)
	}()
	t.Cleanup(func() {
		stop()
		<-served
		ep.Shutdown(context.Background())
	})

	opts := testOptions(t, t.TempDir())
	opts.Server = serverKey.Public().EndpointID()
	h := thisHost()
	h.connect = dialServer(ep.LocalAddr())
	h.grace = time.Second
	d := startServe(t, opts, h)
	p := newTestPath(t, "over the dial of the command\n")
	d.want(http.StatusOK, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
	d.want(http.StatusOK, http.MethodPut, "/"+p.hash+".narinfo", p.narinfo)

	// The server knows who wrote the references: the key that serve made.
	client, err := node.LoadOrCreateKey(filepath.Join(opts.ConfigDir, "client.key"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"acme/" + p.hash + ".narinfo", "acme/nar/" + p.narHash + ".nar"} {
		ref, err := database.Ref(context.Background(), name)
		if err != nil {
			t.Fatalf("the server's reference %s: %v", name, err)
		}
		if want := client.Public().EndpointID().String(); ref.UpdatedBy != want {
			t.Errorf("the server has %s as written by %s, want the client key of the configuration directory, %s", name, ref.UpdatedBy, want)
		}
	}
	if err := d.stop(); err != nil {
		t.Errorf("serve returned %v when it was stopped", err)
	}

	// With another ID in --server nobody answers at the same address: the
	// server there is not the one that was asked for.
	other := testOptions(t, t.TempDir())
	d = startServe(t, other, h)
	d.want(http.StatusBadGateway, http.MethodPut, "/nar/"+p.narHash+".nar", p.archive)
}

// thisHost keeps the times the design names: ten seconds for requests that
// run when serve is told to stop, a look at the staged uploads every
// minute, and the uploader's own idle time.
func TestThisHostKeepsTheTimesOfTheDesign(t *testing.T) {
	h := thisHost()
	if h.grace != 10*time.Second {
		t.Errorf("running requests get %v to finish, want 10s", h.grace)
	}
	if h.sweepEvery != time.Minute {
		t.Errorf("staged uploads are looked at every %v, want every minute", h.sweepEvery)
	}
	if h.idleExpiry != 0 {
		t.Errorf("the idle time is %v, want it left to the uploader", h.idleExpiry)
	}
}

func TestAdvertised(t *testing.T) {
	bound := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4711}
	for _, tc := range []struct{ listen, want string }{
		{"127.0.0.1:8484", "127.0.0.1:8484"},
		{"127.0.0.1:0", "127.0.0.1:4711"},
		{":0", ":4711"},
		{"[::1]:0", "[::1]:4711"},
		{"localhost:00", "localhost:4711"},
		{"0.0.0.0:8484", "0.0.0.0:8484"},
	} {
		if got := advertised(tc.listen, bound); got != tc.want {
			t.Errorf("advertised(%q) = %q, want %q", tc.listen, got, tc.want)
		}
	}
}
