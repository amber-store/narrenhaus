package main

import (
	"bytes"
	"encoding/base32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amber-store/core/reference"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/urfave/cli/v2"
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
func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range optionVariables {
		t.Setenv(name, "") // registers the restore
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
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

func TestServeValidatesItsOptions(t *testing.T) {
	clearEnvironment(t)
	dir := t.TempDir()
	good := []string{"serve", "--server", testServer, "--prefix", "acme/", "--cache-dir", dir, "--config-dir", dir}

	_, _, err := run(t, good...)
	if err == nil || err.Error() != "not implemented" {
		t.Fatalf("serve with good options: error %v, want \"not implemented\"", err)
	}

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"--prefix", []string{"--prefix", "a@b/"}},
		{"--server", []string{"--server", "nonsense"}},
		{"--cache-size", []string{"--cache-size", "0"}},
		{"--cache-size", []string{"--cache-size", "lots"}},
	} {
		_, _, err := run(t, append(append([]string{}, good...), tc.args...)...)
		if err == nil || !strings.Contains(err.Error(), tc.name) {
			t.Errorf("serve %q: error %v, want one naming %s", tc.args, err, tc.name)
		}
	}

	_, _, err = run(t, "serve")
	if err == nil || err.Error() == "not implemented" {
		t.Errorf("serve without options: error %v, want one about a missing option", err)
	}
}

// check needs the address and the configuration directory and nothing else.
func TestCheckNeedsNoServer(t *testing.T) {
	clearEnvironment(t)
	_, _, err := run(t, "check", "--listen", "127.0.0.1:1", "--config-dir", t.TempDir())
	if err == nil || err.Error() != "not implemented" {
		t.Fatalf("check: error %v, want \"not implemented\"", err)
	}
	_, _, err = run(t, "check")
	if err == nil || err.Error() != "not implemented" {
		t.Fatalf("check with the defaults: error %v, want \"not implemented\"", err)
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
}
