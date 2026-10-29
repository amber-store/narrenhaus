package nixconf

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	testListen = "127.0.0.1:8484"
	testURL    = "http://127.0.0.1:8484"
	testHome   = "/home/alice"
	userConf   = "/home/alice/.config/nix/nix.conf"

	substituterLine = "extra-substituters = http://127.0.0.1:8484"
	fallbackTrue    = "fallback = true"
	uploadLine      = "nix copy --to 'http://127.0.0.1:8484?compression=none' <path>"
	stallNote       = "--option stalled-download-timeout <seconds>"
	nixosKey        = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
)

// testKey is a public key as narrenhaus names its own: the RFC 8032 test
// vector under the name made of its first eight hex digits.
var testKey = "narrenhaus-d75a9801:" + base64.StdEncoding.EncodeToString(mustHex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"))

// otherKey has testKey's name and another key.
var otherKey = "narrenhaus-d75a9801:" + base64.StdEncoding.EncodeToString(mustHex("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c"))

var keyLine = "extra-trusted-public-keys = " + testKey

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// values are the three settings the check reads.
type values struct {
	substituters []string
	keys         []string
	fallback     bool
}

// stock is what the captured configuration holds: a machine that has never
// heard of narrenhaus.
func stock() values {
	return values{
		substituters: []string{"https://cache.nixos.org/"},
		keys:         []string{nixosKey},
	}
}

// ready is a configuration with everything narrenhaus asks for.
func ready() values {
	v := stock()
	v.substituters = append(v.substituters, testURL)
	v.keys = append(v.keys, testKey)
	v.fallback = true
	return v
}

// configJSON is the captured output of `nix config show --json` with the
// three settings changed to v. Everything else stays as Nix printed it.
func configJSON(t *testing.T, v values) []byte {
	t.Helper()
	captured, err := os.ReadFile("testdata/config-show.json")
	if err != nil {
		t.Fatal(err)
	}
	var all map[string]map[string]json.RawMessage
	if err := json.Unmarshal(captured, &all); err != nil {
		t.Fatal(err)
	}
	set := func(name string, value any) {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if all[name] == nil {
			t.Fatalf("the captured configuration has no setting %q", name)
		}
		all[name]["value"] = raw
	}
	set("substituters", v.substituters)
	set("trusted-public-keys", v.keys)
	set("fallback", v.fallback)
	out, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// readTestdata reads a captured output of nix:
//
//   - config-show.json: `nix --extra-experimental-features nix-command
//     config show --json` of Nix 2.35.4, cut down to the settings the check
//     reads and a few others (each kept whole) and indented;
//   - store-info.json: `nix ... store info --json` of the same Nix;
//   - store-info-2.26.json: the same command's answer from Nix 2.26.1, which
//     writes the field as a number.
func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// call is one run of nix the check asked for.
type call struct {
	env  []string
	args []string
}

// machine is a fake of what the check reads.
type machine struct {
	t *testing.T

	full   values // the user's view: system file, user file and NIX_CONFIG
	system values // the daemon's view: the system file alone

	info    []byte // answer of `nix store info --json`
	infoErr error

	nixErr      error // every run of nix fails with this
	noConfig    bool  // `config show` is not a command (older Nix)
	fullOutput  []byte
	systemErr   error             // reading the daemon's view fails
	files       map[string]string // ReadFile
	links       map[string]string // Readlink
	calls       []call
	readFileErr error
}

// trustedMachine is a stock machine whose daemon trusts the user, with a
// plain /etc/nix/nix.conf.
func trustedMachine(t *testing.T) *machine {
	return &machine{
		t:      t,
		full:   stock(),
		system: stock(),
		info:   readTestdata(t, "store-info.json"),
		files: map[string]string{
			"/etc/nix/nix.conf": "build-users-group = nixbld\nexperimental-features = nix-command flakes\n",
		},
	}
}

// untrustedMachine is the same with a daemon that does not trust the user.
func untrustedMachine(t *testing.T) *machine {
	m := trustedMachine(t)
	m.info = []byte(`{"trusted":false,"url":"daemon","version":"2.35.4"}` + "\n")
	return m
}

func (m *machine) env() Env {
	return Env{
		Run: m.run,
		ReadFile: func(path string) ([]byte, error) {
			if m.readFileErr != nil {
				return nil, m.readFileErr
			}
			s, ok := m.files[path]
			if !ok {
				return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
			}
			return []byte(s), nil
		},
		Readlink: func(path string) (string, error) {
			s, ok := m.links[path]
			if !ok {
				// What readlink(2) answers for a file that is not a link.
				return "", &os.PathError{Op: "readlink", Path: path, Err: syscall.EINVAL}
			}
			return s, nil
		},
		Home: testHome,
	}
}

func (m *machine) run(_ context.Context, env []string, args ...string) ([]byte, error) {
	m.t.Helper()
	m.calls = append(m.calls, call{env: slices.Clone(env), args: slices.Clone(args)})
	if m.nixErr != nil {
		return nil, m.nixErr
	}
	if len(args) < 2 || args[0] != "--extra-experimental-features" || args[1] != "nix-command" {
		m.t.Errorf("nix was run without --extra-experimental-features nix-command: %q", args)
		return nil, errors.New("experimental feature 'nix-command' is disabled")
	}
	daemonView, err := m.daemonView(env)
	if err != nil {
		return nil, err
	}
	switch strings.Join(args[2:], " ") {
	case "config show --json":
		if m.noConfig {
			return nil, errors.New("error: 'config' is not a recognised command")
		}
		return m.config(daemonView)
	case "show-config --json":
		return m.config(daemonView)
	case "store info --json":
		if daemonView {
			m.t.Errorf("store info was run with a changed environment: %q", env)
		}
		return m.info, m.infoErr
	}
	m.t.Errorf("unexpected nix command: %q", args)
	return nil, errors.New("unexpected command")
}

// daemonView reports whether env is the one that makes nix print the
// daemon's view. Any other change of the environment is an error.
func (m *machine) daemonView(env []string) (bool, error) {
	if len(env) == 0 {
		return false, nil
	}
	sorted := slices.Sorted(slices.Values(env))
	if !slices.Equal(sorted, []string{"NIX_CONFIG", "NIX_USER_CONF_FILES="}) {
		m.t.Errorf("nix was run with the environment changes %q; want NIX_USER_CONF_FILES= and NIX_CONFIG removed", env)
		return false, errors.New("unexpected environment")
	}
	return true, nil
}

func (m *machine) config(daemonView bool) ([]byte, error) {
	if daemonView {
		if m.systemErr != nil {
			return nil, m.systemErr
		}
		return configJSON(m.t, m.system), nil
	}
	if m.fullOutput != nil {
		return m.fullOutput, nil
	}
	return configJSON(m.t, m.full), nil
}

// ran reports whether nix was asked for command (the arguments after the
// feature flag) with or without the daemon's environment.
func (m *machine) ran(command string, daemonView bool) bool {
	for _, c := range m.calls {
		if len(c.args) >= 2 && strings.Join(c.args[2:], " ") == command && (len(c.env) > 0) == daemonView {
			return true
		}
	}
	return false
}

func check(m *machine) Report {
	return Check(context.Background(), m.env(), Input{Listen: testListen, PublicKey: testKey})
}

func wantContains(t *testing.T, text string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(text, p) {
			t.Errorf("the text lacks %q:\n%s", p, text)
		}
	}
}

func wantLacks(t *testing.T, text string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if strings.Contains(text, p) {
			t.Errorf("the text contains %q:\n%s", p, text)
		}
	}
}

// status is the text before the upload hint.
func status(t *testing.T, text string) string {
	t.Helper()
	i := strings.Index(text, "To upload")
	if i < 0 {
		t.Fatalf("no upload hint in:\n%s", text)
	}
	return strings.TrimSpace(text[:i])
}

func TestConfigured(t *testing.T) {
	// The daemon trusts the user and the lines are in the user's file: the
	// system view does not have them.
	m := trustedMachine(t)
	m.full = ready()
	r := check(m)
	if !r.Configured {
		t.Errorf("not configured:\n%s", r.Text)
	}
	st := status(t, r.Text)
	if strings.Contains(st, "\n") {
		t.Errorf("the status is more than one line:\n%s", st)
	}
	wantContains(t, st, testURL)
	wantLacks(t, r.Text, "extra-substituters", "extra-trusted-public-keys", fallbackTrue, "restart", "/etc/nix")
	if !strings.HasSuffix(r.Text, "\n") || strings.HasSuffix(r.Text, "\n\n") {
		t.Errorf("the text does not end with exactly one newline: %q", r.Text)
	}
}

func TestConfiguredInSystemFileSaysRestart(t *testing.T) {
	t.Run("untrusted user", func(t *testing.T) {
		m := untrustedMachine(t)
		m.full = ready()
		m.system = ready()
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
		wantContains(t, r.Text, "/etc/nix", "restarted")
		wantLacks(t, r.Text, "extra-substituters")
	})
	t.Run("trusted user", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		m.system = ready()
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
		wantContains(t, r.Text, "/etc/nix", "restarted")
	})
	t.Run("only the substituter is in the system file", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		m.system.substituters = append(m.system.substituters, testURL)
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
		wantContains(t, r.Text, "/etc/nix", "restarted")
	})
	t.Run("the system view cannot be read", func(t *testing.T) {
		// Where the lines are is then unknown: the note is conditional.
		m := trustedMachine(t)
		m.full = ready()
		m.systemErr = errors.New("exit status 1")
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
		wantContains(t, r.Text, "If ", "/etc/nix", "restarted")
	})
}

func TestMissingEverythingTrustedUser(t *testing.T) {
	m := trustedMachine(t)
	r := check(m)
	if r.Configured {
		t.Error("configured")
	}
	wantContains(t, r.Text, substituterLine, keyLine, fallbackTrue, userConf)
	wantLacks(t, r.Text, "/etc/nix", "restart the Nix daemon", "restarted", "nix.settings")
	// The sentence on what fallback is for and what it changes.
	wantContains(t, r.Text, "when narrenhaus is not running", "failed download from any cache")
	// The three lines in the order of the specification, on lines of their own.
	var got []string
	for line := range strings.SplitSeq(r.Text, "\n") {
		switch line = strings.TrimSpace(line); line {
		case substituterLine, keyLine, fallbackTrue:
			got = append(got, line)
		}
	}
	if want := []string{substituterLine, keyLine, fallbackTrue}; !slices.Equal(got, want) {
		t.Errorf("configuration lines: got %q, want %q", got, want)
	}
	// A trusted user's lines are judged by the user's view alone.
	if m.ran("config show --json", true) {
		t.Error("the daemon's view was read for a trusted user who is not configured")
	}
}

func TestUntrustedPlainNixConf(t *testing.T) {
	m := untrustedMachine(t)
	r := check(m)
	if r.Configured {
		t.Error("configured")
	}
	wantContains(t, r.Text, substituterLine, keyLine, fallbackTrue, "/etc/nix/nix.conf", "restart the Nix daemon")
	wantLacks(t, r.Text, "nix.custom.conf", "nix.settings", "do not count")
	// The two lines the daemon must see come under /etc/nix/nix.conf; the
	// fallback line is offered the user's file, after them.
	system := strings.Index(r.Text, "/etc/nix/nix.conf")
	sub := strings.Index(r.Text, substituterLine)
	key := strings.Index(r.Text, keyLine)
	user := strings.Index(r.Text, userConf)
	fb := strings.Index(r.Text, fallbackTrue)
	if !(system < sub && sub < key && key < user && user < fb) {
		t.Errorf("order of the advice is wrong (system file %d, substituter %d, key %d, user file %d, fallback %d):\n%s", system, sub, key, user, fb, r.Text)
	}
}

func TestGeneratedNixConf(t *testing.T) {
	for _, target := range []string{
		"/etc/static/nix/nix.conf",
		"/nix/store/0c0v49dslq0ihrnpn1h41qc3zsp7ra8b-nix.conf",
	} {
		t.Run(target, func(t *testing.T) {
			m := untrustedMachine(t)
			m.links = map[string]string{"/etc/nix/nix.conf": target}
			r := check(m)
			if r.Configured {
				t.Error("configured")
			}
			wantContains(t, r.Text,
				`nix.settings.extra-substituters = [ "http://127.0.0.1:8484" ];`,
				`nix.settings.extra-trusted-public-keys = [ "`+testKey+`" ];`,
				"generated",
				fallbackTrue, userConf,
			)
			// Nothing tells the user to edit the generated file.
			wantLacks(t, r.Text, substituterLine, keyLine)
		})
	}
	t.Run("a link elsewhere is a plain file", func(t *testing.T) {
		m := untrustedMachine(t)
		m.links = map[string]string{"/etc/nix/nix.conf": "/usr/local/etc/nix.conf"}
		r := check(m)
		wantContains(t, r.Text, substituterLine, keyLine, "/etc/nix/nix.conf")
		wantLacks(t, r.Text, "nix.settings")
	})
	t.Run("a trusted user is sent to the user file all the same", func(t *testing.T) {
		m := trustedMachine(t)
		m.links = map[string]string{"/etc/nix/nix.conf": "/etc/static/nix/nix.conf"}
		r := check(m)
		wantContains(t, r.Text, substituterLine, keyLine, userConf)
		wantLacks(t, r.Text, "nix.settings")
	})
}

func TestDeterminate(t *testing.T) {
	custom := []string{
		"include nix.custom.conf\n",
		"!include nix.custom.conf\n",
		"!include /etc/nix/nix.custom.conf\n",
		"build-users-group = nixbld\n  !include   nix.custom.conf   # the user's settings\nsandbox = true\n",
		"!include ./nix.custom.conf",
	}
	for _, conf := range custom {
		t.Run(conf, func(t *testing.T) {
			m := untrustedMachine(t)
			m.files["/etc/nix/nix.conf"] = conf
			r := check(m)
			if r.Configured {
				t.Error("configured")
			}
			wantContains(t, r.Text, substituterLine, keyLine, "/etc/nix/nix.custom.conf", "restart the Nix daemon")
			wantLacks(t, r.Text, "/etc/nix/nix.conf", "nix.settings")
		})
	}
	plain := []string{
		"# include nix.custom.conf\n",
		"sandbox = true # !include nix.custom.conf\n",
		"include other.conf\n",
		"!include nix.custom.conf.bak\n",
		"extra-substituters = include nix.custom.conf\n",
		"",
	}
	for _, conf := range plain {
		t.Run("not "+conf, func(t *testing.T) {
			m := untrustedMachine(t)
			m.files["/etc/nix/nix.conf"] = conf
			r := check(m)
			wantContains(t, r.Text, substituterLine, keyLine, "/etc/nix/nix.conf")
			wantLacks(t, r.Text, "nix.custom.conf")
		})
	}
	t.Run("nix.conf cannot be read", func(t *testing.T) {
		m := untrustedMachine(t)
		m.readFileErr = &os.PathError{Op: "open", Path: "/etc/nix/nix.conf", Err: syscall.EACCES}
		r := check(m)
		wantContains(t, r.Text, substituterLine, keyLine, "/etc/nix/nix.conf")
		wantLacks(t, r.Text, "nix.custom.conf")
	})
	t.Run("a generated nix.conf comes first", func(t *testing.T) {
		m := untrustedMachine(t)
		m.files["/etc/nix/nix.conf"] = "!include nix.custom.conf\n"
		m.links = map[string]string{"/etc/nix/nix.conf": "/etc/static/nix/nix.conf"}
		r := check(m)
		wantContains(t, r.Text, "nix.settings.extra-substituters")
		wantLacks(t, r.Text, "nix.custom.conf")
	})
}

func TestUntrustedUserFileIsNotCounted(t *testing.T) {
	m := untrustedMachine(t)
	m.full = ready() // the lines are in ~/.config/nix/nix.conf only
	r := check(m)
	if r.Configured {
		t.Errorf("configured by lines the daemon ignores:\n%s", r.Text)
	}
	wantContains(t, r.Text, substituterLine, keyLine, "/etc/nix/nix.conf", "user's own configuration do not count")
	// fallback is honoured from the user's view, so that line is not missing.
	wantLacks(t, r.Text, fallbackTrue)
	if !m.ran("config show --json", false) || !m.ran("config show --json", true) {
		t.Errorf("want the configuration read in both views, got %+v", m.calls)
	}

	t.Run("fallback in the system view alone does not count", func(t *testing.T) {
		// NIX_CONFIG or the user's file can turn off what the system file
		// turns on, and the client's value is the one the daemon gets.
		m := untrustedMachine(t)
		m.system = ready()
		m.full = ready()
		m.full.fallback = false
		r := check(m)
		if r.Configured {
			t.Errorf("configured with fallback off in the user's view:\n%s", r.Text)
		}
		wantContains(t, r.Text, fallbackTrue)
		wantLacks(t, r.Text, "extra-substituters", "extra-trusted-public-keys")
	})
}

func TestFallbackOff(t *testing.T) {
	for name, m := range map[string]*machine{
		"trusted":   trustedMachine(t),
		"untrusted": untrustedMachine(t),
	} {
		t.Run(name, func(t *testing.T) {
			m.full = ready()
			m.full.fallback = false
			m.system = m.full
			r := check(m)
			if r.Configured {
				t.Errorf("configured with fallback off:\n%s", r.Text)
			}
			wantContains(t, r.Text, fallbackTrue, userConf, "when narrenhaus is not running", "failed download from any cache")
			wantLacks(t, r.Text, "extra-substituters", "extra-trusted-public-keys", "restart the Nix daemon")
		})
	}
}

func TestTrustedEncodings(t *testing.T) {
	old := readTestdata(t, "store-info-2.26.json")
	for _, tc := range []struct {
		name    string
		info    string
		err     error
		trusted bool
	}{
		{name: "true", info: string(readTestdata(t, "store-info.json")), trusted: true},
		{name: "1", info: string(old), trusted: true},
		{name: "false", info: `{"trusted":false,"url":"daemon","version":"2.35.4"}`},
		{name: "0", info: `{"trusted":0,"url":"daemon","version":"2.26.1"}`},
		{name: "missing field", info: `{"url":"daemon","version":"2.35.4"}`},
		{name: "null", info: `{"trusted":null,"url":"daemon"}`},
		{name: "a string", info: `{"trusted":"true","url":"daemon"}`},
		{name: "not JSON", info: `warning: something`},
		{name: "empty", info: ``},
		{name: "command failure", info: `{"trusted":true}`, err: errors.New("exit status 1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := trustedMachine(t)
			m.info, m.infoErr = []byte(tc.info), tc.err
			r := check(m)
			if r.Configured {
				t.Error("configured")
			}
			wantContains(t, r.Text, substituterLine, keyLine)
			if tc.trusted {
				wantContains(t, r.Text, userConf)
				wantLacks(t, r.Text, "/etc/nix")
			} else {
				wantContains(t, r.Text, "/etc/nix/nix.conf", "restart the Nix daemon")
			}
			if got := m.ran("config show --json", true); got == tc.trusted {
				t.Errorf("the daemon's view was read: %v, for trusted = %v", got, tc.trusted)
			}
		})
	}
}

func TestSubstituterMatching(t *testing.T) {
	for _, tc := range []struct {
		entry string
		match bool
	}{
		{"http://127.0.0.1:8484", true},
		{"http://127.0.0.1:8484/", true},
		{"http://127.0.0.1:8484?priority=30", true},
		{"http://127.0.0.1:8484?trusted=1", true},
		{"http://127.0.0.1:8484/?trusted=1", true},
		{"http://127.0.0.1:8484/?priority=30&trusted=1", true},
		{"http://127.0.0.1:8485", false},
		{"http://127.0.0.1:8485/?priority=30", false},
		{"http://127.0.0.1:84840", false},
		{"http://127.0.0.1:848", false},
		{"https://127.0.0.1:8484", false},
		{"http://127.0.0.1:8484/nix", false},
		{"http://localhost:8484", false},
		{"http://example.org/?u=http://127.0.0.1:8484", false},
	} {
		t.Run(tc.entry, func(t *testing.T) {
			m := trustedMachine(t)
			m.full = ready()
			m.full.substituters = []string{"https://cache.nixos.org/", tc.entry}
			r := check(m)
			if r.Configured != tc.match {
				t.Errorf("Configured = %v, want %v:\n%s", r.Configured, tc.match, r.Text)
			}
			if got := strings.Contains(r.Text, substituterLine); got == tc.match {
				t.Errorf("substituter line printed: %v, for a match = %v", got, tc.match)
			}
			wantLacks(t, r.Text, "extra-trusted-public-keys", fallbackTrue)
		})
	}
}

func TestConflictingKeyOfTheSameName(t *testing.T) {
	t.Run("another key before ours", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		m.full.keys = []string{nixosKey, otherKey, testKey}
		r := check(m)
		if r.Configured {
			t.Errorf("configured although Nix would use the other key:\n%s", r.Text)
		}
		wantContains(t, r.Text, otherKey, "removed")
		// Our key is there already: no line to add.
		wantLacks(t, r.Text, "extra-trusted-public-keys", "extra-substituters", fallbackTrue)
	})
	t.Run("another key and ours missing", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		m.full.keys = []string{nixosKey, otherKey}
		r := check(m)
		if r.Configured {
			t.Error("configured")
		}
		wantContains(t, r.Text, otherKey, "removed", keyLine)
	})
	t.Run("another key after ours", func(t *testing.T) {
		// Nix keeps the first key of a name, which is ours.
		m := trustedMachine(t)
		m.full = ready()
		m.full.keys = []string{testKey, otherKey}
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
		wantLacks(t, r.Text, otherKey)
	})
	t.Run("a key of another name", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		m.full.keys = []string{"narrenhaus-00000000:" + strings.TrimPrefix(otherKey, "narrenhaus-d75a9801:"), testKey}
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
	})
	t.Run("in the daemon's view of an untrusted user", func(t *testing.T) {
		m := untrustedMachine(t)
		m.full = ready()
		m.system = ready()
		m.system.keys = []string{otherKey, testKey}
		r := check(m)
		if r.Configured {
			t.Error("configured")
		}
		wantContains(t, r.Text, otherKey, "removed")
	})
	t.Run("in the user's view of an untrusted user", func(t *testing.T) {
		// The daemon never sees that entry.
		m := untrustedMachine(t)
		m.system = ready()
		m.full = ready()
		m.full.keys = []string{otherKey, testKey}
		r := check(m)
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
	})
}

func TestURL(t *testing.T) {
	for listen, want := range map[string]string{
		"127.0.0.1:8484":   "http://127.0.0.1:8484",
		":8484":            "http://127.0.0.1:8484",
		"0.0.0.0:8484":     "http://127.0.0.1:8484",
		"[::]:8484":        "http://127.0.0.1:8484",
		"192.168.1.5:8484": "http://192.168.1.5:8484",
		"[::1]:8484":       "http://[::1]:8484",
		"localhost:8484":   "http://localhost:8484",
		"[0:0::0]:1":       "http://127.0.0.1:1",
		// Not an address with a port: left for the listener to refuse.
		"nonsense": "http://nonsense",
	} {
		if got := URL(listen); got != want {
			t.Errorf("URL(%q) = %q, want %q", listen, got, want)
		}
	}
}

func TestNonLoopbackWarning(t *testing.T) {
	const warning = "anyone who can reach the port"
	for listen, warn := range map[string]bool{
		"127.0.0.1:8484":          false,
		"127.0.0.2:8484":          false,
		"[::1]:8484":              false,
		"localhost:8484":          false,
		":8484":                   true,
		"0.0.0.0:8484":            true,
		"[::]:8484":               true,
		"192.168.1.5:8484":        true,
		"[fe80::1]:8484":          true,
		"nas.example:8484":        true,
		"LOCALHOST:8484":          false,
		"[::ffff:127.0.0.1]:8484": false,
		"localhost":               false,
		"nonsense":                true,
	} {
		for _, configured := range []bool{false, true} {
			m := trustedMachine(t)
			if configured {
				m.full = ready()
				m.full.substituters = append(m.full.substituters, URL(listen))
			}
			r := Check(context.Background(), m.env(), Input{Listen: listen, PublicKey: testKey})
			if r.Configured != configured {
				t.Errorf("listen %q: Configured = %v, want %v", listen, r.Configured, configured)
			}
			if got := strings.Contains(r.Text, warning); got != warn {
				t.Errorf("listen %q, configured %v: warning printed = %v, want %v:\n%s", listen, configured, got, warn, r.Text)
			}
			if warn {
				wantContains(t, r.Text, "upload", "signed", listen)
			}
		}
	}
	t.Run("when nix cannot be run", func(t *testing.T) {
		m := trustedMachine(t)
		m.nixErr = exec.ErrNotFound
		r := Check(context.Background(), m.env(), Input{Listen: "0.0.0.0:8484", PublicKey: testKey})
		wantContains(t, r.Text, warning)
	})
}

func TestNixCannotBeRun(t *testing.T) {
	t.Run("no nix", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		m.nixErr = &exec.Error{Name: "nix", Err: exec.ErrNotFound}
		r := check(m)
		if r.Configured {
			t.Error("configured without having read anything")
		}
		wantContains(t, r.Text, "could not be", "executable file not found",
			substituterLine, keyLine, fallbackTrue, "/etc/nix/nix.conf",
			"when narrenhaus is not running")
		wantLacks(t, r.Text, userConf)
	})
	unreadable := map[string]string{
		"not JSON":             "warning: unknown setting 'foo'\n",
		"empty":                "",
		"not an object":        `["substituters"]`,
		"no substituters":      `{"trusted-public-keys":{"value":[]},"fallback":{"value":false}}`,
		"no keys":              `{"substituters":{"value":[]},"fallback":{"value":false}}`,
		"no fallback":          `{"substituters":{"value":[]},"trusted-public-keys":{"value":[]}}`,
		"substituters a table": `{"substituters":{"value":{}},"trusted-public-keys":{"value":[]},"fallback":{"value":false}}`,
		"fallback a number":    `{"substituters":{"value":[]},"trusted-public-keys":{"value":[]},"fallback":{"value":3}}`,
		"settings not objects": `{"substituters":["http://127.0.0.1:8484"],"trusted-public-keys":[],"fallback":true}`,
		"substituters null":    `{"substituters":{"value":null},"trusted-public-keys":{"value":[]},"fallback":{"value":false}}`,
		"keys null":            `{"substituters":{"value":[]},"trusted-public-keys":{"value":null},"fallback":{"value":false}}`,
		"fallback null":        `{"substituters":{"value":[]},"trusted-public-keys":{"value":[]},"fallback":{"value": null }}`,
		"no value":             `{"substituters":{},"trusted-public-keys":{"value":[]},"fallback":{"value":false}}`,
	}
	for name, output := range unreadable {
		t.Run("output "+name, func(t *testing.T) {
			m := trustedMachine(t)
			m.fullOutput = []byte(output)
			if output == "" {
				m.fullOutput = []byte{}
			}
			r := check(m)
			if r.Configured {
				t.Error("configured")
			}
			wantContains(t, r.Text, "could not be", substituterLine, keyLine, fallbackTrue, "/etc/nix/nix.conf")
		})
	}
	t.Run("the daemon's view cannot be read", func(t *testing.T) {
		m := untrustedMachine(t)
		m.full = ready()
		m.system = ready()
		m.systemErr = errors.New("exit status 1")
		r := check(m)
		if r.Configured {
			t.Error("configured without the daemon's view")
		}
		wantContains(t, r.Text, "could not be", substituterLine, keyLine, "/etc/nix/nix.conf")
	})
	t.Run("the error is kept to one short line", func(t *testing.T) {
		m := trustedMachine(t)
		m.nixErr = errors.New("error: first line\n" + strings.Repeat("more\n", 500) + strings.Repeat("x", 5000))
		r := check(m)
		wantContains(t, r.Text, "error: first line", substituterLine)
		if len(r.Text) > 3000 {
			t.Errorf("the text is %d bytes long", len(r.Text))
		}
		wantLacks(t, r.Text, "more")
	})
	t.Run("a long error line is cut", func(t *testing.T) {
		m := trustedMachine(t)
		m.nixErr = errors.New("error: " + strings.Repeat("é", 5000))
		r := check(m)
		wantContains(t, r.Text, "error: éé", "…", substituterLine)
		if len(r.Text) > 3000 {
			t.Errorf("the text is %d bytes long", len(r.Text))
		}
		if !utf8.ValidString(r.Text) {
			t.Error("the cut left half a character")
		}
	})
	t.Run("both commands fail differently", func(t *testing.T) {
		// Which failure tells what is wrong depends on the Nix, so both
		// are quoted.
		m := trustedMachine(t)
		m.noConfig = true
		m.fullOutput = []byte("not JSON")
		r := check(m)
		wantContains(t, r.Text, "nix config show --json: error: 'config' is not a recognised command",
			"nix show-config --json: the output is not understood", substituterLine)
	})
}

func TestOldNixShowConfig(t *testing.T) {
	m := untrustedMachine(t)
	m.noConfig = true
	m.full = ready()
	m.system = ready()
	// What Nix 2.26 prints; older ones have no `store info` at all, which
	// reads as not trusted.
	m.info = readTestdata(t, "store-info-2.26.json")
	r := check(m)
	if !r.Configured {
		t.Errorf("not configured:\n%s", r.Text)
	}
	if !m.ran("show-config --json", false) {
		t.Errorf("show-config was not tried: %+v", m.calls)
	}

	t.Run("the daemon's view too", func(t *testing.T) {
		m := untrustedMachine(t)
		m.noConfig = true
		m.full = ready()
		r := check(m)
		if r.Configured {
			t.Error("configured")
		}
		if !m.ran("show-config --json", true) {
			t.Errorf("show-config was not used for the daemon's view: %+v", m.calls)
		}
		wantContains(t, r.Text, substituterLine, keyLine)
		wantLacks(t, r.Text, "could not be")
	})
}

func TestUploadHintAlwaysPrinted(t *testing.T) {
	cases := map[string]func(m *machine){
		"configured":         func(m *machine) { m.full = ready() },
		"missing everything": func(m *machine) {},
		"untrusted": func(m *machine) {
			m.info = []byte(`{"trusted":false}`)
		},
		"generated": func(m *machine) {
			m.info = []byte(`{"trusted":false}`)
			m.links = map[string]string{"/etc/nix/nix.conf": "/etc/static/nix/nix.conf"}
		},
		"conflicting key": func(m *machine) {
			m.full = ready()
			m.full.keys = []string{otherKey, testKey}
		},
		"no nix": func(m *machine) { m.nixErr = exec.ErrNotFound },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			m := trustedMachine(t)
			change(m)
			r := check(m)
			wantContains(t, r.Text, uploadLine, stallNote)
			if n := strings.Count(r.Text, uploadLine); n != 1 {
				t.Errorf("the upload hint is printed %d times", n)
			}
		})
	}
	t.Run("with the URL of an unspecified address", func(t *testing.T) {
		m := trustedMachine(t)
		r := Check(context.Background(), m.env(), Input{Listen: ":9000", PublicKey: testKey})
		wantContains(t, r.Text, "nix copy --to 'http://127.0.0.1:9000?compression=none' <path>", "extra-substituters = http://127.0.0.1:9000")
	})
}

func TestCheckWithAnEmptyEnv(t *testing.T) {
	// A zero Env reads nothing; the check still answers.
	r := Check(context.Background(), Env{}, Input{Listen: testListen, PublicKey: testKey})
	if r.Configured {
		t.Error("configured")
	}
	wantContains(t, r.Text, substituterLine, keyLine, fallbackTrue, uploadLine)
}

func TestUserFileFollowsXDGConfigHome(t *testing.T) {
	m := trustedMachine(t)
	env := m.env()
	env.ConfigHome = "/home/alice/etc"
	r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
	wantContains(t, r.Text, "/home/alice/etc/nix/nix.conf")
	wantLacks(t, r.Text, userConf)

	t.Run("no home directory known", func(t *testing.T) {
		env := m.env()
		env.Home = ""
		r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
		wantContains(t, r.Text, "~/.config/nix/nix.conf")
	})
}

func TestUserFileFollowsNixUserConfFiles(t *testing.T) {
	// With NIX_USER_CONF_FILES set, Nix reads the user's settings from the
	// files it names and from no other; the first has the last word.
	m := trustedMachine(t)
	env := m.env()
	env.ConfigHome = "/home/alice/etc"
	env.UserConfFiles, env.UserConfFilesSet = "/home/alice/work/nix.conf:/srv/team/nix.conf", true
	r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
	wantContains(t, r.Text, "Add to /home/alice/work/nix.conf, which needs neither root nor a restart:")
	wantLacks(t, r.Text, userConf, "/home/alice/etc", "/srv/team", "NIX_USER_CONF_FILES")

	t.Run("fallback alone", func(t *testing.T) {
		m := untrustedMachine(t)
		env := m.env()
		env.UserConfFiles, env.UserConfFilesSet = "::relative/nix.conf:", true
		r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
		wantContains(t, r.Text, "Add to relative/nix.conf, which needs neither root nor a restart:")
		wantLacks(t, r.Text, userConf, "NIX_USER_CONF_FILES")
	})
	t.Run("set and naming no file", func(t *testing.T) {
		// Nix then reads no file of the user's at all, and the advice
		// must not read as if the lines would count where they are put.
		for _, files := range []string{"", ":", "::"} {
			env := m.env()
			env.UserConfFiles, env.UserConfFilesSet = files, true
			r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
			wantContains(t, r.Text, "Add to "+userConf, "NIX_USER_CONF_FILES is set and names no file")
			if n := strings.Count(r.Text, "NIX_USER_CONF_FILES"); n != 1 {
				t.Errorf("%q: the note is printed %d times:\n%s", files, n, r.Text)
			}
		}
	})
	t.Run("not set", func(t *testing.T) {
		env := m.env()
		env.UserConfFiles = "/home/alice/work/nix.conf"
		r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
		wantContains(t, r.Text, "Add to "+userConf)
		wantLacks(t, r.Text, "/home/alice/work", "NIX_USER_CONF_FILES")
	})
	t.Run("nothing to add to it", func(t *testing.T) {
		m := trustedMachine(t)
		m.full = ready()
		env := m.env()
		env.UserConfFilesSet = true
		r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
		if !r.Configured {
			t.Errorf("not configured:\n%s", r.Text)
		}
		wantLacks(t, r.Text, "NIX_USER_CONF_FILES")
	})
}

func TestRunsOfNixHaveATimeLimit(t *testing.T) {
	// The check bounds its runs of nix itself, all of them together, so
	// that a caller without a deadline is not kept by a daemon that does
	// not answer.
	const limit = 20 * time.Second
	for name, m := range map[string]*machine{
		"trusted":        trustedMachine(t),
		"untrusted":      untrustedMachine(t),
		"old nix":        {t: t, full: stock(), system: stock(), noConfig: true, info: []byte(`{"trusted":0}`)},
		"nix that fails": {t: t, nixErr: errors.New("exit status 1")},
	} {
		t.Run(name, func(t *testing.T) {
			env := m.env()
			run, runs := env.Run, 0
			var started time.Time
			env.Run = func(ctx context.Context, changes []string, args ...string) ([]byte, error) {
				runs++
				deadline, ok := ctx.Deadline()
				switch {
				case !ok:
					t.Errorf("nix %q was run without a time limit", args)
				case deadline.After(started.Add(limit + time.Second)):
					t.Errorf("nix %q may run until %v after the check began, want at most %v", args, deadline.Sub(started), limit)
				}
				return run(ctx, changes, args...)
			}
			started = time.Now()
			Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
			if runs < 2 {
				t.Fatalf("nix was run %d times", runs)
			}

			// A caller's own, shorter limit stays.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			env.Run = func(ctx context.Context, changes []string, args ...string) ([]byte, error) {
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > time.Second {
					t.Errorf("nix %q was run with the limit %v (%v), want the caller's second", args, time.Until(deadline), ok)
				}
				return run(ctx, changes, args...)
			}
			Check(ctx, env, Input{Listen: testListen, PublicKey: testKey})
		})
	}
	// The limit is not shorter than it is said to be.
	env := trustedMachine(t).env()
	run := env.Run
	env.Run = func(ctx context.Context, changes []string, args ...string) ([]byte, error) {
		if deadline, _ := ctx.Deadline(); time.Until(deadline) < limit-5*time.Second {
			t.Errorf("nix %q was run with %v left, want about %v", args, time.Until(deadline), limit)
		}
		return run(ctx, changes, args...)
	}
	Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
}

func TestEnviron(t *testing.T) {
	base := []string{"PATH=/bin", "NIX_CONFIG=extra-substituters = http://x", "HOME=/home/alice", "NIX_CONFIGURED=1"}
	got := environ(base, []string{"NIX_USER_CONF_FILES=", "NIX_CONFIG"})
	want := []string{"PATH=/bin", "HOME=/home/alice", "NIX_CONFIGURED=1", "NIX_USER_CONF_FILES="}
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if base[1] != "NIX_CONFIG=extra-substituters = http://x" || len(base) != 4 {
		t.Errorf("the base environment was changed: %q", base)
	}
	// A set variable replaces the one that is there.
	got = environ([]string{"A=1", "B=2"}, []string{"A=3"})
	if want := []string{"B=2", "A=3"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := environ(base, nil); !slices.Equal(got, base) {
		t.Errorf("no changes: got %q, want %q", got, base)
	}
}

// TestSystemAgainstNix runs the real nix, read-only, to hold the facts the
// check rests on: the shape of the two outputs, and that the daemon's view
// leaves out the user's files and NIX_CONFIG.
func TestSystemAgainstNix(t *testing.T) {
	if _, err := exec.LookPath("nix"); err != nil {
		t.Skip("nix is not on PATH")
	}
	const (
		marker     = "http://127.0.0.1:1/narrenhaus-test"
		userMarker = "http://127.0.0.1:1/narrenhaus-test-user-file"
	)
	// A user's nix.conf of the test's own, where Nix looks for one.
	configHome := t.TempDir()
	if err := os.MkdirAll(configHome+"/nix", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configHome+"/nix/nix.conf", []byte("extra-substituters = "+userMarker+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configHome)
	t.Setenv("NIX_USER_CONF_FILES", "")
	os.Unsetenv("NIX_USER_CONF_FILES")
	t.Setenv("NIX_CONFIG", "extra-substituters = "+marker)
	env := System()
	ctx := context.Background()
	userFile, read := env.userConf()
	if want := configHome + "/nix/nix.conf"; userFile != want || !read {
		t.Errorf("the user's file is %q (read %v), want %q", userFile, read, want)
	}

	full, err := readSettings(ctx, env, nil)
	if err != nil {
		t.Fatalf("the user's view: %v", err)
	}
	if !slices.Contains(full.substituters, marker) {
		t.Errorf("the user's view lacks the substituter of NIX_CONFIG: %q", full.substituters)
	}
	if !slices.Contains(full.substituters, userMarker) {
		t.Errorf("the user's view lacks the substituter of %s: %q", userFile, full.substituters)
	}
	system, err := readSettings(ctx, env, daemonEnv)
	if err != nil {
		t.Fatalf("the daemon's view: %v", err)
	}
	if slices.Contains(system.substituters, marker) || slices.Contains(system.substituters, userMarker) {
		t.Errorf("the daemon's view has a substituter of the user's: %q", system.substituters)
	}
	if len(system.keys) == 0 {
		t.Error("no trusted public keys were read")
	}

	// Whatever this machine is, the check answers with advice or one line.
	r := Check(ctx, env, Input{Listen: "127.0.0.1:1", PublicKey: testKey})
	if r.Configured {
		t.Errorf("configured for a cache nobody set up:\n%s", r.Text)
	}
	wantContains(t, r.Text, "nix copy --to 'http://127.0.0.1:1?compression=none' <path>")
	wantLacks(t, r.Text, "could not be")
	t.Log(r.Text)

	t.Run("NIX_USER_CONF_FILES", func(t *testing.T) {
		// Nix reads the files the variable names and not the one under
		// XDG_CONFIG_HOME, and the first of them has the last word: it is
		// the one the advice names.
		const firstMarker, secondMarker = marker + "-first", marker + "-second"
		dir := t.TempDir()
		first, second := dir+"/first.conf", dir+"/second.conf"
		for file, text := range map[string]string{
			first:  "extra-substituters = " + firstMarker + "\nfallback = true\n",
			second: "extra-substituters = " + secondMarker + "\nfallback = false\n",
		} {
			if err := os.WriteFile(file, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		t.Setenv("NIX_CONFIG", "")
		t.Setenv("NIX_USER_CONF_FILES", ":"+first+":"+second)
		env := System()
		if got, read := env.userConf(); got != first || !read {
			t.Errorf("the user's file is %q (read %v), want %q", got, read, first)
		}
		full, err := readSettings(ctx, env, nil)
		if err != nil {
			t.Fatalf("the user's view: %v", err)
		}
		if !slices.Contains(full.substituters, firstMarker) || !slices.Contains(full.substituters, secondMarker) {
			t.Errorf("the user's view lacks a substituter of the files named: %q", full.substituters)
		}
		if slices.Contains(full.substituters, userMarker) {
			t.Errorf("the user's view has the substituter of %s: %q", userFile, full.substituters)
		}
		if !full.fallback {
			t.Error("fallback is off: the second file had the last word")
		}

		// Set and naming nothing, no file of the user's is read.
		t.Setenv("NIX_USER_CONF_FILES", "")
		env = System()
		if got, read := env.userConf(); got != userFile || read {
			t.Errorf("the user's file is %q (read %v), want %q, not read", got, read, userFile)
		}
		if full, err = readSettings(ctx, env, nil); err != nil {
			t.Fatalf("the user's view: %v", err)
		}
		if slices.Contains(full.substituters, userMarker) {
			t.Errorf("the user's view has the substituter of %s: %q", userFile, full.substituters)
		}
	})
}

func TestSystem(t *testing.T) {
	env := System()
	if env.Run == nil || env.ReadFile == nil || env.Readlink == nil {
		t.Fatal("System leaves a function unset")
	}
	if home, err := os.UserHomeDir(); err == nil && env.Home != home {
		t.Errorf("Home = %q, want %q", env.Home, home)
	}
	t.Run("the user's files", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/somewhere/etc")
		t.Setenv("NIX_USER_CONF_FILES", "/somewhere/nix.conf:/elsewhere/nix.conf")
		if got, read := System().userConf(); got != "/somewhere/nix.conf" || !read {
			t.Errorf("with NIX_USER_CONF_FILES: the user's file is %q (read %v)", got, read)
		}
		t.Setenv("NIX_USER_CONF_FILES", "")
		if got, read := System().userConf(); got != "/somewhere/etc/nix/nix.conf" || read {
			t.Errorf("with NIX_USER_CONF_FILES empty: the user's file is %q (read %v)", got, read)
		}
		os.Unsetenv("NIX_USER_CONF_FILES")
		if got, read := System().userConf(); got != "/somewhere/etc/nix/nix.conf" || !read {
			t.Errorf("without NIX_USER_CONF_FILES: the user's file is %q (read %v)", got, read)
		}
	})
	t.Run("nix missing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		if _, err := env.Run(context.Background(), nil, "--version"); err == nil {
			t.Error("no error from running a nix that is not there")
		}
		r := Check(context.Background(), env, Input{Listen: testListen, PublicKey: testKey})
		if r.Configured {
			t.Error("configured")
		}
		wantContains(t, r.Text, "could not be", substituterLine, keyLine, fallbackTrue, uploadLine)
	})
	t.Run("standard output only, and the error names what nix said", func(t *testing.T) {
		if err := exec.Command("/bin/sh", "-c", "true").Run(); err != nil {
			t.Skipf("no /bin/sh to play nix with: %v", err)
		}
		dir := t.TempDir()
		script := "#!/bin/sh\necho 'warning: on standard error' >&2\nif [ \"$1\" = fail ]; then echo 'error: no such thing' >&2; exit 1; fi\necho \"out:$NIX_USER_CONF_FILES:${NIX_CONFIG-unset}\"\n"
		if err := os.WriteFile(dir+"/nix", []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
		t.Setenv("NIX_CONFIG", "fallback = true")
		t.Setenv("NIX_USER_CONF_FILES", "/somewhere")
		out, err := env.Run(context.Background(), nil, "ok")
		if err != nil || string(out) != "out:/somewhere:fallback = true\n" {
			t.Errorf("got %q, %v", out, err)
		}
		out, err = env.Run(context.Background(), daemonEnv, "ok")
		if err != nil || string(out) != "out::unset\n" {
			t.Errorf("with the daemon's environment: got %q, %v", out, err)
		}
		_, err = env.Run(context.Background(), nil, "fail")
		if err == nil || !strings.Contains(err.Error(), "error: no such thing") {
			t.Errorf("got %v, want an error with nix's own message", err)
		}
	})
}
