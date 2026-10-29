package narinfo

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The first test vector of RFC 8032, section 7.1: the private key's seed,
// its public key and the signature over the empty message.
const (
	rfcSeed   = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
	rfcPublic = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
	rfcSig    = "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// rfcKeyFile is the RFC key as Nix writes a secret key file: the name, a
// colon and the base 64 of the seed followed by the public key.
func rfcKeyFile(t *testing.T, name string) string {
	t.Helper()
	secret := append(unhex(t, rfcSeed), unhex(t, rfcPublic)...)
	return name + ":" + base64.StdEncoding.EncodeToString(secret)
}

func writeKeyFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCreateThenLoad(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "config", "narrenhaus", "signing-key")

	created, err := LoadOrCreateSigningKey(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := st.Mode(); mode != 0o600 {
		t.Errorf("the key file has mode %v, want -rw-------", mode)
	}
	for _, dir := range []string{filepath.Dir(path), filepath.Dir(filepath.Dir(path))} {
		st, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if perm := st.Mode().Perm(); perm != 0o700 {
			t.Errorf("%s has mode %v, want rwx------", dir, perm)
		}
	}
	// Nothing but the key is left in its directory.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("the key's directory holds %d entries, want the key alone", len(entries))
	}

	name, publicText, ok := strings.Cut(created.Public(), ":")
	if !ok {
		t.Fatalf("Public = %q", created.Public())
	}
	public, err := base64.StdEncoding.DecodeString(publicText)
	if err != nil || len(public) != ed25519.PublicKeySize {
		t.Fatalf("Public = %q: not the base 64 of 32 bytes", created.Public())
	}
	if !regexp.MustCompile(`^narrenhaus-[0-9a-f]{8}$`).MatchString(name) {
		t.Errorf("the key is named %q", name)
	}
	if want := "narrenhaus-" + hex.EncodeToString(public)[:8]; name != want {
		t.Errorf("the key is named %q, want %q after its public key", name, want)
	}
	if created.Name() != name {
		t.Errorf("Name = %q, Public names it %q", created.Name(), name)
	}

	// The file is what Nix reads as a secret key: name, colon, 64 bytes.
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fileName, payload, _ := strings.Cut(string(content), ":")
	secret, err := base64.StdEncoding.DecodeString(payload)
	if fileName != name || err != nil || len(secret) != ed25519.PrivateKeySize {
		t.Errorf("the key file is not <name>:<base 64 of 64 bytes>")
	}

	loaded, err := LoadOrCreateSigningKey(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Public() != created.Public() {
		t.Errorf("loaded %q, created %q", loaded.Public(), created.Public())
	}
	// Ed25519 signatures are deterministic: the same key signs the same.
	if a, b := created.Sign("1;x"), loaded.Sign("1;x"); a != b {
		t.Errorf("the loaded key signs %q, the created one %q", b, a)
	}
	again, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, content) {
		t.Error("loading rewrote the key file")
	}

	// Another path gets another key.
	other, err := LoadOrCreateSigningKey(filepath.Join(base, "other-key"))
	if err != nil {
		t.Fatal(err)
	}
	if other.Public() == created.Public() {
		t.Error("two created keys are the same")
	}
}

func TestPublicFormat(t *testing.T) {
	for name, content := range map[string]string{
		"as Nix writes it": rfcKeyFile(t, "cache.example.org-1"),
		"with a line feed": rfcKeyFile(t, "cache.example.org-1") + "\n",
	} {
		key, err := LoadOrCreateSigningKey(writeKeyFile(t, content))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := key.Name(); got != "cache.example.org-1" {
			t.Errorf("%s: Name = %q", name, got)
		}
		want := "cache.example.org-1:" + base64.StdEncoding.EncodeToString(unhex(t, rfcPublic))
		if got := key.Public(); got != want {
			t.Errorf("%s: Public = %q, want %q", name, got, want)
		}
	}
}

func TestSignKnownVector(t *testing.T) {
	key, err := LoadOrCreateSigningKey(writeKeyFile(t, rfcKeyFile(t, "test-1")))
	if err != nil {
		t.Fatal(err)
	}
	want := "test-1:" + base64.StdEncoding.EncodeToString(unhex(t, rfcSig))
	if got := key.Sign(""); got != want {
		t.Errorf("Sign of the empty message = %q, want %q", got, want)
	}

	sig := key.Sign("1;/nix/store/x;sha256:y;1;")
	_, payload, _ := strings.Cut(sig, ":")
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil || len(raw) != ed25519.SignatureSize {
		t.Fatalf("Sign = %q: not the base 64 of 64 bytes", sig)
	}
	if !ed25519.Verify(unhex(t, rfcPublic), []byte("1;/nix/store/x;sha256:y;1;"), raw) {
		t.Error("the signature does not verify")
	}
}

func TestLoadRefusesGarbage(t *testing.T) {
	good := rfcKeyFile(t, "test-1")
	_, payload, _ := strings.Cut(good, ":")
	wrongPublic := append(unhex(t, rfcSeed), make([]byte, 32)...)
	for name, content := range map[string]string{
		"empty":            "",
		"only a line feed": "\n",
		"no colon":         "test-1",
		"no name":          ":" + payload,
		"no key":           "test-1:",
		"not base 64":      "test-1:!!!",
		"a public key":     "test-1:" + base64.StdEncoding.EncodeToString(unhex(t, rfcPublic)),
		"65 bytes":         "test-1:" + base64.StdEncoding.EncodeToString(make([]byte, 65)),
		"halves differ":    "test-1:" + base64.StdEncoding.EncodeToString(wrongPublic),
		"name with space":  "test 1:" + payload,
		"name with a feed": "test\n1:" + payload,
		"two lines":        good + "\n" + good,
		"binary":           "\x00\x01\x02\xff",
	} {
		path := writeKeyFile(t, content)
		_, err := LoadOrCreateSigningKey(path)
		if err == nil {
			t.Errorf("%s: loaded", name)
			continue
		}
		if !strings.Contains(err.Error(), path) {
			t.Errorf("%s: the error does not name the file: %v", name, err)
		}
		if strings.Contains(err.Error(), payload) {
			t.Errorf("%s: the error shows the key", name)
		}
		// A file that cannot be read is never replaced by a new key.
		after, readErr := os.ReadFile(path)
		if readErr != nil || string(after) != content {
			t.Errorf("%s: the file was changed", name)
		}
	}
}

func TestLoadOrCreateFailsClearly(t *testing.T) {
	base := t.TempDir()

	// The path is a directory.
	if _, err := LoadOrCreateSigningKey(base); err == nil || !strings.Contains(err.Error(), base) {
		t.Errorf("a directory as the key: %v", err)
	}

	// A parent that is a file.
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	under := filepath.Join(file, "key")
	if _, err := LoadOrCreateSigningKey(under); err == nil || !strings.Contains(err.Error(), under) {
		t.Errorf("a key under a file: %v", err)
	}
}

// Nix itself reads a narinfo signed with a created key and takes the
// signature as that of the key's public half.
func TestNixAcceptsTheSignature(t *testing.T) {
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("no nix on PATH")
	}

	key, err := LoadOrCreateSigningKey(filepath.Join(t.TempDir(), "key"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := LoadOrCreateSigningKey(filepath.Join(t.TempDir(), "key"))
	if err != nil {
		t.Fatal(err)
	}

	// The stored form with this key's signature and none other, so that
	// nothing but it can make the path trusted.
	info := mustParse(t, nixWrote)
	info.Sigs = nil
	signed := info.Signed(key.Sign(info.Fingerprint()))

	root := t.TempDir()
	for name, content := range map[string][]byte{
		"nix-cache-info":             []byte("StoreDir: " + StoreDir + "\n"),
		info.HashPart() + ".narinfo": signed,
	} {
		if err := os.WriteFile(filepath.Join(root, name), content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(root)))
	defer srv.Close()

	// Nix with no configuration of the user's and a cache of its own, so
	// that neither an earlier answer nor a setting decides the outcome.
	home := t.TempDir()
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "NIX_CONFIG", "NIX_USER_CONF_FILES", "XDG_CACHE_HOME", "NIX_REMOTE":
		default:
			env = append(env, kv)
		}
	}
	env = append(env, "NIX_USER_CONF_FILES=", "XDG_CACHE_HOME="+home)

	run := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		args = append([]string{"--extra-experimental-features", "nix-command"}, args...)
		cmd := exec.CommandContext(ctx, nix, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	// Nix reads the narinfo and shows the signature as the key's.
	out, err := run("path-info", "--store", srv.URL,
		"--option", "trusted-public-keys", key.Public(), "--sigs", info.StorePath)
	t.Logf("nix path-info: %v\n%s", err, out)
	if err != nil {
		t.Fatalf("nix path-info: %v\n%s", err, out)
	}
	if !strings.Contains(out, info.StorePath) || !strings.Contains(out, key.Name()+":") {
		t.Fatalf("nix path-info does not show the path with its signature:\n%s", out)
	}

	// It verifies under the key's public half; with only another key
	// trusted, or the key's name on another key's bytes, the path is
	// untrusted.
	verify := func(trusted string) (string, error) {
		out, err := run("store", "verify", "--no-contents", "--store", srv.URL,
			"--option", "trusted-public-keys", trusted, info.StorePath)
		t.Logf("nix store verify, trusting %s: %v\n%s", trusted, err, out)
		return out, err
	}
	if out, err := verify(key.Public()); err != nil {
		t.Fatalf("nix store verify with the key trusted: %v\n%s", err, out)
	}
	_, otherBytes, _ := strings.Cut(other.Public(), ":")
	for _, trusted := range []string{other.Public(), key.Name() + ":" + otherBytes} {
		out, err := verify(trusted)
		if err == nil {
			t.Fatalf("nix store verify succeeded trusting only %s:\n%s", trusted, out)
		}
		if !strings.Contains(out, "untrusted") {
			t.Fatalf("nix store verify trusting %s failed for another reason than trust:\n%s", trusted, out)
		}
	}
}
