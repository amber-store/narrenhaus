package narinfo

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// SigningKey is a named Ed25519 key that signs narinfo fingerprints, in
// the form Nix knows as a binary cache key. The zero value signs nothing:
// take a key from LoadOrCreateSigningKey.
type SigningKey struct {
	name string
	key  ed25519.PrivateKey
}

// LoadOrCreateSigningKey reads the key in the file at path, which holds
// "<name>:<base64 of 64 bytes>" as Nix writes a secret key: the 32-byte
// seed followed by the 32-byte public key. When there is no such file it
// makes a key named "narrenhaus-<first 8 hex digits of the public key>"
// and writes it with mode 0600, making missing parent directories with
// mode 0700.
//
// A file that is there and cannot be read as a key is an error and is
// left as it is. Errors name the path and never show the key.
func LoadOrCreateSigningKey(path string) (SigningKey, error) {
	key, err := loadSigningKey(path)
	if errors.Is(err, fs.ErrNotExist) {
		key, err = createSigningKey(path)
	}
	if err != nil {
		return SigningKey{}, fmt.Errorf("signing key %s: %w", path, err)
	}
	return key, nil
}

// loadSigningKey reads the key file at path. Its error for a missing file
// is fs.ErrNotExist.
func loadSigningKey(path string) (SigningKey, error) {
	text, err := os.ReadFile(path)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			// The caller names the path.
			err = pathErr.Err
		}
		return SigningKey{}, err
	}
	return parseSigningKey(string(text))
}

// parseSigningKey reads "<name>:<base64 of 64 bytes>", with or without
// white space around it.
func parseSigningKey(text string) (SigningKey, error) {
	name, payload, ok := strings.Cut(strings.TrimSpace(text), ":")
	if !ok || name == "" {
		return SigningKey{}, errors.New("not a key file: want <name>:<base 64 of 64 bytes>")
	}
	// The name goes into narinfo lines and into Nix's space-separated
	// trusted-public-keys.
	for i := range len(name) {
		if name[i] <= ' ' || name[i] > '~' {
			return SigningKey{}, errors.New("the key's name is not printable ASCII without spaces")
		}
	}
	raw, err := decodeBase64(payload, ed25519.PrivateKeySize)
	if err != nil {
		return SigningKey{}, fmt.Errorf("the key is %w", err)
	}
	key := ed25519.NewKeyFromSeed(raw[:ed25519.SeedSize])
	if !key.Equal(ed25519.PrivateKey(raw)) {
		return SigningKey{}, errors.New("the key's public half does not belong to its private half")
	}
	return SigningKey{name: name, key: key}, nil
}

// createSigningKey makes a key and writes it to path. If another process
// writes a key there first, that key is returned.
func createSigningKey(path string) (SigningKey, error) {
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		return SigningKey{}, fmt.Errorf("making a key: %w", err)
	}
	key := SigningKey{
		name: "narrenhaus-" + hex.EncodeToString(public[:4]),
		key:  private,
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return SigningKey{}, err
	}
	// The file is written under another name and then linked to its own,
	// so that it is never there half-written and never replaces a key.
	tmp, err := os.CreateTemp(dir, ".signing-key-*") // mode 0600
	if err != nil {
		return SigningKey{}, err
	}
	defer os.Remove(tmp.Name())
	// As Nix writes it: no line feed at the end.
	_, err = tmp.WriteString(key.name + ":" + base64.StdEncoding.EncodeToString(private))
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return SigningKey{}, err
	}
	if err := os.Link(tmp.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return loadSigningKey(path)
		}
		return SigningKey{}, err
	}
	return key, nil
}

// Name returns the key's name, the part of a signature before its colon.
func (k SigningKey) Name() string {
	return k.name
}

// Public returns "<name>:<base64 of the 32-byte public key>", the form of
// an entry in Nix's trusted-public-keys.
func (k SigningKey) Public() string {
	public := k.key.Public().(ed25519.PublicKey)
	return k.name + ":" + base64.StdEncoding.EncodeToString(public)
}

// Sign returns "<name>:<base64 of the 64-byte signature>" over
// fingerprint, the value of a narinfo's Sig line.
func (k SigningKey) Sign(fingerprint string) string {
	sig := ed25519.Sign(k.key, []byte(fingerprint))
	return k.name + ":" + base64.StdEncoding.EncodeToString(sig)
}
