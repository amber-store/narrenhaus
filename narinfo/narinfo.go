// Package narinfo reads Nix's narinfo text, writes the form narrenhaus
// stores and serves, and signs it.
//
// A narinfo describes one store path: the hash and size of its NAR, the
// paths it refers to, who built it and who vouches for it. Nix uploads one
// with the URL and compression of the file it sent; narrenhaus keeps the
// NAR uncompressed under its own hash, so it stores the text again with
// those fields rewritten (Stored) and, when serving, adds the signature of
// this machine's key (Signed).
package narinfo

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/amber-store/narrenhaus/nix32"
)

// StoreDir is the store narrenhaus serves; Nix refuses a cache made for
// another.
const StoreDir = "/nix/store"

// MaxStored is the most bytes a stored narinfo may have. Parse does not
// apply it: the caller compares it with the length of Stored.
const MaxStored = 1 << 20

// ErrMalformed wraps every error of Parse.
var ErrMalformed = errors.New("narinfo: malformed")

const (
	// hashPartLen is the length of a store path's hash in Nix's base 32.
	hashPartLen = 32
	// maxNameLen is the longest name Nix allows after the hash and dash.
	maxNameLen = 211
	// sigLen is the length of an Ed25519 signature.
	sigLen = 64
)

// Info is a parsed narinfo. Take one from Parse: Stored, Signed and
// Fingerprint write the fields as they are, and only Parse checks them.
type Info struct {
	StorePath   string   // full path under StoreDir
	NarHash     [32]byte // SHA-256
	NarSize     uint64
	References  []string // base names, as given
	Deriver     string   // "" when absent or "unknown-deriver"
	Sigs        []string // "name:base64", as given
	CA          string
	URL         string // as parsed; Stored ignores it
	Compression string // as parsed; Stored ignores it
}

// Parse reads Nix's narinfo text. Unknown keys are ignored. NarHash is
// accepted as sha256:<nix32>, sha256:<hex> and SRI sha256-<base64>.
//
// Parse takes no text Nix would read differently. Every line must be
// "Key: value" ended by a line feed. StorePath, NarHash and NarSize are
// required and NarSize is not zero; StorePath, the references and the
// deriver must be store path names as Nix allows them; a signature must be
// a name and the base 64 of 64 bytes; a second References or CA line is an
// error. Of another key given twice the last one counts, as for Nix.
// Errors wrap ErrMalformed and are one short line whatever the input.
func Parse(b []byte) (*Info, error) {
	info := &Info{}
	var havePath, haveHash, haveReferences, haveCA bool

	rest := b
	for n := 1; len(rest) > 0; n++ {
		eol := bytes.IndexByte(rest, '\n')
		if eol < 0 {
			return nil, malformed("line %d does not end in a line feed", n)
		}
		line := string(rest[:eol])
		rest = rest[eol+1:]

		// Nix takes what is before the first colon as the key and what
		// starts two characters after it as the value, whatever the
		// character between is, and goes on into the next line when there
		// is none. Only lines it reads as written here are accepted.
		colon := strings.IndexByte(line, ':')
		if colon <= 0 || colon+1 >= len(line) || line[colon+1] != ' ' {
			return nil, malformed("line %d is not \"Key: value\": %s", n, clip(line))
		}
		key, value := line[:colon], line[colon+2:]

		var err error
		switch key {
		case "StorePath":
			err = checkStorePath(value)
			info.StorePath, havePath = value, true
		case "URL":
			info.URL = value
		case "Compression":
			info.Compression = value
		case "NarHash":
			info.NarHash, err = parseNarHash(value)
			haveHash = true
		case "NarSize":
			info.NarSize, err = parseSize(value)
		case "References":
			if haveReferences {
				err = errors.New("a second References line")
				break
			}
			haveReferences = true
			info.References, err = parseReferences(value)
		case "Deriver":
			info.Deriver = ""
			if value != "unknown-deriver" {
				err = checkBaseName(value)
				info.Deriver = value
			}
		case "Sig":
			err = checkSig(value)
			info.Sigs = append(info.Sigs, value)
		case "CA":
			if haveCA {
				err = errors.New("a second CA line")
				break
			}
			haveCA = true
			err = checkCA(value)
			info.CA = value
		}
		if err != nil {
			return nil, malformed("line %d, %s: %v", n, key, err)
		}
	}

	switch {
	case !havePath:
		return nil, malformed("no StorePath")
	case !haveHash:
		return nil, malformed("no NarHash")
	case info.NarSize == 0:
		return nil, malformed("NarSize is missing or zero")
	}
	return info, nil
}

// malformed returns an error wrapping ErrMalformed.
func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}

// clip quotes s for an error message as printable ASCII, cut to a length
// that keeps the message short.
func clip(s string) string {
	const limit = 80
	cut := len(s) > limit
	if cut {
		s = s[:limit]
	}
	quoted := strconv.QuoteToASCII(s)
	quoted = quoted[1 : len(quoted)-1]
	if len(quoted) > limit {
		// Escapes made it longer. The cut may fall inside one, which
		// leaves the message odd and still one line of text.
		quoted, cut = quoted[:limit], true
	}
	if cut {
		return `"` + quoted + `"...`
	}
	return `"` + quoted + `"`
}

// checkStorePath checks that s is a path directly under StoreDir, written
// exactly so.
func checkStorePath(s string) error {
	base, ok := strings.CutPrefix(s, StoreDir+"/")
	if !ok {
		return fmt.Errorf("%s is not under %s", clip(s), StoreDir)
	}
	return checkBaseName(base)
}

// checkBaseName checks that s is the base name of a store path: 32
// characters of Nix's base 32, a dash and a name, by the rules of Nix's
// StorePath.
func checkBaseName(s string) error {
	if len(s) < hashPartLen+2 || s[hashPartLen] != '-' {
		return fmt.Errorf("%s is not <%d characters>-<name>", clip(s), hashPartLen)
	}
	hash, name := s[:hashPartLen], s[hashPartLen+1:]
	// 32 digits are 160 bits exactly, so every text of the alphabet is
	// the hash of some 20 bytes and Decode refuses nothing more than Nix.
	if _, err := nix32.Decode(hash); err != nil {
		return fmt.Errorf("%s does not begin with a store path hash", clip(s))
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("the name of %s is longer than %d characters", clip(s), maxNameLen)
	}
	// The first component of a name must not be "." or "..".
	for _, dots := range []string{".", ".."} {
		if name == dots || strings.HasPrefix(name, dots+"-") {
			return fmt.Errorf("the name of %s begins with %q", clip(s), dots)
		}
	}
	for i := range len(name) {
		if !nameChar(name[i]) {
			return fmt.Errorf("the name of %s has the character %q", clip(s), name[i])
		}
	}
	return nil
}

// nameChar reports whether c may appear in a store path's name.
func nameChar(c byte) bool {
	switch {
	case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		return true
	}
	return strings.IndexByte("+-._?=", c) >= 0
}

// parseNarHash reads a SHA-256 written as sha256:<nix32>, sha256:<hex> or
// sha256-<base64>.
func parseNarHash(s string) (hash [32]byte, err error) {
	var raw []byte
	if digest, ok := strings.CutPrefix(s, "sha256:"); ok {
		switch len(digest) {
		case nix32.EncodedLen(len(hash)):
			raw, err = nix32.Decode(digest)
		case hex.EncodedLen(len(hash)):
			raw, err = hex.DecodeString(digest)
		default:
			err = errors.New("wrong length")
		}
	} else if digest, ok := strings.CutPrefix(s, "sha256-"); ok {
		raw, err = decodeBase64(digest, len(hash))
	} else {
		return hash, fmt.Errorf("%s is not a SHA-256 (sha256:<base 32 or 16> or sha256-<base 64>)", clip(s))
	}
	if err != nil {
		return hash, fmt.Errorf("%s is not a SHA-256", clip(s))
	}
	copy(hash[:], raw)
	return hash, nil
}

// decodeBase64 decodes the padded base 64 of exactly n bytes.
func decodeBase64(s string, n int) ([]byte, error) {
	// DecodeString skips carriage returns and line feeds; Nix does not.
	if len(s) != base64.StdEncoding.EncodedLen(n) || strings.ContainsAny(s, "\r\n") {
		return nil, fmt.Errorf("not the base 64 of %d bytes", n)
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil || len(raw) != n {
		return nil, fmt.Errorf("not the base 64 of %d bytes", n)
	}
	return raw, nil
}

// parseSize reads a decimal number of bytes: digits and nothing else.
func parseSize(s string) (uint64, error) {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("%s is not a number", clip(s))
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is not a number of 64 bits", clip(s))
	}
	return n, nil
}

// parseReferences splits a References value at its spaces, as Nix does,
// and checks every name.
func parseReferences(s string) ([]string, error) {
	var refs []string
	for ref := range strings.SplitSeq(s, " ") {
		if ref == "" {
			continue
		}
		if err := checkBaseName(ref); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// checkSig checks that s is a key name, a colon and the base 64 of an
// Ed25519 signature. A signature of another shape makes Nix refuse the
// whole narinfo, or fail when it trusts a key of that name.
func checkSig(s string) error {
	name, payload, ok := strings.Cut(s, ":")
	if !ok || name == "" {
		return fmt.Errorf("%s is not <key name>:<base 64>", clip(s))
	}
	if _, err := decodeBase64(payload, sigLen); err != nil {
		return fmt.Errorf("the signature named %s is %v", clip(name), err)
	}
	return nil
}

// checkCA checks that a content address is printable ASCII without
// spaces, which every form Nix knows is. Its meaning is left to Nix, whose
// versions know different forms.
func checkCA(s string) error {
	for i := range len(s) {
		if s[i] <= ' ' || s[i] > '~' {
			return fmt.Errorf("%s is not a content address", clip(s))
		}
	}
	return nil
}

// HashPart returns the 32 characters before the first '-' of the store
// path's base name.
func (i *Info) HashPart() string {
	base := strings.TrimPrefix(i.StorePath, StoreDir+"/")
	hash, _, _ := strings.Cut(base, "-")
	return hash
}

// NarHashNix32 returns the NAR's hash in Nix's base 32, 52 characters.
func (i *Info) NarHashNix32() string {
	return nix32.Encode(i.NarHash[:])
}

// NarURL returns where narrenhaus serves the NAR, relative to the cache:
// "nar/<NarHashNix32>.nar".
func (i *Info) NarURL() string {
	return "nar/" + i.NarHashNix32() + ".nar"
}

// Stored is the text narrenhaus keeps: what Nix writes for an upload
// without compression. The URL is NarURL, Compression is none, FileHash
// and FileSize are the NAR's, and the fields come in Nix's order, each
// line ended by a line feed: StorePath, URL, Compression, FileHash,
// FileSize, NarHash, NarSize, References (present even when empty),
// Deriver if there is one, the Sig lines sorted and each once, CA if
// there is one.
func (i *Info) Stored() []byte {
	return i.text(i.Sigs)
}

// Signed is Stored with one more Sig line, sig, placed so that the Sig
// lines stay sorted. A sig the narinfo already has adds nothing.
func (i *Info) Signed(sig string) []byte {
	sigs := make([]string, 0, len(i.Sigs)+1)
	sigs = append(sigs, i.Sigs...)
	return i.text(append(sigs, sig))
}

// text writes the stored form with the signatures sigs, which it does not
// change.
func (i *Info) text(sigs []string) []byte {
	hash := "sha256:" + i.NarHashNix32()
	size := strconv.FormatUint(i.NarSize, 10)

	var b bytes.Buffer
	field := func(key, value string) {
		b.WriteString(key)
		b.WriteString(": ")
		b.WriteString(value)
		b.WriteByte('\n')
	}
	field("StorePath", i.StorePath)
	field("URL", i.NarURL())
	field("Compression", "none")
	field("FileHash", hash)
	field("FileSize", size)
	field("NarHash", hash)
	field("NarSize", size)
	// An empty list is "References: " with the space: without it Nix
	// reads the next line as the value.
	field("References", strings.Join(i.References, " "))
	if i.Deriver != "" {
		field("Deriver", i.Deriver)
	}
	for _, sig := range sortedSet(sigs) {
		field("Sig", sig)
	}
	if i.CA != "" {
		field("CA", i.CA)
	}
	return b.Bytes()
}

// Fingerprint is what a signature covers:
//
//	1;<StorePath>;sha256:<nix32>;<NarSize>;<references>
//
// with the references as full paths, sorted, each once, joined by commas.
// Nix keeps references as a set, so the order and repeats of the
// References line do not change it.
func (i *Info) Fingerprint() string {
	refs := sortedSet(i.References)
	for n, ref := range refs {
		refs[n] = StoreDir + "/" + ref
	}
	return "1;" + i.StorePath +
		";sha256:" + i.NarHashNix32() +
		";" + strconv.FormatUint(i.NarSize, 10) +
		";" + strings.Join(refs, ",")
}

// sortedSet returns the strings of s in byte order, each once, in a slice
// of its own.
func sortedSet(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return slices.Compact(out)
}
