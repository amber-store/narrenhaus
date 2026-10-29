// Package nix32 is Nix's base-32 encoding, the form of the hashes in store
// paths, narinfos and NAR URLs.
//
// It is not RFC 4648's: the alphabet leaves out e, o, u and t, and the
// digits are written from the most significant end of the bytes read as
// one little-endian number, so the first character holds the top bits of
// the last byte. A SHA-256 is 52 characters and a store path's 20-byte
// hash is 32.
package nix32

import (
	"errors"
	"fmt"
)

// alphabet is the digits in order of value.
const alphabet = "0123456789abcdfghijklmnpqrsvwxyz"

// invalid marks a byte that is not a digit in values.
const invalid = 0xff

// values maps a byte to the value of the digit it is, or to invalid.
var values = func() (v [256]byte) {
	for i := range v {
		v[i] = invalid
	}
	for i := range len(alphabet) {
		v[alphabet[i]] = byte(i)
	}
	return v
}()

// ErrLength is returned by Decode for a text whose length no byte count
// encodes to.
var ErrLength = errors.New("nix32: no number of bytes is encoded in this many characters")

// EncodedLen returns the number of characters n bytes encode to: 52 for 32
// bytes, 32 for 20.
func EncodedLen(n int) int {
	if n <= 0 {
		return 0
	}
	return (n*8-1)/5 + 1
}

// Encode returns b in Nix's base 32.
func Encode(b []byte) string {
	n := EncodedLen(len(b))
	out := make([]byte, n)
	// Digit d holds bits 5d to 5d+4 of the little-endian number, and is
	// written at n-1-d.
	for d := range n {
		bit := d * 5
		i, shift := bit/8, bit%8
		c := b[i] >> shift
		if i+1 < len(b) {
			c |= b[i+1] << (8 - shift)
		}
		out[n-1-d] = alphabet[c&0x1f]
	}
	return string(out)
}

// Decode returns the bytes s is the encoding of. It is the exact inverse
// of Encode: it refuses a character outside the alphabet (uppercase
// included), a length that no byte count encodes to, and a text whose
// leading digit has bits beyond the last byte, so that no two texts decode
// to the same bytes.
func Decode(s string) ([]byte, error) {
	size := len(s) * 5 / 8
	if EncodedLen(size) != len(s) {
		return nil, fmt.Errorf("%w: %d", ErrLength, len(s))
	}
	out := make([]byte, size)
	for d := range len(s) {
		at := len(s) - 1 - d
		v := values[s[at]]
		if v == invalid {
			return nil, fmt.Errorf("nix32: character %q at %d is not in the alphabet", s[at], at)
		}
		bit := d * 5
		i, shift := bit/8, bit%8
		out[i] |= v << shift
		// What does not fit in this byte goes to the next. When the shift
		// is under four, the whole digit fits.
		over := v >> (8 - shift)
		switch {
		case i+1 < size:
			out[i+1] |= over
		case over != 0:
			return nil, fmt.Errorf("nix32: character %q at %d has bits beyond the last byte", s[at], at)
		}
	}
	return out, nil
}
