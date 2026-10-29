package nix32

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"strings"
	"testing"
)

func TestEncodeKnownHash(t *testing.T) {
	sum := sha256.Sum256(nil)
	const want = "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73"
	if got := Encode(sum[:]); got != want {
		t.Fatalf("Encode(sha256 of nothing) = %q, want %q", got, want)
	}
	back, err := Decode(want)
	if err != nil {
		t.Fatalf("Decode(%q): %v", want, err)
	}
	if !bytes.Equal(back, sum[:]) {
		t.Fatalf("Decode(%q) = %x, want %x", want, back, sum)
	}
}

// The 20-byte form, which store path hashes have. The expected text is what
// `nix hash convert --hash-algo sha1 --to nix32` prints for the SHA-1 of
// the empty string.
func TestEncodeKnownShortHash(t *testing.T) {
	sum := sha1.Sum(nil)
	const want = "143xibwh31h9bvxzalr0sjvbbvpa6ffs"
	if got := Encode(sum[:]); got != want {
		t.Fatalf("Encode(sha1 of nothing) = %q, want %q", got, want)
	}
}

func TestEncodeSmallInputs(t *testing.T) {
	for _, tc := range []struct {
		in   []byte
		want string
	}{
		{nil, ""},
		{[]byte{}, ""},
		{[]byte{0x00}, "00"},
		{[]byte{0x1f}, "0z"},
		{[]byte{0xff}, "7z"},
		{[]byte{0x01, 0x00}, "0001"},
		{[]byte{0x00, 0x80}, "1000"},
	} {
		if got := Encode(tc.in); got != tc.want {
			t.Errorf("Encode(%x) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	for _, size := range []int{20, 32} {
		for range 200 {
			in := make([]byte, size)
			if _, err := rand.Read(in); err != nil {
				t.Fatal(err)
			}
			s := Encode(in)
			if len(s) != EncodedLen(size) {
				t.Fatalf("Encode of %d bytes has %d characters, want %d", size, len(s), EncodedLen(size))
			}
			if i := strings.IndexFunc(s, func(r rune) bool { return !strings.ContainsRune(alphabet, r) }); i >= 0 {
				t.Fatalf("Encode(%x) = %q has a character outside the alphabet at %d", in, s, i)
			}
			out, err := Decode(s)
			if err != nil {
				t.Fatalf("Decode(%q): %v", s, err)
			}
			if !bytes.Equal(in, out) {
				t.Fatalf("Decode(Encode(%x)) = %x", in, out)
			}
		}
	}
}

// Every length, so that the partial last digit is right at each remainder.
func TestRoundTripEveryLength(t *testing.T) {
	for size := range 70 {
		in := make([]byte, size)
		if _, err := rand.Read(in); err != nil {
			t.Fatal(err)
		}
		s := Encode(in)
		if len(s) != EncodedLen(size) {
			t.Fatalf("Encode of %d bytes has %d characters, want %d", size, len(s), EncodedLen(size))
		}
		out, err := Decode(s)
		if err != nil {
			t.Fatalf("Decode(%q) of %d bytes: %v", s, size, err)
		}
		if !bytes.Equal(in, out) {
			t.Fatalf("Decode(Encode(%x)) = %x", in, out)
		}
	}
}

func TestEncodedLen(t *testing.T) {
	for _, tc := range []struct{ n, want int }{
		{0, 0}, {1, 2}, {2, 4}, {3, 5}, {4, 7}, {5, 8}, {16, 26}, {20, 32}, {32, 52}, {64, 103},
	} {
		if got := EncodedLen(tc.n); got != tc.want {
			t.Errorf("EncodedLen(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestDecodeRejectsForeignCharacters(t *testing.T) {
	const good = "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73"
	if _, err := Decode(good); err != nil {
		t.Fatalf("Decode(%q): %v", good, err)
	}
	for _, foreign := range []string{"e", "o", "u", "t", "A", "Z", "M", " ", "-", "=", "\x00", "é"} {
		// In the middle, at the least significant end and at the most
		// significant end.
		for _, s := range []string{
			good[:20] + foreign + good[21:],
			good[:len(good)-1] + foreign,
			foreign + good[1:],
		} {
			if out, err := Decode(s); err == nil {
				t.Errorf("Decode(%q) = %x, want an error for %q", s, out, foreign)
			}
		}
	}
	if out, err := Decode(strings.ToUpper(good)); err == nil {
		t.Errorf("Decode of the uppercase form = %x, want an error", out)
	}
}

// Two texts must not decode to the same bytes: a hash names a reference,
// and a second spelling of it would name a second one.
func TestDecodeRejectsNonCanonicalInput(t *testing.T) {
	for _, s := range []string{
		// 52 digits carry 260 bits and 32 bytes have 256: the top four
		// bits, which are in the first digit, must be zero.
		"2" + strings.Repeat("0", 51),
		"z" + strings.Repeat("0", 51),
		"2mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73",
		// One byte in two digits leaves two bits over.
		"80",
		"zz",
		// Lengths that no byte count encodes to.
		"0",
		"000",
		"000000",
		strings.Repeat("0", 51),
	} {
		if out, err := Decode(s); err == nil {
			t.Errorf("Decode(%q) = %x, want an error", s, out)
		}
	}
	// And the canonical neighbours do decode.
	for _, s := range []string{"", "00", "7z", strings.Repeat("0", 52), "1" + strings.Repeat("0", 51), strings.Repeat("z", 32)} {
		if _, err := Decode(s); err != nil {
			t.Errorf("Decode(%q): %v", s, err)
		}
	}
}
