package narinfo

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/amber-store/narrenhaus/nix32"
)

// nixWrote is a narinfo as cache.nixos.org serves it: an xz URL, two
// references of which one is the path itself, a deriver and one signature.
const nixWrote = `StorePath: /nix/store/1afzp6fpynhjhz6msfa9app1z1vpm2n3-coreutils-9.5
URL: nar/0ic7zmg3652sajsdksw5smc9a65m22flz92ch93hxng9rgszkrdf.nar.xz
Compression: xz
FileHash: sha256:0ic7zmg3652sajsdksw5smc9a65m22flz92ch93hxng9rgszkrdf
FileSize: 504612
NarHash: sha256:05mqnvzbhhjyi4prg1d3yqai1q7jdcj3b8hqlj74k8l314rv718n
NarSize: 1570488
References: 1afzp6fpynhjhz6msfa9app1z1vpm2n3-coreutils-9.5 264l5za80368irngs493hb9rljiz6am9-gmp-with-cxx-6.3.0
Deriver: f8cqhzm9xf3im3zyl48xyr47xfv3ggkr-coreutils-9.5.drv
Sig: cache.nixos.org-1:j5lY0T9K/LTEHamVVxt0DVFlvX1z5dEC1N6L+xmr8s+SHxhXEgNeNWdVPf2NnPdHcy8XXo2VUjGr2uuauvPdBg==
`

// The NAR hash of nixWrote in the three forms Nix reads, as `nix hash
// convert` prints them.
const (
	narHashNix32 = "05mqnvzbhhjyi4prg1d3yqai1q7jdcj3b8hqlj74k8l314rv718n"
	narHashHex   = "1685b3330983a2498ea418a235246bf2e01015f6a385972f895e42b8feb6b816"
	narHashSRI   = "sha256-FoWzMwmDokmOpBiiNSRr8uAQFfajhZcviV5CuP62uBY="
)

const (
	nixWroteSig = "cache.nixos.org-1:j5lY0T9K/LTEHamVVxt0DVFlvX1z5dEC1N6L+xmr8s+SHxhXEgNeNWdVPf2NnPdHcy8XXo2VUjGr2uuauvPdBg=="
	// The public key of cache.nixos.org, as Nix ships it.
	nixosPublicKey = "6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
)

// Store path base names and signatures the tests build narinfos from.
const (
	pathA = "0c6kzph7l0dcbfmjap64f0czdafn3b7x-a"
	pathB = "264l5za80368irngs493hb9rljiz6am9-b"
	pathC = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-c"
)

// sigNamed returns a well-formed signature line value under name; the
// signature itself is 64 bytes of fill.
func sigNamed(name string, fill byte) string {
	raw := make([]byte, ed25519.SignatureSize)
	for i := range raw {
		raw[i] = fill
	}
	return name + ":" + base64.StdEncoding.EncodeToString(raw)
}

// minimal is the least a narinfo must say, with the lines of extra after
// it.
func minimal(extra ...string) string {
	lines := []string{
		"StorePath: " + StoreDir + "/" + pathA,
		"NarHash: sha256:" + narHashNix32,
		"NarSize: 120",
	}
	lines = append(lines, extra...)
	return strings.Join(lines, "\n") + "\n"
}

func mustParse(t *testing.T, text string) *Info {
	t.Helper()
	info, err := Parse([]byte(text))
	if err != nil {
		t.Fatalf("Parse: %v\n%s", err, text)
	}
	return info
}

func wantHash(t *testing.T) [32]byte {
	t.Helper()
	raw, err := hex.DecodeString(narHashHex)
	if err != nil {
		t.Fatal(err)
	}
	return [32]byte(raw)
}

func TestParseWhatNixWrites(t *testing.T) {
	got := mustParse(t, nixWrote)
	want := &Info{
		StorePath: "/nix/store/1afzp6fpynhjhz6msfa9app1z1vpm2n3-coreutils-9.5",
		NarHash:   wantHash(t),
		NarSize:   1570488,
		References: []string{
			"1afzp6fpynhjhz6msfa9app1z1vpm2n3-coreutils-9.5",
			"264l5za80368irngs493hb9rljiz6am9-gmp-with-cxx-6.3.0",
		},
		Deriver:     "f8cqhzm9xf3im3zyl48xyr47xfv3ggkr-coreutils-9.5.drv",
		Sigs:        []string{nixWroteSig},
		URL:         "nar/0ic7zmg3652sajsdksw5smc9a65m22flz92ch93hxng9rgszkrdf.nar.xz",
		Compression: "xz",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Parse:\n got %+v\nwant %+v", got, want)
	}
	if got, want := got.HashPart(), "1afzp6fpynhjhz6msfa9app1z1vpm2n3"; got != want {
		t.Errorf("HashPart = %q, want %q", got, want)
	}
	if got := got.NarHashNix32(); got != narHashNix32 {
		t.Errorf("NarHashNix32 = %q, want %q", got, narHashNix32)
	}
	if got, want := got.NarURL(), "nar/"+narHashNix32+".nar"; got != want {
		t.Errorf("NarURL = %q, want %q", got, want)
	}
}

func TestParseNarHashForms(t *testing.T) {
	want := wantHash(t)
	for _, form := range []string{
		"sha256:" + narHashNix32,
		"sha256:" + narHashHex,
		"sha256:" + strings.ToUpper(narHashHex),
		narHashSRI,
	} {
		text := strings.Replace(nixWrote, "sha256:"+narHashNix32, form, 1)
		info, err := Parse([]byte(text))
		if err != nil {
			t.Errorf("NarHash %q: %v", form, err)
			continue
		}
		if info.NarHash != want {
			t.Errorf("NarHash %q parsed as %x, want %x", form, info.NarHash, want)
		}
	}

	md5 := "d41d8cd98f00b204e9800998ecf8427e"
	sha512 := strings.Repeat("0", 128)
	for _, form := range []string{
		"md5:" + md5,
		"sha512:" + sha512,
		"sha1:" + strings.Repeat("0", 40),
		"sha512-" + base64.StdEncoding.EncodeToString(make([]byte, 64)),
		narHashNix32,                                // no algorithm
		"sha256:",                                   // no digest
		"sha256:" + narHashNix32[1:],                // 51 characters
		"sha256:" + narHashHex[2:],                  // 31 bytes of hex
		"sha256:" + "z" + narHashNix32[1:],          // bits beyond the 256th
		"sha256:" + "e" + narHashNix32[1:],          // not in Nix's alphabet
		"sha256:" + narHashHex[:63] + "g",           // not hex
		"sha256-" + narHashHex,                      // SRI wants base 64
		"sha256-" + narHashSRI[7:len(narHashSRI)-1], // padding cut off
		"SHA256:" + narHashNix32,
		" sha256:" + narHashNix32,
	} {
		text := strings.Replace(nixWrote, "sha256:"+narHashNix32, form, 1)
		_, err := Parse([]byte(text))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("NarHash %q: error %v, want ErrMalformed", form, err)
		}
	}
}

func TestStored(t *testing.T) {
	const want = "StorePath: /nix/store/1afzp6fpynhjhz6msfa9app1z1vpm2n3-coreutils-9.5\n" +
		"URL: nar/05mqnvzbhhjyi4prg1d3yqai1q7jdcj3b8hqlj74k8l314rv718n.nar\n" +
		"Compression: none\n" +
		"FileHash: sha256:05mqnvzbhhjyi4prg1d3yqai1q7jdcj3b8hqlj74k8l314rv718n\n" +
		"FileSize: 1570488\n" +
		"NarHash: sha256:05mqnvzbhhjyi4prg1d3yqai1q7jdcj3b8hqlj74k8l314rv718n\n" +
		"NarSize: 1570488\n" +
		"References: 1afzp6fpynhjhz6msfa9app1z1vpm2n3-coreutils-9.5 264l5za80368irngs493hb9rljiz6am9-gmp-with-cxx-6.3.0\n" +
		"Deriver: f8cqhzm9xf3im3zyl48xyr47xfv3ggkr-coreutils-9.5.drv\n" +
		"Sig: cache.nixos.org-1:j5lY0T9K/LTEHamVVxt0DVFlvX1z5dEC1N6L+xmr8s+SHxhXEgNeNWdVPf2NnPdHcy8XXo2VUjGr2uuauvPdBg==\n"
	if got := string(mustParse(t, nixWrote).Stored()); got != want {
		t.Fatalf("Stored:\n got %q\nwant %q", got, want)
	}

	// The same narinfo with the hash in another form is stored the same.
	sri := strings.Replace(nixWrote, "sha256:"+narHashNix32, narHashSRI, 1)
	if got := string(mustParse(t, sri).Stored()); got != want {
		t.Fatalf("Stored of the SRI form:\n got %q\nwant %q", got, want)
	}
}

// Every field Stored writes, in Nix's order: the signatures sorted and
// the content address last.
func TestStoredAllFields(t *testing.T) {
	const ca = "fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s"
	sigZ, sigA := sigNamed("zeta-1", 1), sigNamed("alpha-1", 2)
	text := "CA: " + ca + "\n" +
		"Sig: " + sigZ + "\n" +
		"Sig: " + sigA + "\n" +
		"Sig: " + sigZ + "\n" +
		"Deriver: " + pathC + ".drv\n" +
		"References: " + pathB + " " + pathA + "\n" +
		"NarSize: 120\n" +
		"NarHash: sha256:" + narHashHex + "\n" +
		"StorePath: /nix/store/" + pathA + "\n"
	want := "StorePath: /nix/store/" + pathA + "\n" +
		"URL: nar/" + narHashNix32 + ".nar\n" +
		"Compression: none\n" +
		"FileHash: sha256:" + narHashNix32 + "\n" +
		"FileSize: 120\n" +
		"NarHash: sha256:" + narHashNix32 + "\n" +
		"NarSize: 120\n" +
		"References: " + pathB + " " + pathA + "\n" +
		"Deriver: " + pathC + ".drv\n" +
		"Sig: " + sigA + "\n" +
		"Sig: " + sigZ + "\n" +
		"CA: " + ca + "\n"
	info := mustParse(t, text)
	if got := string(info.Stored()); got != want {
		t.Fatalf("Stored:\n got %q\nwant %q", got, want)
	}
	// Sigs holds what was given; only the text is sorted.
	if want := []string{sigZ, sigA, sigZ}; !slices.Equal(info.Sigs, want) {
		t.Errorf("Sigs = %q, want %q", info.Sigs, want)
	}
	if info.CA != ca {
		t.Errorf("CA = %q, want %q", info.CA, ca)
	}
}

func TestStoredEmptyReferences(t *testing.T) {
	for name, text := range map[string]string{
		"no line":    minimal(),
		"empty line": minimal("References: "),
	} {
		info := mustParse(t, text)
		if len(info.References) != 0 {
			t.Errorf("%s: References = %q, want none", name, info.References)
		}
		stored := string(info.Stored())
		if !strings.Contains(stored, "\nReferences: \n") {
			t.Errorf("%s: Stored has no \"References: \" line with its trailing space:\n%q", name, stored)
		}
	}
}

func TestStoredOmitsUnknownDeriver(t *testing.T) {
	for name, text := range map[string]string{
		"no line":         minimal(),
		"unknown-deriver": minimal("Deriver: unknown-deriver"),
	} {
		info := mustParse(t, text)
		if info.Deriver != "" {
			t.Errorf("%s: Deriver = %q, want none", name, info.Deriver)
		}
		if stored := string(info.Stored()); strings.Contains(stored, "Deriver") {
			t.Errorf("%s: Stored has a Deriver line:\n%q", name, stored)
		}
	}
}

func TestStoredParsesBackEqual(t *testing.T) {
	for name, text := range map[string]string{
		"what Nix writes": nixWrote,
		"minimal":         minimal(),
		"all fields": minimal(
			"References: "+pathA+" "+pathC+" "+pathB,
			"Deriver: "+pathC+".drv",
			"Sig: "+sigNamed("alpha-1", 2),
			"Sig: "+sigNamed("zeta-1", 1),
			"CA: fixed:r:sha256:"+narHashNix32,
			"System: x86_64-linux",
		),
	} {
		info := mustParse(t, text)
		stored := info.Stored()
		back, err := Parse(stored)
		if err != nil {
			t.Errorf("%s: Parse(Stored): %v\n%s", name, err, stored)
			continue
		}
		if back.URL != info.NarURL() || back.Compression != "none" {
			t.Errorf("%s: stored URL %q and Compression %q, want %q and \"none\"",
				name, back.URL, back.Compression, info.NarURL())
		}
		// Stored ignores these two; everything else must come back.
		back.URL, back.Compression = info.URL, info.Compression
		if !reflect.DeepEqual(back, info) {
			t.Errorf("%s: Parse(Stored):\n got %+v\nwant %+v", name, back, info)
		}
		if again := back.Stored(); string(again) != string(stored) {
			t.Errorf("%s: Stored is not stable:\n got %q\nwant %q", name, again, stored)
		}
	}
}

func TestFingerprint(t *testing.T) {
	info := mustParse(t, minimal("References: "+pathC+" "+pathA+" "+pathB+" "+pathA))
	want := "1;/nix/store/" + pathA + ";sha256:" + narHashNix32 + ";120;" +
		"/nix/store/" + pathA + ",/nix/store/" + pathB + ",/nix/store/" + pathC
	if got := info.Fingerprint(); got != want {
		t.Errorf("Fingerprint:\n got %q\nwant %q", got, want)
	}
	// References stay as they were given.
	if want := []string{pathC, pathA, pathB, pathA}; !slices.Equal(info.References, want) {
		t.Errorf("References after Fingerprint = %q, want %q", info.References, want)
	}

	none := mustParse(t, minimal())
	want = "1;/nix/store/" + pathA + ";sha256:" + narHashNix32 + ";120;"
	if got := none.Fingerprint(); got != want {
		t.Errorf("Fingerprint without references:\n got %q\nwant %q", got, want)
	}
}

// The signature cache.nixos.org made over nixWrote is one over what
// Fingerprint returns.
func TestFingerprintIsWhatNixSigned(t *testing.T) {
	public, err := base64.StdEncoding.DecodeString(nixosPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, payload, _ := strings.Cut(nixWroteSig, ":")
	sig, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	info := mustParse(t, nixWrote)
	if !ed25519.Verify(public, []byte(info.Fingerprint()), sig) {
		t.Fatalf("the signature of cache.nixos.org does not verify over %q", info.Fingerprint())
	}
	// And still over the stored form, whose URL and compression differ.
	if back := mustParse(t, string(info.Stored())); !ed25519.Verify(public, []byte(back.Fingerprint()), sig) {
		t.Fatalf("the signature does not verify over the stored form: %q", back.Fingerprint())
	}
}

func sigLines(text []byte) []string {
	var sigs []string
	for line := range strings.SplitSeq(string(text), "\n") {
		if sig, ok := strings.CutPrefix(line, "Sig: "); ok {
			sigs = append(sigs, sig)
		}
	}
	return sigs
}

func TestSigned(t *testing.T) {
	key, err := LoadOrCreateSigningKey(t.TempDir() + "/key")
	if err != nil {
		t.Fatal(err)
	}
	info := mustParse(t, nixWrote)
	sig := key.Sign(info.Fingerprint())
	signed := info.Signed(sig)

	// Stored, with the one line more after the signature that sorts
	// before it.
	stored := string(info.Stored())
	if want := stored + "Sig: " + sig + "\n"; string(signed) != want {
		t.Fatalf("Signed:\n got %q\nwant %q", signed, want)
	}
	if len(info.Sigs) != 1 {
		t.Errorf("Signed changed Sigs: %q", info.Sigs)
	}

	// What a reader sees: the narinfo, and a signature that verifies
	// under the key's public half.
	back := mustParse(t, string(signed))
	if !slices.Contains(back.Sigs, sig) {
		t.Fatalf("the signed narinfo does not carry %q: %q", sig, back.Sigs)
	}
	name, publicText, _ := strings.Cut(key.Public(), ":")
	public, err := base64.StdEncoding.DecodeString(publicText)
	if err != nil {
		t.Fatal(err)
	}
	sigName, payload, _ := strings.Cut(sig, ":")
	if sigName != name {
		t.Errorf("the signature is named %q, the key %q", sigName, name)
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(public, []byte(back.Fingerprint()), raw) {
		t.Fatal("the added signature does not verify over the fingerprint")
	}
}

func TestSignedKeepsTheLinesSorted(t *testing.T) {
	info := mustParse(t, minimal(
		"Sig: "+sigNamed("zeta-1", 1),
		"Sig: "+sigNamed("beta-1", 2),
		"CA: text:sha256:"+narHashNix32,
	))
	for _, name := range []string{"alpha-1", "gamma-1", "zeta-2"} {
		sig := sigNamed(name, 3)
		signed := info.Signed(sig)
		got := sigLines(signed)
		want := []string{sigNamed("zeta-1", 1), sigNamed("beta-1", 2), sig}
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: Sig lines\n got %q\nwant %q", name, got, want)
		}
		if !strings.HasSuffix(string(signed), "\nCA: text:sha256:"+narHashNix32+"\n") {
			t.Errorf("%s: CA is not the last line:\n%q", name, signed)
		}
	}

	// A signature that is there already is not written twice.
	if got, want := string(info.Signed(sigNamed("beta-1", 2))), string(info.Stored()); got != want {
		t.Errorf("Signed with a signature it has:\n got %q\nwant %q", got, want)
	}
}

func TestParseRefuses(t *testing.T) {
	goodSig := sigNamed("alpha-1", 1)
	longName := strings.Repeat("n", 212)
	cases := map[string]string{
		"empty":                  "",
		"missing StorePath":      "NarHash: sha256:" + narHashNix32 + "\nNarSize: 120\n",
		"missing NarHash":        "StorePath: /nix/store/" + pathA + "\nNarSize: 120\n",
		"missing NarSize":        "StorePath: /nix/store/" + pathA + "\nNarHash: sha256:" + narHashNix32 + "\n",
		"StorePath outside":      strings.Replace(minimal(), "/nix/store/", "/gnu/store/", 1),
		"StorePath relative":     strings.Replace(minimal(), "/nix/store/", "", 1),
		"StorePath nested":       strings.Replace(minimal(), "/nix/store/", "/nix/store/sub/", 1),
		"StorePath not clean":    strings.Replace(minimal(), "/nix/store/", "/nix/store//", 1),
		"StorePath is the store": strings.Replace(minimal(), "/nix/store/"+pathA, "/nix/store", 1),
		"hash part of 31":        strings.Replace(minimal(), pathA, pathA[1:], 1),
		"hash part of 33":        strings.Replace(minimal(), pathA, "0"+pathA, 1),
		"hash part with e":       strings.Replace(minimal(), pathA, "e"+pathA[1:], 1),
		"hash part uppercase":    strings.Replace(minimal(), pathA, "A"+pathA[1:], 1),
		"no name":                strings.Replace(minimal(), pathA, pathA[:33], 1),
		"no dash":                strings.Replace(minimal(), pathA, pathA[:32], 1),
		"name with a space":      strings.Replace(minimal(), pathA, pathA+" x", 1),
		"name with a slash":      strings.Replace(minimal(), pathA, pathA+"/x", 1),
		"name is a dot":          strings.Replace(minimal(), pathA, pathA[:33]+".", 1),
		"name is two dots":       strings.Replace(minimal(), pathA, pathA[:33]+"..", 1),
		"name starts dot dash":   strings.Replace(minimal(), pathA, pathA[:33]+".-x", 1),
		"name of 212":            strings.Replace(minimal(), pathA, pathA[:33]+longName, 1),
		"carriage returns":       strings.ReplaceAll(minimal(), "\n", "\r\n"),
		"NarSize 0":              strings.Replace(minimal(), "NarSize: 120", "NarSize: 0", 1),
		"NarSize negative":       strings.Replace(minimal(), "NarSize: 120", "NarSize: -120", 1),
		"NarSize signed":         strings.Replace(minimal(), "NarSize: 120", "NarSize: +120", 1),
		"NarSize empty":          strings.Replace(minimal(), "NarSize: 120", "NarSize: ", 1),
		"NarSize not a number":   strings.Replace(minimal(), "NarSize: 120", "NarSize: 12o", 1),
		"NarSize hexadecimal":    strings.Replace(minimal(), "NarSize: 120", "NarSize: 0x78", 1),
		"NarSize with a space":   strings.Replace(minimal(), "NarSize: 120", "NarSize:  120", 1),
		"NarSize beyond 64 bits": strings.Replace(minimal(), "NarSize: 120", "NarSize: 18446744073709551616", 1),
		"two References lines":   minimal("References: "+pathA, "References: "+pathB),
		"two empty References":   minimal("References: ", "References: "),
		"reference a full path":  minimal("References: /nix/store/" + pathA),
		"reference no name":      minimal("References: " + pathA[:32]),
		"reference tabbed":       minimal("References: " + pathA + "\t" + pathB),
		"two CA lines":           minimal("CA: text:sha256:"+narHashNix32, "CA: text:sha256:"+narHashNix32),
		"CA with a space":        minimal("CA: text:sha256:" + narHashNix32 + " x"),
		"CA not text":            minimal("CA: text:sha256:\x00"),
		"deriver a full path":    minimal("Deriver: /nix/store/" + pathC + ".drv"),
		"deriver empty":          minimal("Deriver: "),
		"sig without a name":     minimal("Sig: " + goodSig[strings.Index(goodSig, ":"):]),
		"sig without a colon":    minimal("Sig: alpha-1"),
		"sig empty":              minimal("Sig: "),
		"sig not base 64":        minimal("Sig: alpha-1:!!!!"),
		"sig of 63 bytes":        minimal("Sig: alpha-1:" + base64.StdEncoding.EncodeToString(make([]byte, 63))),
		"no final line feed":     strings.TrimSuffix(minimal(), "\n"),
		"line without a colon":   minimal("garbage"),
		"empty line":             minimal("", "System: x86_64-linux"),
		"no space after colon":   minimal("System:x86_64-linux"),
		"colon ends the line":    minimal("System:"),
		"line starts with colon": minimal(": x"),
	}
	for name, text := range cases {
		info, err := Parse([]byte(text))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: error %v, want ErrMalformed", name, err)
		}
		if info != nil {
			t.Errorf("%s: Parse returned an Info with its error", name)
		}
	}
}

// What Parse must take: the longest name, a reference list with more than
// one space between names, and repeated single-valued keys, of which Nix
// keeps the last.
func TestParseAccepts(t *testing.T) {
	long := pathA[:33] + strings.Repeat("n", 211)
	if info := mustParse(t, strings.Replace(minimal(), pathA, long, 1)); info.StorePath != "/nix/store/"+long {
		t.Errorf("StorePath = %q", info.StorePath)
	}

	odd := pathA[:33] + "A.z_0+9-x?y=1"
	if info := mustParse(t, strings.Replace(minimal(), pathA, odd, 1)); info.StorePath != "/nix/store/"+odd {
		t.Errorf("StorePath = %q", info.StorePath)
	}

	info := mustParse(t, minimal("References:  "+pathA+"   "+pathB+" "))
	if want := []string{pathA, pathB}; !slices.Equal(info.References, want) {
		t.Errorf("References = %q, want %q", info.References, want)
	}

	info = mustParse(t, minimal("NarSize: 121", "URL: a", "URL: b", "Compression: xz", "Compression: zstd"))
	if info.NarSize != 121 || info.URL != "b" || info.Compression != "zstd" {
		t.Errorf("repeated keys: NarSize %d, URL %q, Compression %q; want the last of each",
			info.NarSize, info.URL, info.Compression)
	}

	info = mustParse(t, minimal("NarSize: 18446744073709551615"))
	if info.NarSize != 1<<64-1 {
		t.Errorf("NarSize = %d", info.NarSize)
	}

	// Nix takes whatever is before the first colon as the key's name.
	spaced := "alpha 1" + sigNamed("", 1)
	info = mustParse(t, minimal("Sig: "+spaced))
	if want := []string{spaced}; !slices.Equal(info.Sigs, want) {
		t.Errorf("Sigs = %q, want %q", info.Sigs, want)
	}

	info = mustParse(t, minimal("CA: "))
	if info.CA != "" {
		t.Errorf("CA = %q, want none", info.CA)
	}
	if stored := string(info.Stored()); strings.Contains(stored, "CA") {
		t.Errorf("Stored has a CA line for an empty one:\n%q", stored)
	}
}

// caTaken returns content addresses in every form Nix 2.35.4 reads and
// gets through checking a path with: text with SHA-256, and fixed with
// each method and the algorithms it has, the digest in each of the three
// encodings Nix tells apart by their lengths.
func caTaken() []string {
	var out []string
	digests := func(size int) []string {
		raw := make([]byte, size)
		for i := range raw {
			raw[i] = byte(i*7 + 1)
		}
		raw[size-1] &= 0x0f // a digest that Nix's base 32 holds exactly
		return []string{
			hex.EncodeToString(raw),
			strings.ToUpper(hex.EncodeToString(raw)),
			nix32.Encode(raw),
			base64.StdEncoding.EncodeToString(raw),
		}
	}
	for _, digest := range digests(32) {
		out = append(out, "text:sha256:"+digest)
	}
	for _, algo := range []struct {
		name string
		size int
	}{{"md5", 16}, {"sha1", 20}, {"sha256", 32}, {"sha512", 64}, {"blake3", 32}} {
		for _, method := range []string{"", "r:", "git:"} {
			if method == "git:" && algo.name != "sha1" && algo.name != "sha256" {
				continue
			}
			for _, digest := range digests(algo.size) {
				out = append(out, "fixed:"+method+algo.name+":"+digest)
			}
		}
	}
	return out
}

const (
	zeros52 = "0000000000000000000000000000000000000000000000000000"
	zeros64 = zeros52 + "000000000000"
	// The base 64 of 32 zero bytes, and the same with bits set that belong
	// to no byte.
	zeros44     = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	zeros44Bits = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB="
)

// caNixRefuses are content addresses that make Nix 2.35.4 refuse the whole
// narinfo.
var caNixRefuses = []string{
	"garbage",
	"text:",
	"fixed:",
	"fixed:r:",
	"fixed:sha256",
	"fixed:sha256:",
	"fixed:r:sha256:",
	"fixed:sha256:000",
	"fixed:sha256:" + zeros52[1:],
	"fixed:sha256:" + zeros52 + "0",
	"fixed:sha256:" + zeros52[1:] + "e", // not in Nix's alphabet
	"fixed:sha256:z" + zeros52[1:],      // bits beyond the 256th
	"fixed:sha256:" + strings.ToUpper("1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s"),
	"fixed:sha256:" + zeros64[1:] + "g",               // not hex
	"fixed:r:sha256:" + zeros44[:43] + "A",            // 33 bytes
	"fixed:r:sha256:" + zeros44[:42] + "==",           // 31 bytes
	"fixed:r:sha256:" + strings.Repeat("-", 43) + "=", // the alphabet of URLs
	"fixed:sha1:" + zeros52,                           // the length of another algorithm
	"fixed:md5:" + zeros52[:32] + "0",
	"fixed:sha3:" + zeros52,
	"fixed:r:r:sha256:" + zeros52,
	"fixed:text:sha256:" + zeros52,
	"text:r:sha256:" + zeros52,
	"text:git:sha256:" + zeros52,
	"FIXED:sha256:" + zeros52,
	"Text:sha256:" + zeros52,
	"fixed:SHA256:" + zeros52,
	"fixed:R:sha256:" + zeros52,
	"fixed:r:sha256-" + zeros44, // SRI
	"fixed:r:sha256:sha256:" + zeros52,
	"fixed:r:sha256:" + zeros52 + ":x",
	"sha256:" + zeros52,
	"source:sha256:" + zeros52,
}

// caNixFailsOver are content addresses Nix 2.35.4 parses and then fails
// over when it checks the path's signatures: with a failed assertion for a
// text address of another algorithm than SHA-256, with an error for a git
// address of another than SHA-1 or SHA-256.
var caNixFailsOver = []string{
	"text:sha1:" + zeros52[:32],
	"text:md5:" + zeros52[:26],
	"text:sha512:" + zeros52 + zeros52[:51],
	"text:blake3:" + zeros52,
	"fixed:git:md5:" + zeros52[:26],
	"fixed:git:sha512:" + zeros52 + zeros52[:51],
	"fixed:git:blake3:" + zeros52,
}

// caReferences are content addresses with the references of their paths,
// to the path itself and to another, and whether Nix 2.35.4 gets through
// checking the path's signatures: a text refers to other paths only (a
// failed assertion otherwise), a NAR hashed with SHA-256 to any, and what
// else is fixed to none.
var caReferences = []struct {
	ca          string
	self, other bool
	ok          bool
}{
	{"text:sha256:" + zeros52, false, true, true},
	{"text:sha256:" + zeros52, true, false, false},
	{"text:sha256:" + zeros52, true, true, false},
	{"fixed:r:sha256:" + zeros52, true, true, true},
	{"fixed:sha256:" + zeros52, false, true, false},
	{"fixed:sha256:" + zeros52, true, false, false},
	{"fixed:r:sha1:" + zeros52[:32], false, true, false},
	{"fixed:r:sha512:" + zeros52 + zeros52[:51], true, true, false},
	{"fixed:git:sha1:" + zeros52[:32], false, true, false},
	{"fixed:git:sha256:" + zeros52, true, false, false},
	{"", true, true, true},
}

func TestParseCA(t *testing.T) {
	for _, ca := range caTaken() {
		info, err := Parse([]byte(minimal("CA: " + ca)))
		if err != nil {
			t.Errorf("CA %q: %v", ca, err)
			continue
		}
		if info.CA != ca {
			t.Errorf("CA %q parsed as %q", ca, info.CA)
		}
		if !strings.HasSuffix(string(info.Stored()), "\nCA: "+ca+"\n") {
			t.Errorf("CA %q is not stored as it came:\n%s", ca, info.Stored())
		}
	}
	// Stricter than Nix, which reads it as zeros: no two texts for one
	// digest, as for a signature.
	stricter := []string{"fixed:r:sha256:" + zeros44Bits}
	for _, ca := range slices.Concat(caNixRefuses, caNixFailsOver, stricter) {
		info, err := Parse([]byte(minimal("CA: " + ca)))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("CA %q: error %v, want ErrMalformed", ca, err)
		}
		if info != nil {
			t.Errorf("CA %q: Parse returned an Info with its error", ca)
		}
	}

	// The references, whichever of the two lines comes first.
	for _, c := range caReferences {
		var names []string
		if c.other {
			names = append(names, pathB)
		}
		if c.self {
			names = append(names, pathA)
		}
		references, ca := "References: "+strings.Join(names, " "), "CA: "+c.ca
		for _, text := range []string{minimal(references, ca), minimal(ca, references)} {
			_, err := Parse([]byte(text))
			if c.ok && err != nil {
				t.Errorf("CA %q with the references %q: %v", c.ca, names, err)
			}
			if !c.ok && !errors.Is(err, ErrMalformed) {
				t.Errorf("CA %q with the references %q: error %v, want ErrMalformed", c.ca, names, err)
			}
		}
	}
}

// TestCAAgainstNix asks the nix on PATH, read-only, about the content
// addresses of TestParseCA: a binary cache of narinfos in a directory of
// the test's own, and Nix's cache of what it read there in another.
func TestCAAgainstNix(t *testing.T) {
	if _, err := exec.LookPath("nix"); err != nil {
		t.Skip("nix is not on PATH")
	}
	cache, home := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(cache, "nix-cache-info"), []byte("StoreDir: "+StoreDir+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// narinfo writes the narinfo of a store path of its own with the
	// content address ca and references to itself and to another path,
	// and returns the path.
	narinfo := func(ca string, self, other bool) string {
		t.Helper()
		sum := sha256.Sum256(fmt.Append(nil, ca, self, other))
		base := nix32.Encode(sum[:20]) + "-probe"
		var references []string
		if other {
			references = append(references, pathB)
		}
		if self {
			references = append(references, base)
		}
		text := "StorePath: " + StoreDir + "/" + base + "\nURL: nar/" + narHashNix32 + ".nar\nCompression: none\n" +
			"NarHash: sha256:" + narHashNix32 + "\nNarSize: 120\nReferences: " + strings.Join(references, " ") + "\nCA: " + ca + "\n"
		if err := os.WriteFile(filepath.Join(cache, base[:hashPartLen]+".narinfo"), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return StoreDir + "/" + base
	}
	// The two forms that are experimental in Nix 2.35 are switched on for
	// the command alone.
	nix := func(args ...string) (string, error) {
		cmd := exec.Command("nix", append([]string{"--extra-experimental-features", "nix-command git-hashing blake3-hashes"}, args...)...)
		cmd.Env = append(os.Environ(), "XDG_CACHE_HOME="+home, "NIX_CONFIG=")
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	store := "file://" + cache
	switch out, err := nix("store", "info", "--store", store); {
	case err != nil:
		t.Skipf("this nix does not read a cache of files: %v\n%s", err, out)
	case strings.Contains(out, "unknown experimental feature"):
		t.Skipf("this nix is older than the forms it is asked about:\n%s", out)
	}

	// What Parse takes, Nix reads, and gets through checking the path's
	// signatures with.
	var taken, failing []string
	for _, ca := range caTaken() {
		taken = append(taken, narinfo(ca, false, false))
	}
	for _, ca := range caNixFailsOver {
		failing = append(failing, narinfo(ca, false, false))
	}
	for _, c := range caReferences {
		if c.ok {
			taken = append(taken, narinfo(c.ca, c.self, c.other))
		} else {
			failing = append(failing, narinfo(c.ca, c.self, c.other))
		}
	}
	if out, err := nix(append([]string{"path-info", "--store", store}, taken...)...); err != nil {
		t.Errorf("nix does not read what Parse takes: %v\n%s", err, out)
	}
	// None of them is signed, and Nix says so of each when it got that far.
	out, err := nix(append([]string{"store", "verify", "--no-contents", "--store", store}, taken...)...)
	if n := strings.Count(out, "is untrusted"); n != len(taken) || strings.Contains(out, "Assertion failed") {
		t.Errorf("nix got through the signatures of %d of the %d paths Parse takes: %v\n%s", n, len(taken), err, out)
	}

	// What Parse refuses, Nix refuses, or reads and then fails over.
	for _, ca := range caNixRefuses {
		if out, err := nix("path-info", "--store", store, narinfo(ca, false, false)); err == nil {
			t.Errorf("nix reads the content address %q, which Parse refuses:\n%s", ca, out)
		}
	}
	for _, path := range failing {
		if out, err := nix("path-info", "--store", store, path); err != nil {
			t.Errorf("nix does not read the narinfo of %s: %v\n%s", path, err, out)
		}
		out, err := nix("store", "verify", "--no-contents", "--store", store, path)
		if err == nil || strings.Contains(out, "is untrusted") {
			t.Errorf("nix gets through the signatures of %s, which Parse refuses: %v\n%s", path, err, out)
		}
	}
}

func TestParseIgnoresUnknownKeys(t *testing.T) {
	plain := mustParse(t, nixWrote)
	with := mustParse(t, "System: x86_64-linux\n"+nixWrote+"System: x86_64-linux\nFileHash: nonsense\nX: \nA key with spaces: a value: with colons\n")
	if !reflect.DeepEqual(with, plain) {
		t.Fatalf("with unknown keys:\n got %+v\nwant %+v", with, plain)
	}
	if strings.Contains(string(with.Stored()), "System") {
		t.Errorf("Stored kept an unknown key:\n%s", with.Stored())
	}
}

// An error ends up in an HTTP body Nix shows only when it is short, so it
// must not grow with what was sent.
func TestErrorsAreShort(t *testing.T) {
	huge := strings.Repeat("x", 1<<16)
	for name, text := range map[string]string{
		"store path":  "StorePath: /nix/store/" + huge + "\n",
		"key":         huge + "\n",
		"hash":        minimal("NarHash: sha256:" + huge),
		"size":        minimal("NarSize: " + huge),
		"reference":   minimal("References: " + huge),
		"deriver":     minimal("Deriver: " + huge),
		"sig":         minimal("Sig: " + huge + ":" + huge),
		"ca":          minimal("CA: " + huge + "\x01"),
		"binary line": minimal(strings.Repeat("\x00", 1<<16)),
	} {
		_, err := Parse([]byte(text))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: error %v, want ErrMalformed", name, err)
			continue
		}
		if n := len(err.Error()); n > 300 {
			t.Errorf("%s: an error of %d bytes", name, n)
		}
		if strings.ContainsAny(err.Error(), "\n\x00\x01") {
			t.Errorf("%s: the error is not one line of text: %q", name, err.Error())
		}
	}
}
