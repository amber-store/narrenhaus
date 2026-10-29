package decompress

import (
	"bytes"
	"compress/bzip2"
	"encoding/base64"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"

	"github.com/amber-store/narrenhaus/nar"
)

// smallNAR is the archive of a directory with a script, a file and a
// symlink: the same bytes every time.
func smallNAR(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := nar.NewWriter(&buf)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("writing the archive: %v", err)
		}
	}
	put := func(name string, executable bool, body string) {
		t.Helper()
		f, err := w.Regular(name, executable, uint64(len(body)))
		must(err)
		_, err = io.WriteString(f, body)
		must(err)
	}
	must(w.Directory(""))
	must(w.Directory("bin"))
	put("hello", true, "#!/bin/sh\necho hello\n")
	must(w.Leave())
	must(w.Symlink("latest", "bin/hello"))
	put("readme", false, "narrenhaus keeps a NAR as chunks.\n")
	must(w.Leave())
	must(w.Close())
	return buf.Bytes()
}

// smallNARBzip2 is smallNAR as `bzip2 -9` wrote it: the standard library
// reads bzip2 and does not write it.
const smallNARBzip2 = "QlpoOTFBWSZTWe4kuH8AAQf/gH/yAgB4Y6AAIAEQAD7v32AwARiTCUQ1EZDT1PUNANBoB6mhgAaNDRhGTQaNNAYJ" +
	"FFTybIA1NRoANHqDR6nkhMPi6HE2YaoNyC0kILc8xmIWvLINaQCNE4IyA0xCC7nNgkGjSSRU+jE2kkjZr8abSM+x" +
	"igCQMGqQQQSkw8HN7VdOb5uhblZECzB/O46X9b1146A+FMHtDPkO8baqWHJEHDeg+hNCt2FW1TVwcWItYRMyzfCB" +
	"bKTSmk0jIy4it7lnClyktQn7bqxFbhKAcjqFYr7gwKE0V3KVozSJacEco3x1pOXGFccCzAgKKNTKqrEsxFO9Dhdy" +
	"RThQkO4kuH8="

// noise returns n bytes that do not compress, the same for the same seed.
func noise(seed byte, n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

func xzOf(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := xz.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zstdOf(t *testing.T, data []byte) []byte {
	t.Helper()
	w, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	return w.EncodeAll(data, nil)
}

func gzipOf(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func bzip2Of(t *testing.T) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(smallNARBzip2)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// read returns everything Reader gives for body.
func read(t *testing.T, body io.Reader) (data []byte, compressed bool) {
	t.Helper()
	out, compressed, err := Reader(body)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	data, err = io.ReadAll(out)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	return data, compressed
}

func TestRecognises(t *testing.T) {
	archive := smallNAR(t)

	// The fixture is of this archive: the standard library reads it too.
	plain, err := io.ReadAll(bzip2.NewReader(bytes.NewReader(bzip2Of(t))))
	if err != nil || !bytes.Equal(plain, archive) {
		t.Fatalf("the bzip2 fixture is not the archive of the test (%v)", err)
	}

	for _, c := range []struct {
		name       string
		body       []byte
		compressed bool
	}{
		{"xz", xzOf(t, archive), true},
		{"zstd", zstdOf(t, archive), true},
		{"gzip", gzipOf(t, archive), true},
		{"bzip2", bzip2Of(t), true},
		{"none", archive, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.compressed && bytes.Equal(c.body, archive) {
				t.Fatal("the fixture is not compressed")
			}
			got, compressed := read(t, bytes.NewReader(c.body))
			if compressed != c.compressed {
				t.Errorf("compressed = %v, want %v", compressed, c.compressed)
			}
			if !bytes.Equal(got, archive) {
				t.Errorf("read %d bytes that are not the archive of %d bytes", len(got), len(archive))
			}

			// A body that arrives a byte at a time is the same body.
			got, compressed = read(t, iotest.OneByteReader(bytes.NewReader(c.body)))
			if compressed != c.compressed || !bytes.Equal(got, archive) {
				t.Errorf("a byte at a time: compressed = %v, %d bytes, equal %v", compressed, len(got), bytes.Equal(got, archive))
			}
		})
	}
}

func TestZstdOfSeveralFrames(t *testing.T) {
	// Nix 2.35 ends a frame every 16 MiB; the frames follow one another
	// without anything between them.
	data := noise(1, 300<<10)
	var body []byte
	for _, part := range [][]byte{data[:100<<10], data[100<<10 : 100<<10+1], nil, data[100<<10+1:]} {
		body = append(body, zstdOf(t, part)...)
	}
	got, compressed := read(t, bytes.NewReader(body))
	if !compressed {
		t.Error("compressed = false")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want the %d of the four frames", len(got), len(data))
	}
}

func TestSeveralStreams(t *testing.T) {
	// What xz, gzip and bzip2 write one after another reads as one stream,
	// as their own programs read it.
	a, b := noise(2, 70<<10), noise(3, 10<<10)
	want := append(bytes.Clone(a), b...)
	for _, c := range []struct {
		name string
		of   func(*testing.T, []byte) []byte
	}{{"xz", xzOf}, {"gzip", gzipOf}} {
		got, _ := read(t, bytes.NewReader(append(c.of(t, a), c.of(t, b)...)))
		if !bytes.Equal(got, want) {
			t.Errorf("%s: read %d bytes, want %d", c.name, len(got), len(want))
		}
	}
	archive := smallNAR(t)
	got, _ := read(t, bytes.NewReader(append(bzip2Of(t), bzip2Of(t)...)))
	if !bytes.Equal(got, append(bytes.Clone(archive), archive...)) {
		t.Errorf("bzip2: read %d bytes, want twice %d", len(got), len(archive))
	}
}

func TestUnsupported(t *testing.T) {
	archive := smallNAR(t)
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"one byte", []byte{0x0d}},
		// Brotli has no magic number; this is what it makes of "hello\n".
		{"brotli", []byte{0x8b, 0x02, 0x80, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a, 0x03}},
		{"lzip", append([]byte("LZIP\x01\x0c"), noise(4, 64)...)},
		{"lz4", append([]byte{0x04, 0x22, 0x4d, 0x18}, noise(5, 64)...)},
		{"text", []byte("StorePath: /nix/store/0c6kzph7l0dcbfmjap64f0czdafn3b7x-hello\n")},
		{"the header of an archive cut short", archive[:20]},
		{"an archive of another version", bytes.Replace(archive, []byte("nix-archive-1"), []byte("nix-archive-2"), 1)},
		{"a string of thirteen bytes that is no archive's", append([]byte{13, 0, 0, 0, 0, 0, 0, 0}, "hello, world!\x00\x00\x00"...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, compressed, err := Reader(bytes.NewReader(c.body))
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("Reader: error %v, want ErrUnsupported", err)
			}
			if out != nil || compressed {
				t.Errorf("Reader returned a reader (%v) or compressed = %v with its error", out != nil, compressed)
			}
		})
	}
}

func TestBodyThatFails(t *testing.T) {
	// A body that cannot be read is not a body of an unknown compression:
	// the reader's own error comes back.
	broken := errors.New("the connection is gone")
	_, _, err := Reader(iotest.ErrReader(broken))
	if !errors.Is(err, broken) || errors.Is(err, ErrUnsupported) {
		t.Errorf("a body that fails at once: error %v, want the body's", err)
	}

	archive := smallNAR(t)
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"xz", xzOf(t, archive)},
		{"zstd", zstdOf(t, archive)},
		{"gzip", gzipOf(t, archive)},
		{"bzip2", bzip2Of(t)},
		{"none", archive},
	} {
		t.Run(c.name, func(t *testing.T) {
			// The body ends with an error after its first half.
			half := io.MultiReader(bytes.NewReader(c.body[:len(c.body)/2]), iotest.ErrReader(broken))
			out, _, err := Reader(half)
			if err == nil {
				_, err = io.ReadAll(out)
			}
			if !errors.Is(err, broken) {
				t.Errorf("error %v, want the body's", err)
			}
		})
	}
}

func TestCutShortAndCorrupt(t *testing.T) {
	// A compressed body that ends early, or that is not what its first
	// bytes promise, fails when it is read: never a short archive and no
	// error.
	archive := smallNAR(t)
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"xz", xzOf(t, archive)},
		{"zstd", zstdOf(t, archive)},
		{"gzip", gzipOf(t, archive)},
		{"bzip2", bzip2Of(t)},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, cut := range []int{len(c.body) - 1, len(c.body) / 2, 7} {
				out, _, err := Reader(bytes.NewReader(c.body[:cut]))
				if err == nil {
					_, err = io.ReadAll(out)
				}
				if err == nil || errors.Is(err, ErrUnsupported) {
					t.Errorf("cut at %d of %d: error %v, want a failed read", cut, len(c.body), err)
				}
			}
			out, _, err := Reader(bytes.NewReader(append(bytes.Clone(c.body), "trailing bytes that are no stream"...)))
			if err == nil {
				_, err = io.ReadAll(out)
			}
			if err == nil {
				t.Error("bytes after the stream were read without an error")
			}
		})
	}
}

// Bodies of 19 and 25 bytes as the xz program wrote them when told the
// dictionary to state: a stream states one for each of its blocks, and its
// reader has to hold as much.
const (
	// xz --lzma2=dict=64MiB, the dictionary of xz -9.
	xzDictionary64MiB = "/Td6WFoAAATm1rRGBMAXEyEBHAAAAAAAAAAAAJ35zMkBABJhIE5BUiB3b3VsZCBiZSBoZXJlAACPKcuinFM2xgABMxPFkVNQH7bzfQEAAAAABFla"
	// xz --lzma2=dict=65MiB, which the format rounds up to 96 MiB.
	xzDictionary96MiB = "/Td6WFoAAATm1rRGBMAXEyEBHQAAAAAAAAAAAKOSDiYBABJhIE5BUiB3b3VsZCBiZSBoZXJlAACPKcuinFM2xgABMxPFkVNQH7bzfQEAAAAABFla"
	// xz --lzma2=dict=1536MiB.
	xzDictionary1536MiB = "/Td6WFoAAATm1rRGBMAXEyEBJQAAAAAAAAAAAG9QKl0BABJhIE5BUiB3b3VsZCBiZSBoZXJlAACPKcuinFM2xgABMxPFkVNQH7bzfQEAAAAABFla"
	// xz --filters1=lzma2:dict=1MiB --filters2=lzma2:dict=1536MiB
	// --block-list=1:12,2:0: one stream whose second block states the
	// large dictionary.
	xzSecondBlock1536MiB = "/Td6WFoAAATm1rRGBMAQDCEBEAAAAAAAAAAAADzASiUBAAtmaXJzdCBibG9jaywAk4z/X+yQvy0EwBENIQElAAAAAAAAAAAAs7EJ6wEADCBzZWNvbmQgYmxvY2sA" +
		"AAAAhQ+wEuCxknMAAiwMLQ0AALwCox2xxGf7AgAAAAAEWVo="
)

func TestXzDictionaryLimit(t *testing.T) {
	// The dictionary is memory the body makes its reader hold: 84 bytes can
	// ask for gigabytes. What is above the largest that the xz program's
	// presets write is refused, in whichever block or stream it stands.
	fixture := func(s string) []byte {
		t.Helper()
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	if got, compressed := read(t, bytes.NewReader(fixture(xzDictionary64MiB))); !compressed || string(got) != "a NAR would be here" {
		t.Errorf("a dictionary of 64 MiB: read %q, compressed = %v", got, compressed)
	}

	for _, c := range []struct {
		name string
		body []byte
	}{
		{"96 MiB", fixture(xzDictionary96MiB)},
		{"1536 MiB", fixture(xzDictionary1536MiB)},
		{"1536 MiB in the second block", fixture(xzSecondBlock1536MiB)},
		{"1536 MiB in the second stream", append(xzOf(t, smallNAR(t)), fixture(xzDictionary1536MiB)...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			out, _, err := Reader(bytes.NewReader(c.body))
			if err == nil {
				_, err = io.ReadAll(out)
			}
			runtime.ReadMemStats(&after)
			if err == nil || errors.Is(err, ErrUnsupported) {
				t.Errorf("error %v, want a failed read", err)
			}
			// Refused before the memory is asked for, not after.
			if grown := after.TotalAlloc - before.TotalAlloc; grown > maxXzDictionary {
				t.Errorf("reading %d bytes allocated %d MiB", len(c.body), grown>>20)
			}
		})
	}
}

func TestZstdWindowLimit(t *testing.T) {
	// A frame of nine bytes: the magic number, a header that states the
	// window and nothing else, and one last block that is empty.
	frame := func(window byte) []byte {
		return []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, window, 0x01, 0x00, 0x00}
	}
	// 2^27 bytes, the limit.
	if got, compressed := read(t, bytes.NewReader(frame(0x88))); !compressed || len(got) != 0 {
		t.Errorf("a window of 128 MiB: read %d bytes, compressed = %v", len(got), compressed)
	}
	// 2^28 bytes, which the decoder would read if it were left to its own
	// limit.
	out, _, err := Reader(bytes.NewReader(frame(0x90)))
	if err == nil {
		_, err = io.ReadAll(out)
	}
	if err == nil || errors.Is(err, ErrUnsupported) {
		t.Errorf("a window of 256 MiB: error %v, want a failed read", err)
	}
}

func TestReadsWhatTheProgramsWrite(t *testing.T) {
	// The streams of the programs Nix's library is made of, in the shapes
	// that the Go writers above do not produce: xz in several blocks, as
	// written with threads, and zstd with a checksum and a long window.
	data := append(noise(6, 3<<20), bytes.Repeat([]byte("narrenhaus "), 200_000)...)
	for _, c := range []struct {
		program string
		args    []string
	}{
		{"xz", []string{"-c", "-T2", "--block-size=1MiB", "-1"}},
		{"xz", []string{"-c", "-9"}},
		{"zstd", []string{"-c", "-q", "--long=27", "-3"}},
		{"zstd", []string{"-c", "-q", "-T2", "-19"}},
		{"gzip", []string{"-c", "-9"}},
		{"bzip2", []string{"-c", "-1"}},
	} {
		t.Run(c.program+" "+c.args[len(c.args)-1], func(t *testing.T) {
			path, err := exec.LookPath(c.program)
			if err != nil {
				t.Skipf("no %s on PATH", c.program)
			}
			cmd := exec.Command(path, c.args...)
			cmd.Stdin = bytes.NewReader(data)
			cmd.Stderr = os.Stderr
			body, err := cmd.Output()
			if err != nil {
				t.Skipf("%s %v: %v", c.program, c.args, err)
			}
			got, compressed := read(t, bytes.NewReader(body))
			if !compressed {
				t.Error("compressed = false")
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("read %d bytes that are not the %d that were compressed", len(got), len(data))
			}
		})
	}
}
