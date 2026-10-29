package nar

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

// event is one call of a Handler, or of a Writer.
type event struct {
	op     string // "dir", "leave", "file" or "link"
	name   string
	exec   bool
	body   string
	target string
}

func dir(name string) event          { return event{op: "dir", name: name} }
func leave() event                   { return event{op: "leave"} }
func file(name, body string) event   { return event{op: "file", name: name, body: body} }
func script(name, body string) event { return event{op: "file", name: name, exec: true, body: body} }
func link(name, target string) event { return event{op: "link", name: name, target: target} }

// roundTripArchives is the table of canonical archives. Task B3's round
// trip uses the same archives.
var roundTripArchives = []struct {
	name   string
	events []event
}{
	{"directory root", []event{
		dir(""),
		file("empty", ""),
		file("file", "plain\n"),
		link("link", "file"),
		dir("nothing"),
		leave(),
		script("run", "#!/bin/sh\n"),
		dir("sub"),
		dir("deeper"),
		link("up", "../.."),
		leave(),
		file("eight", "12345678"),
		file("inner", "x"),
		leave(),
		leave(),
	}},
	{"empty directory root", []event{dir(""), leave()}},
	{"file root", []event{file("", "hello\n")}},
	{"empty file root", []event{file("", "")}},
	{"executable file root", []event{script("", "#!/bin/sh\nexit 0\n")}},
	{"symlink root", []event{link("", "/nix/store/0000-target")}},
	{"names in byte order", []event{
		dir(""),
		file("B", "upper"),
		file("a", "lower"),
		file("é", "accent"),
		leave(),
	}},
	{"names that are not UTF-8", []event{
		dir(""),
		file("\x80", "a"),
		link("\xfe\xff", "\xff"),
		leave(),
	}},
}

// recorder is a Handler that keeps what it is told.
type recorder struct {
	events []event
}

func (r *recorder) Directory(name string) error {
	r.events = append(r.events, dir(name))
	return nil
}

func (r *recorder) Leave() error {
	r.events = append(r.events, leave())
	return nil
}

func (r *recorder) Regular(name string, executable bool, size uint64, body io.Reader) error {
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if uint64(len(b)) != size {
		return fmt.Errorf("body of %q has %d bytes, size says %d", name, len(b), size)
	}
	r.events = append(r.events, event{op: "file", name: name, exec: executable, body: string(b)})
	return nil
}

func (r *recorder) Symlink(name, target string) error {
	r.events = append(r.events, link(name, target))
	return nil
}

// replay makes the calls of events on w, without closing it.
func replay(w *Writer, events []event) error {
	for i, e := range events {
		var err error
		switch e.op {
		case "dir":
			err = w.Directory(e.name)
		case "leave":
			err = w.Leave()
		case "file":
			var body io.Writer
			if body, err = w.Regular(e.name, e.exec, uint64(len(e.body))); err == nil {
				_, err = io.WriteString(body, e.body)
			}
		case "link":
			err = w.Symlink(e.name, e.target)
		default:
			panic("unknown event " + e.op)
		}
		if err != nil {
			return fmt.Errorf("event %d (%s %q): %w", i, e.op, e.name, err)
		}
	}
	return nil
}

// write returns the archive a Writer makes of events.
func write(t *testing.T, events []event) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := replay(w, events); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	return buf.Bytes()
}

// The functions below encode an archive by the grammar, independently of
// Writer, so a mistake shared by Writer and Parse does not go unseen.

// str is one string of the format: length, bytes, zero padding to 8.
func str(s string) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(len(s)))
	b = append(b, s...)
	for len(b)%8 != 0 {
		b = append(b, 0)
	}
	return b
}

// length is the 64-bit length that starts a string.
func length(n uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, n)
}

// dirtyPad is str(s) with the last byte of padding set.
func dirtyPad(s string) []byte {
	if len(s)%8 == 0 {
		panic("string without padding")
	}
	b := str(s)
	b[len(b)-1] = 1
	return b
}

func join(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func words(ss ...string) []byte {
	var b []byte
	for _, s := range ss {
		b = append(b, str(s)...)
	}
	return b
}

func archive(node []byte) []byte { return join(str("nix-archive-1"), node) }

func rawFile(body string) []byte {
	return words("(", "type", "regular", "contents", body, ")")
}

func rawScript(body string) []byte {
	return words("(", "type", "regular", "executable", "", "contents", body, ")")
}

func rawLink(target string) []byte {
	return words("(", "type", "symlink", "target", target, ")")
}

func rawDir(entries ...[]byte) []byte {
	return join(words("(", "type", "directory"), join(entries...), str(")"))
}

func rawEntry(name string, node []byte) []byte {
	return join(words("entry", "(", "name", name, "node"), node, str(")"))
}

// rawEvents encodes the archive events describe.
func rawEvents(events []event) []byte {
	var nodes func() []byte
	nodes = func() []byte {
		e := events[0]
		events = events[1:]
		switch e.op {
		case "file":
			if e.exec {
				return rawScript(e.body)
			}
			return rawFile(e.body)
		case "link":
			return rawLink(e.target)
		}
		var entries [][]byte
		for events[0].op != "leave" {
			name := events[0].name
			entries = append(entries, rawEntry(name, nodes()))
		}
		events = events[1:]
		return rawDir(entries...)
	}
	return archive(nodes())
}

// nested is levels directories one inside the other, the root included,
// with leaf in the innermost when leaf is not nil.
func nested(levels int, leaf []byte) []byte {
	var node []byte
	if leaf != nil {
		node = rawDir(rawEntry("leaf", leaf))
	} else {
		node = rawDir()
	}
	for range levels - 1 {
		node = rawDir(rawEntry("d", node))
	}
	return archive(node)
}

func TestRoundTrip(t *testing.T) {
	readers := []struct {
		name string
		wrap func(io.Reader) io.Reader
	}{
		{"whole", func(r io.Reader) io.Reader { return r }},
		{"byte by byte", iotest.OneByteReader},
		{"data with the end", iotest.DataErrReader},
	}
	for _, tc := range roundTripArchives {
		t.Run(tc.name, func(t *testing.T) {
			written := write(t, tc.events)
			if want := rawEvents(tc.events); !bytes.Equal(written, want) {
				t.Fatalf("Writer's archive is not the grammar's:\n got %q\nwant %q", written, want)
			}
			for _, rd := range readers {
				var rec recorder
				if err := Parse(rd.wrap(bytes.NewReader(written)), &rec); err != nil {
					t.Fatalf("%s: Parse: %v", rd.name, err)
				}
				if !reflect.DeepEqual(rec.events, tc.events) {
					t.Fatalf("%s: parsed\n %v\nwant\n %v", rd.name, rec.events, tc.events)
				}
				if again := write(t, rec.events); !bytes.Equal(again, written) {
					t.Fatalf("%s: written again, the archive differs", rd.name)
				}
			}
		})
	}
}

func TestGolden(t *testing.T) {
	want := []byte("" +
		"\x0d\x00\x00\x00\x00\x00\x00\x00nix-archive-1\x00\x00\x00" +
		"\x01\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00" +
		"\x04\x00\x00\x00\x00\x00\x00\x00type\x00\x00\x00\x00" +
		"\x07\x00\x00\x00\x00\x00\x00\x00regular\x00" +
		"\x08\x00\x00\x00\x00\x00\x00\x00contents" +
		"\x06\x00\x00\x00\x00\x00\x00\x00hello\n\x00\x00" +
		"\x01\x00\x00\x00\x00\x00\x00\x00)\x00\x00\x00\x00\x00\x00\x00")
	if len(want) != 120 {
		t.Fatalf("the golden archive has %d bytes, not 120", len(want))
	}
	got := write(t, []event{file("", "hello\n")})
	if !bytes.Equal(got, want) {
		t.Fatalf("archive of a file holding \"hello\\n\":\n got %q\nwant %q", got, want)
	}
	var rec recorder
	if err := Parse(bytes.NewReader(want), &rec); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if want := []event{file("", "hello\n")}; !reflect.DeepEqual(rec.events, want) {
		t.Fatalf("parsed %v, want %v", rec.events, want)
	}
}

// TestGoldenDirectory pins a directory with an executable and a symlink,
// written out token by token.
func TestGoldenDirectory(t *testing.T) {
	want := words(
		"nix-archive-1",
		"(", "type", "directory",
		"entry", "(", "name", "bin", "node",
		"(", "type", "directory",
		"entry", "(", "name", "run", "node",
		"(", "type", "regular", "executable", "", "contents", "#!/bin/sh\n", ")",
		")",
		")",
		")",
		"entry", "(", "name", "link", "node",
		"(", "type", "symlink", "target", "bin/run", ")",
		")",
		")",
	)
	got := write(t, []event{
		dir(""),
		dir("bin"),
		script("run", "#!/bin/sh\n"),
		leave(),
		link("link", "bin/run"),
		leave(),
	})
	if !bytes.Equal(got, want) {
		t.Fatalf("archive differs:\n got %q\nwant %q", got, want)
	}
}

// endless gives zero bytes for ever and counts them.
type endless struct {
	read int
}

func (e *endless) Read(p []byte) (int, error) {
	clear(p)
	e.read += len(p)
	return len(p), nil
}

func TestParseRefuses(t *testing.T) {
	long := func(n int) string { return strings.Repeat("n", n) }
	cases := []struct {
		name    string
		archive []byte
		why     string // part of the error's text
	}{
		{"empty input", nil, `does not begin`},
		{"wrong magic", join(str("nix-archive-2"), rawFile("x")), `does not begin`},
		{"short magic", join(str("nix-archive"), rawFile("x")), `does not begin`},
		{"magic in capitals", join(str("NIX-ARCHIVE-1"), rawFile("x")), `does not begin`},
		{"magic only", str("nix-archive-1"), `ends early`},
		{"no magic", rawFile("x"), `does not begin`},

		{"unsorted names", archive(rawDir(rawEntry("b", rawFile("")), rawEntry("a", rawFile("")))), `must ascend`},
		{"names sorted without regard to case", archive(rawDir(rawEntry("a", rawFile("")), rawEntry("B", rawFile("")))), `must ascend`},
		{"duplicate name", archive(rawDir(rawEntry("a", rawFile("")), rawEntry("a", rawFile("")))), `twice`},
		{"duplicate name of another type", archive(rawDir(rawEntry("a", rawFile("")), rawEntry("a", rawDir()))), `twice`},
		{"shorter name after its extension", archive(rawDir(rawEntry("ab", rawFile("")), rawEntry("a", rawFile("")))), `must ascend`},
		{"long names out of order", archive(rawDir(rawEntry(long(255), rawFile("")), rawEntry(long(254), rawFile("")))), `must ascend`},
		{"long name with a slash", archive(rawDir(rawEntry(long(254)+"/", rawFile("")))), `slash`},
		{"long name of bytes that need escaping", archive(rawDir(rawEntry(strings.Repeat("\xff", 254)+"/", rawFile("")))), `slash`},
		{"empty name", archive(rawDir(rawEntry("", rawFile("")))), `name is empty`},
		{"name .", archive(rawDir(rawEntry(".", rawFile("")))), `named "."`},
		{"name ..", archive(rawDir(rawEntry("..", rawFile("")))), `named ".."`},
		{"name with a slash", archive(rawDir(rawEntry("a/b", rawFile("")))), `slash`},
		{"name that is a slash", archive(rawDir(rawEntry("/", rawFile("")))), `slash`},
		{"name with NUL", archive(rawDir(rawEntry("a\x00b", rawFile("")))), `NUL`},
		{"name of 256 bytes", archive(rawDir(rawEntry(long(256), rawFile("")))), `name has 256 bytes`},

		{"empty symlink target", archive(rawLink("")), `target is empty`},
		{"target with NUL", archive(rawLink("a\x00b")), `NUL`},
		{"target of 4096 bytes", archive(rawLink(long(4096))), `target has 4096 bytes`},
		{"empty target in a directory", archive(rawDir(rawEntry("l", rawLink("")))), `target is empty`},

		{"nesting of 65 levels", nested(65, nil), `deeper than 64`},
		{"a file at the 65th level", nested(64, rawFile("x")), `deeper than 64`},
		{"a symlink at the 65th level", nested(64, rawLink("x")), `deeper than 64`},
		{"nesting of 200 levels", nested(200, nil), `deeper than 64`},

		{"executable with a non-empty value", archive(words("(", "type", "regular", "executable", "x", "contents", "", ")")), `executable marker`},
		{"executable twice", archive(words("(", "type", "regular", "executable", "", "executable", "", "contents", "", ")")), `expected "contents"`},
		{"executable after contents", archive(words("(", "type", "regular", "contents", "", "executable", "", ")")), `expected ")"`},
		{"regular without contents", archive(words("(", "type", "regular", ")")), `expected "contents"`},
		{"regular with a foreign field", archive(words("(", "type", "regular", "foo", ")")), `expected "contents"`},
		{"contents twice", archive(words("(", "type", "regular", "contents", "a", "contents", "b", ")")), `expected ")"`},

		{"non-zero padding in the magic", join(dirtyPad("nix-archive-1"), rawFile("x")), `does not begin`},
		{"non-zero padding in a keyword", archive(join(dirtyPad("("), words("type", "regular", "contents", "x", ")"))), `padding`},
		{"non-zero padding in a file", archive(join(words("(", "type", "regular", "contents"), dirtyPad("x"), str(")"))), `padding`},
		{"non-zero padding in a name", archive(rawDir(join(words("entry", "(", "name"), dirtyPad("a"), str("node"), rawFile(""), str(")")))), `padding`},
		{"non-zero padding in a target", archive(join(words("(", "type", "symlink", "target"), dirtyPad("t"), str(")"))), `padding`},
		{"non-zero padding in the last token", join(archive(words("(", "type", "regular", "contents", "x")), dirtyPad(")")), `padding`},

		{"trailing byte after the root", append(archive(rawFile("x")), 0), `after the root`},
		{"trailing string after the root", join(archive(rawFile("x")), str("")), `after the root`},
		{"two archives", join(archive(rawFile("x")), archive(rawFile("x"))), `after the root`},
		{"two root nodes", join(archive(rawFile("x")), rawFile("x")), `after the root`},

		{"unknown node type", archive(words("(", "type", "fifo", ")")), `unknown node type`},
		{"empty node type", archive(words("(", "type", "", ")")), `unknown node type`},
		{"node type in capitals", archive(words("(", "type", "Regular", "contents", "", ")")), `unknown node type`},
		{"node without type", archive(words("(", "regular", "contents", "", ")")), `expected "type"`},
		{"node without parenthesis", archive(words("type", "regular", "contents", "", ")")), `expected "("`},
		{"node closed twice", archive(words("(", "type", "regular", "contents", "", ")", ")")), `after the root`},
		{"symlink without target", archive(words("(", "type", "symlink", ")")), `expected "target"`},
		{"symlink with contents", archive(words("(", "type", "symlink", "contents", "x", ")")), `expected "target"`},
		{"directory with a stray token", archive(words("(", "type", "directory", "name", ")")), `expected "entry" or ")"`},
		{"entry without name", archive(rawDir(join(words("entry", "(", "node"), rawFile(""), str(")")))), `expected "name"`},
		{"entry without node", archive(rawDir(words("entry", "(", "name", "a", ")"))), `expected "node"`},
		{"entry without parenthesis", archive(rawDir(join(words("entry", "name", "a", "node"), rawFile("")))), `expected "("`},
		{"entry not closed", archive(rawDir(join(words("entry", "(", "name", "a", "node"), rawFile("")))), `ends early`},
		{"entry with two nodes", archive(rawDir(join(words("entry", "(", "name", "a", "node"), rawFile(""), rawFile(""), str(")")))), `expected ")"`},
		{"directory not closed", archive(words("(", "type", "directory")), `ends early`},

		{"keyword token of 1 MiB", archive(str(strings.Repeat("(", 1<<20))), `keyword has 1048576 bytes`},
		{"keyword token of 33 bytes", archive(str(strings.Repeat("(", 33))), `keyword has 33 bytes`},
		{"keyword with the largest length", archive(length(1<<64 - 1)), `keyword has 18446744073709551615 bytes`},
		{"file with the largest size", archive(join(words("(", "type", "regular", "contents"), length(1<<64-1), []byte("abc"))), `ends early`},
		{"file shorter than its size", archive(join(words("(", "type", "regular", "contents"), length(16), []byte("12345678"))), `ends early`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec recorder
			err := Parse(bytes.NewReader(tc.archive), &rec)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("Parse: %v, want an error wrapping ErrMalformed", err)
			}
			if !strings.Contains(err.Error(), tc.why) {
				t.Errorf("refused for another reason than %q: %v", tc.why, err)
			}
			if len(err.Error()) > 512 {
				t.Errorf("the error has %d bytes: %v", len(err.Error()), err)
			}
		})
	}
}

// TestParseRefusesEveryTruncation cuts an archive at each of its bytes.
func TestParseRefusesEveryTruncation(t *testing.T) {
	for _, tc := range roundTripArchives {
		whole := write(t, tc.events)
		for n := range len(whole) {
			var rec recorder
			if err := Parse(bytes.NewReader(whole[:n]), &rec); !errors.Is(err, ErrMalformed) {
				t.Fatalf("%s cut to %d of %d bytes: %v, want an error wrapping ErrMalformed", tc.name, n, len(whole), err)
			}
		}
	}
}

// TestParseDoesNotReadOversizedTokens gives a length that is over the
// limit of its place, with endless data behind it: the token must be
// refused by its length alone.
func TestParseDoesNotReadOversizedTokens(t *testing.T) {
	cases := []struct {
		name   string
		prefix []byte
		length uint64
	}{
		{"magic", nil, 1 << 20},
		{"keyword of 1 MiB", archive(nil), 1 << 20},
		{"keyword of 1 TiB", archive(nil), 1 << 40},
		{"keyword after a node began", archive(words("(", "type")), 1 << 20},
		{"keyword in a directory", archive(words("(", "type", "directory")), 1 << 20},
		{"keyword closing a file", archive(words("(", "type", "regular", "contents", "x")), 1 << 20},
		{"value of executable", archive(words("(", "type", "regular", "executable")), 1 << 20},
		{"name", archive(words("(", "type", "directory", "entry", "(", "name")), 1 << 20},
		{"name of 1 TiB", archive(words("(", "type", "directory", "entry", "(", "name")), 1 << 40},
		{"target", archive(words("(", "type", "symlink", "target")), 1 << 20},
		{"target of 1 TiB", archive(words("(", "type", "symlink", "target")), 1 << 40},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rest := &endless{}
			r := io.MultiReader(bytes.NewReader(join(tc.prefix, length(tc.length))), rest)
			var rec recorder
			if err := Parse(r, &rec); !errors.Is(err, ErrMalformed) {
				t.Fatalf("Parse: %v, want an error wrapping ErrMalformed", err)
			}
			if rest.read > 128<<10 {
				t.Fatalf("Parse read %d bytes of a token it had to refuse by its length", rest.read)
			}
		})
	}
}

func TestParseAcceptsTheLimits(t *testing.T) {
	name := strings.Repeat("n", 255)
	target := strings.Repeat("t", 4095)
	cases := []struct {
		name    string
		archive []byte
		last    event // the last event that is not a leave
	}{
		{"name of 255 bytes", archive(rawDir(rawEntry(name, rawFile("x")))), file(name, "x")},
		{"target of 4095 bytes", archive(rawLink(target)), link("", target)},
		{"target of 4095 bytes in a directory", archive(rawDir(rawEntry("l", rawLink(target)))), link("l", target)},
		{"nesting of 64 levels", nested(64, nil), dir("d")},
		{"a file at the 64th level", nested(63, rawFile("x")), file("leaf", "x")},
		{"a symlink at the 64th level", nested(63, rawLink("x")), link("leaf", "x")},
		{"keyword-like names", archive(rawDir(rawEntry("(", rawFile("")), rawEntry(")", rawFile("")), rawEntry("entry", rawFile("")))), file("entry", "")},
		{"name ...", archive(rawDir(rawEntry("...", rawFile("")))), file("...", "")},
		{"name with a backslash and a space", archive(rawDir(rawEntry(" \\", rawFile("")))), file(" \\", "")},
		{"target that is a slash", archive(rawLink("/")), link("", "/")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec recorder
			if err := Parse(bytes.NewReader(tc.archive), &rec); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			var last event
			for _, e := range rec.events {
				if e.op != "leave" {
					last = e
				}
			}
			if last != tc.last {
				t.Fatalf("last node %v, want %v", last, tc.last)
			}
			// What Parse accepts, Writer writes, to the byte.
			if again := write(t, rec.events); !bytes.Equal(again, tc.archive) {
				t.Fatal("written again, the archive differs")
			}
		})
	}
}

// failing is a recorder whose call number at returns err.
type failing struct {
	recorder
	at    int
	calls int
	err   error
}

func (f *failing) fail() bool {
	f.calls++
	return f.calls == f.at
}

func (f *failing) Directory(name string) error {
	if f.fail() {
		return f.err
	}
	return f.recorder.Directory(name)
}

func (f *failing) Leave() error {
	if f.fail() {
		return f.err
	}
	return f.recorder.Leave()
}

func (f *failing) Regular(name string, executable bool, size uint64, body io.Reader) error {
	if f.fail() {
		// The body is left unread: the handler's error still wins.
		return f.err
	}
	return f.recorder.Regular(name, executable, size, body)
}

func (f *failing) Symlink(name, target string) error {
	if f.fail() {
		return f.err
	}
	return f.recorder.Symlink(name, target)
}

func TestHandlerErrorEndsParse(t *testing.T) {
	events := roundTripArchives[0].events
	whole := write(t, events)
	// A plain value, so that only the very error can compare equal.
	stop := errors.New("stop")
	for at := 1; at <= len(events); at++ {
		h := &failing{at: at, err: stop}
		err := Parse(bytes.NewReader(whole), h)
		if err != stop {
			t.Fatalf("handler failed at call %d (%s): Parse returned %v, want the handler's error as it is", at, events[at-1].op, err)
		}
		if h.calls != at {
			t.Fatalf("handler failed at call %d: %d calls were made", at, h.calls)
		}
		if !slices.Equal(h.events, events[:at-1]) {
			t.Fatalf("handler failed at call %d: it had seen %v", at, h.events)
		}
	}
}

// partial reads only some of each file.
type partial struct {
	recorder
	read int64
}

func (p *partial) Regular(name string, executable bool, size uint64, body io.Reader) error {
	_, err := io.CopyN(io.Discard, body, p.read)
	return err
}

func TestParseRefusesAnUnreadBody(t *testing.T) {
	whole := archive(rawDir(rawEntry("a", rawFile("0123456789")), rawEntry("b", rawFile("x"))))
	for _, read := range []int64{0, 1, 9} {
		err := Parse(bytes.NewReader(whole), &partial{read: read})
		if err == nil {
			t.Fatalf("%d of 10 bytes read: Parse succeeded", read)
		}
		if errors.Is(err, ErrMalformed) {
			t.Fatalf("%d of 10 bytes read: %v blames the archive", read, err)
		}
	}
}

// keeper holds on to the body it is given.
type keeper struct {
	recorder
	kept io.Reader
}

func (k *keeper) Regular(name string, executable bool, size uint64, body io.Reader) error {
	if k.kept == nil {
		k.kept = body
	}
	return k.recorder.Regular(name, executable, size, body)
}

func (k *keeper) Leave() error {
	// The first file's body, read after its end, must not eat the archive.
	var b [8]byte
	if n, err := k.kept.Read(b[:]); n != 0 || err == nil {
		return fmt.Errorf("a body read after its file: %d bytes, error %v", n, err)
	}
	return k.recorder.Leave()
}

func TestBodyEndsWithItsFile(t *testing.T) {
	events := []event{dir(""), file("a", "0123456789"), file("b", "x"), leave()}
	k := &keeper{}
	if err := Parse(bytes.NewReader(write(t, events)), k); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !reflect.DeepEqual(k.events, events) {
		t.Fatalf("parsed %v, want %v", k.events, events)
	}
}

// endWithData gives as much as a read asks for, and its end together
// with its last byte.
type endWithData struct {
	data []byte
}

func (e *endWithData) Read(p []byte) (int, error) {
	n := copy(p, e.data)
	e.data = e.data[n:]
	if len(e.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func TestBodyReads(t *testing.T) {
	whole := archive(rawFile("0123456789"))

	// A read of no bytes neither ends the body nor fails.
	h := handlerFuncs{regular: func(name string, executable bool, size uint64, body io.Reader) error {
		if n, err := body.Read(nil); n != 0 || err != nil {
			return fmt.Errorf("reading no bytes: %d, %v", n, err)
		}
		b, err := io.ReadAll(body)
		if err != nil || string(b) != "0123456789" {
			return fmt.Errorf("the body is %q, %v", b, err)
		}
		if n, err := body.Read(make([]byte, 8)); n != 0 || err != io.EOF {
			return fmt.Errorf("reading past the end: %d, %v", n, err)
		}
		return nil
	}}
	if err := Parse(bytes.NewReader(whole), h); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// The same when the reader gives its last bytes with its end.
	if err := Parse(iotest.DataErrReader(bytes.NewReader(whole)), h); err != nil {
		t.Fatalf("Parse of data given with the end: %v", err)
	}
	big := archive(rawFile(strings.Repeat("0123456789", 10000)))
	h = handlerFuncs{regular: func(name string, executable bool, size uint64, body io.Reader) error {
		// Reads larger than Parse's buffer go to the reader itself.
		b := make([]byte, 1<<20)
		n, err := io.ReadFull(body, b)
		if uint64(n) != size || err != io.ErrUnexpectedEOF {
			return fmt.Errorf("read %d of %d bytes, %v", n, size, err)
		}
		return nil
	}}
	if err := Parse(&endWithData{big}, h); err != nil {
		t.Fatalf("Parse of a large file given with the end: %v", err)
	}
	// An archive that ends with the last byte of a body: the body is
	// whole, the archive is not.
	if err := Parse(&endWithData{big[:len(big)-16]}, h); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Parse of an archive ending with a body: %v, want an error wrapping ErrMalformed", err)
	}

	// A body cut short fails the same way however often it is read.
	cut := whole[:len(whole)-16-6-3] // without the ")", the padding and 3 bytes
	h = handlerFuncs{regular: func(name string, executable bool, size uint64, body io.Reader) error {
		b, first := io.ReadAll(body)
		if string(b) != "0123456" || !errors.Is(first, ErrMalformed) {
			return fmt.Errorf("the cut body is %q, %v", b, first)
		}
		if n, err := body.Read(make([]byte, 8)); n != 0 || err != first {
			return fmt.Errorf("reading after the failure: %d, %v", n, err)
		}
		return nil
	}}
	if err := Parse(bytes.NewReader(cut), h); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Parse of a cut body: %v, want an error wrapping ErrMalformed", err)
	}
}

// stalling gives nothing and no error, for ever.
type stalling struct{}

func (stalling) Read([]byte) (int, error) { return 0, nil }

func TestParseGivesUpOnAReaderThatStalls(t *testing.T) {
	whole := archive(rawFile(strings.Repeat("x", 100)))
	for _, n := range []int{0, 30, len(whole) - 30, len(whole)} {
		done := make(chan error, 1)
		go func() {
			done <- Parse(io.MultiReader(bytes.NewReader(whole[:n]), stalling{}), &recorder{})
		}()
		if err := <-done; !errors.Is(err, io.ErrNoProgress) {
			t.Fatalf("reader stalling after %d bytes: %v, want io.ErrNoProgress", n, err)
		}
	}
}

// swallower ignores what reading a body returns.
type swallower struct{ recorder }

func (s *swallower) Regular(name string, executable bool, size uint64, body io.Reader) error {
	io.Copy(io.Discard, body)
	return nil
}

func TestTruncatedBodyIsMalformedWhateverTheHandlerDoes(t *testing.T) {
	whole := archive(rawFile("0123456789"))
	cut := whole[:len(whole)-16-6-3] // without the ")", the padding and 3 bytes
	if err := Parse(bytes.NewReader(cut), &swallower{}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Parse: %v, want an error wrapping ErrMalformed", err)
	}
}

func TestReadErrorIsNotMalformed(t *testing.T) {
	broken := errors.New("the line went dead")
	whole := write(t, roundTripArchives[0].events)
	for n := range len(whole) {
		for _, h := range []Handler{&recorder{}, &swallower{}} {
			r := io.MultiReader(bytes.NewReader(whole[:n]), iotest.ErrReader(broken))
			err := Parse(r, h)
			if !errors.Is(err, broken) {
				t.Fatalf("reader failing after %d bytes: Parse returned %v, want the reader's error", n, err)
			}
			if errors.Is(err, ErrMalformed) {
				t.Fatalf("reader failing after %d bytes: %v blames the archive", n, err)
			}
		}
	}
	// A reader that fails where the archive ends is not a trailing byte,
	// and not a complete archive either.
	r := io.MultiReader(bytes.NewReader(whole), iotest.ErrReader(broken))
	if err := Parse(r, &recorder{}); !errors.Is(err, broken) || errors.Is(err, ErrMalformed) {
		t.Fatalf("reader failing at the end: Parse returned %v, want the reader's error", err)
	}
	// An unexpected end reported by the reader itself, as a decompressor
	// reports a cut stream, is the reader's error too.
	r = io.MultiReader(bytes.NewReader(whole[:40]), iotest.ErrReader(io.ErrUnexpectedEOF))
	if err := Parse(r, &recorder{}); err != io.ErrUnexpectedEOF {
		t.Fatalf("reader ending unexpectedly: Parse returned %v, want the reader's error as it is", err)
	}
}

// heapAllocated returns the bytes f allocates.
func heapAllocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestParseIsStreaming(t *testing.T) {
	const size = 256 << 20
	r := io.MultiReader(
		bytes.NewReader(archive(join(words("(", "type", "regular", "contents"), length(size)))),
		io.LimitReader(&endless{}, size),
		bytes.NewReader(str(")")),
	)
	var read int64
	h := handlerFuncs{regular: func(name string, executable bool, n uint64, body io.Reader) error {
		var err error
		read, err = io.Copy(io.Discard, body)
		return err
	}}
	var err error
	allocated := heapAllocated(func() { err = Parse(r, h) })
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if read != size {
		t.Fatalf("the handler read %d bytes, want %d", read, size)
	}
	if allocated > 1<<20 {
		t.Fatalf("parsing a file of %d bytes allocated %d bytes", size, allocated)
	}
}

// handlerFuncs is a Handler of which a test sets what it needs.
type handlerFuncs struct {
	regular func(name string, executable bool, size uint64, body io.Reader) error
}

func (h handlerFuncs) Directory(string) error { return nil }
func (h handlerFuncs) Leave() error           { return nil }
func (h handlerFuncs) Regular(name string, executable bool, size uint64, body io.Reader) error {
	return h.regular(name, executable, size, body)
}
func (h handlerFuncs) Symlink(string, string) error { return nil }

func TestWriterIsStreaming(t *testing.T) {
	const size = 256 << 20
	var err error
	allocated := heapAllocated(func() {
		w := NewWriter(io.Discard)
		var body io.Writer
		if body, err = w.Regular("", false, size); err != nil {
			return
		}
		if _, err = io.CopyN(body, &endless{}, size); err != nil {
			return
		}
		err = w.Close()
	})
	if err != nil {
		t.Fatalf("writing: %v", err)
	}
	if allocated > 1<<20 {
		t.Fatalf("writing a file of %d bytes allocated %d bytes", size, allocated)
	}
}

func TestWriterRefusesShortFile(t *testing.T) {
	next := []struct {
		name string
		call func(w *Writer) error
	}{
		{"Close", func(w *Writer) error { return w.Close() }},
		{"Leave", func(w *Writer) error { return w.Leave() }},
		{"Directory", func(w *Writer) error { return w.Directory("z") }},
		{"Symlink", func(w *Writer) error { return w.Symlink("z", "t") }},
		{"Regular", func(w *Writer) error { _, err := w.Regular("z", false, 0); return err }},
	}
	for _, written := range []string{"", "abc", "abcd"} {
		for _, n := range next {
			t.Run(fmt.Sprintf("%d of 5 bytes, then %s", len(written), n.name), func(t *testing.T) {
				var buf bytes.Buffer
				w := NewWriter(&buf)
				if err := w.Directory(""); err != nil {
					t.Fatal(err)
				}
				body, err := w.Regular("a", false, 5)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(body, written); err != nil {
					t.Fatal(err)
				}
				err = n.call(w)
				if !errors.Is(err, ErrMisuse) {
					t.Fatalf("%s after a short file: %v, want an error wrapping ErrMisuse", n.name, err)
				}
				// The writer stays failed.
				if err := w.Close(); !errors.Is(err, ErrMisuse) {
					t.Fatalf("Close after the failure: %v", err)
				}
			})
		}
	}

	t.Run("file root", func(t *testing.T) {
		w := NewWriter(io.Discard)
		body, err := w.Regular("", false, 5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(body, "abc"); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); !errors.Is(err, ErrMisuse) {
			t.Fatalf("Close after a short file: %v, want an error wrapping ErrMisuse", err)
		}
	})
}

func TestWriterRefusesLongFile(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	body, err := w.Regular("", false, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(body, "abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(body, "def"); !errors.Is(err, ErrMisuse) {
		t.Fatalf("writing 6 bytes to a file of 5: %v, want an error wrapping ErrMisuse", err)
	}
	if _, err := io.WriteString(body, "de"); !errors.Is(err, ErrMisuse) {
		t.Fatalf("writing after the failure: %v, want the failure", err)
	}
	if got := buf.Bytes(); !bytes.HasSuffix(got, []byte("abc")) {
		t.Fatalf("the refused bytes were written: %q", got)
	}
	if err := w.Close(); !errors.Is(err, ErrMisuse) {
		t.Fatalf("Close after the failure: %v", err)
	}
}

func TestWriterRefusesAStaleBody(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Directory(""); err != nil {
		t.Fatal(err)
	}
	first, err := w.Regular("a", false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Regular("b", false, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(first, "late"); !errors.Is(err, ErrMisuse) {
		t.Fatalf("writing to the body of an earlier file: %v, want an error wrapping ErrMisuse", err)
	}
}

func TestWriterRefusesWhatParseRefuses(t *testing.T) {
	long := func(n int) string { return strings.Repeat("n", n) }
	var deep []event
	for i := range 65 {
		name := "d"
		if i == 0 {
			name = ""
		}
		deep = append(deep, dir(name))
	}
	deepFile := append(append([]event{}, deep[:64]...), file("f", ""))

	cases := []struct {
		name   string
		events []event // the last one must fail
	}{
		{"unsorted names", []event{dir(""), file("b", ""), file("a", "")}},
		{"duplicate name", []event{dir(""), file("a", ""), link("a", "t")}},
		{"unsorted after a subdirectory", []event{dir(""), dir("b"), file("z", ""), leave(), file("a", "")}},
		{"empty name", []event{dir(""), file("", "")}},
		{"name .", []event{dir(""), dir(".")}},
		{"name ..", []event{dir(""), link("..", "t")}},
		{"name with a slash", []event{dir(""), file("a/b", "")}},
		{"name with NUL", []event{dir(""), file("a\x00", "")}},
		{"name of 256 bytes", []event{dir(""), file(long(256), "")}},
		{"empty target", []event{link("", "")}},
		{"target with NUL", []event{dir(""), link("l", "a\x00b")}},
		{"target of 4096 bytes", []event{link("", long(4096))}},
		{"nesting of 65 levels", deep},
		{"a file at the 65th level", deepFile},
		{"named file root", []event{file("nar", "")}},
		{"named directory root", []event{dir("nar")}},
		{"named symlink root", []event{link("nar", "t")}},
		{"second root", []event{file("", ""), file("", "")}},
		{"second root after a directory", []event{dir(""), leave(), link("", "t")}},
		{"Leave without a directory", []event{leave()}},
		{"Leave after the root", []event{dir(""), leave(), leave()}},
		{"Leave after a file root", []event{file("", ""), leave()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			last := len(tc.events) - 1
			if err := replay(w, tc.events[:last]); err != nil {
				t.Fatalf("the calls before the bad one: %v", err)
			}
			if err := replay(w, tc.events[last:]); !errors.Is(err, ErrMisuse) {
				t.Fatalf("the bad call: %v, want an error wrapping ErrMisuse", err)
			}
			// After a failure every call returns it.
			failure := w.Close()
			if !errors.Is(failure, ErrMisuse) {
				t.Fatalf("Close after the failure: %v, want the failure", failure)
			}
			if err := w.Directory("zz"); err != failure {
				t.Errorf("Directory after the failure: %v", err)
			}
			if err := w.Leave(); err != failure {
				t.Errorf("Leave after the failure: %v", err)
			}
			if err := w.Symlink("zz", "t"); err != failure {
				t.Errorf("Symlink after the failure: %v", err)
			}
			if _, err := w.Regular("zz", false, 0); err != failure {
				t.Errorf("Regular after the failure: %v", err)
			}
			if err := w.Close(); err != failure {
				t.Errorf("second Close after the failure: %v", err)
			}
		})
	}
}

func TestWriterAcceptsTheLimits(t *testing.T) {
	var deep []event
	for i := range 63 {
		name := "d"
		if i == 0 {
			name = ""
		}
		deep = append(deep, dir(name))
	}
	deep = append(deep, file(strings.Repeat("n", 255), "x"), link("z", strings.Repeat("t", 4095)))
	for range 63 {
		deep = append(deep, leave())
	}
	written := write(t, deep)
	var rec recorder
	if err := Parse(bytes.NewReader(written), &rec); err != nil {
		t.Fatalf("Parse of what Writer wrote: %v", err)
	}
	if !reflect.DeepEqual(rec.events, deep) {
		t.Fatal("parsed events differ from the written ones")
	}
}

func TestCloseNeedsOneWholeRoot(t *testing.T) {
	cases := []struct {
		name   string
		events []event
	}{
		{"nothing written", nil},
		{"open root directory", []event{dir("")}},
		{"open subdirectory", []event{dir(""), dir("a"), leave()}},
		{"two open directories", []event{dir(""), dir("a")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWriter(io.Discard)
			if err := replay(w, tc.events); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); !errors.Is(err, ErrMisuse) {
				t.Fatalf("Close: %v, want an error wrapping ErrMisuse", err)
			}
		})
	}

	t.Run("calls after Close", func(t *testing.T) {
		w := NewWriter(io.Discard)
		if err := replay(w, []event{dir(""), leave()}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if err := w.Directory(""); !errors.Is(err, ErrMisuse) {
			t.Fatalf("Directory after Close: %v", err)
		}
		if err := w.Leave(); !errors.Is(err, ErrMisuse) {
			t.Fatalf("Leave after Close: %v", err)
		}
		if err := w.Symlink("", "t"); !errors.Is(err, ErrMisuse) {
			t.Fatalf("Symlink after Close: %v", err)
		}
		if _, err := w.Regular("", false, 0); !errors.Is(err, ErrMisuse) {
			t.Fatalf("Regular after Close: %v", err)
		}
	})
}

// failAfter accepts n bytes and then fails.
type failAfter struct {
	n   int
	err error
}

func (f *failAfter) Write(p []byte) (int, error) {
	if len(p) > f.n {
		n := f.n
		f.n = 0
		return n, f.err
	}
	f.n -= len(p)
	return len(p), nil
}

func TestWriteErrorIsReturnedAsItIs(t *testing.T) {
	full := errors.New("disk full")
	for _, tc := range roundTripArchives {
		whole := write(t, tc.events)
		for n := range len(whole) {
			w := NewWriter(&failAfter{n: n, err: full})
			err := replay(w, tc.events)
			if err == nil {
				err = w.Close()
			}
			if !errors.Is(err, full) {
				t.Fatalf("%s, destination failing after %d bytes: %v, want its error", tc.name, n, err)
			}
			if errors.Is(err, ErrMisuse) {
				t.Fatalf("%s, destination failing after %d bytes: %v blames the caller", tc.name, n, err)
			}
			if err := w.Close(); err != full {
				t.Fatalf("%s: Close after the failure: %v, want the destination's error as it is", tc.name, err)
			}
			if err := w.Leave(); err != full {
				t.Fatalf("%s: Leave after the failure: %v, want the destination's error as it is", tc.name, err)
			}
		}
	}
}

// short takes n bytes and then one byte less than it is given, without an
// error, which a destination must not do.
type short struct {
	n int
}

func (s *short) Write(p []byte) (int, error) {
	if len(p) > s.n {
		s.n = 0
		return len(p) - 1, nil
	}
	s.n -= len(p)
	return len(p), nil
}

func TestShortWriteIsAnError(t *testing.T) {
	for _, tc := range roundTripArchives {
		whole := write(t, tc.events)
		for n := range len(whole) {
			w := NewWriter(&short{n: n})
			err := replay(w, tc.events)
			if err == nil {
				err = w.Close()
			}
			if !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("%s, destination short after %d bytes: %v, want io.ErrShortWrite", tc.name, n, err)
			}
		}
	}
}

// TestAgainstNix compares Writer's archive with what Nix dumps of the same
// tree on disk.
func TestAgainstNix(t *testing.T) {
	nixStore, err := exec.LookPath("nix-store")
	if err != nil {
		t.Skip("nix-store is not installed")
	}
	events := []event{
		dir(""),
		file("B", "upper"),
		file("a", "lower"),
		file("empty", ""),
		file("file", "plain\n"),
		link("link", "file"),
		dir("nothing"),
		leave(),
		script("run", "#!/bin/sh\n"),
		dir("sub"),
		file("eight", "12345678"),
		file("inner", "x"),
		leave(),
		leave(),
	}
	root := filepath.Join(t.TempDir(), "root")
	path := []string{root}
	for _, e := range events {
		at := filepath.Join(append(path, e.name)...)
		var err error
		switch e.op {
		case "dir":
			err = os.Mkdir(at, 0o755)
			path = append(path, e.name)
		case "leave":
			path = path[:len(path)-1]
		case "file":
			mode := os.FileMode(0o644)
			if e.exec {
				mode = 0o755
			}
			err = os.WriteFile(at, []byte(e.body), mode)
		case "link":
			err = os.Symlink(e.target, at)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(nixStore, "--dump", root)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	dumped, err := cmd.Output()
	if err != nil {
		t.Skipf("nix-store --dump does not run here: %v: %s", err, stderr.String())
	}
	if written := write(t, events); !bytes.Equal(written, dumped) {
		t.Fatalf("Writer's archive (%d bytes) is not the one Nix dumps (%d bytes)", len(written), len(dumped))
	}
	var rec recorder
	if err := Parse(bytes.NewReader(dumped), &rec); err != nil {
		t.Fatalf("Parse of Nix's dump: %v", err)
	}
	if !reflect.DeepEqual(rec.events, events) {
		t.Fatalf("Nix's dump parsed to\n %v\nwant\n %v", rec.events, events)
	}
}
