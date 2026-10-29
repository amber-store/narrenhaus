package nar

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// readAhead is the size of Parse's read buffer. A file body read in
// larger pieces goes past it.
const readAhead = 32 << 10

// maxStalls is the number of reads in a row that may give no bytes and
// no error before Parse gives up with io.ErrNoProgress, as bufio does.
const maxStalls = 100

// Parse reads one archive from r and nothing after it: bytes after the
// root node are an error.
//
// Every error that is the archive's fault wraps ErrMalformed, an archive
// that ends early included. An error of r other than its end, and an
// error of h, is returned as it is. Parse holds one string of the archive
// in memory at a time, never a file's contents, and refuses a string that
// is longer than its place allows without reading it.
func Parse(r io.Reader, h Handler) error {
	p := &parser{r: bufio.NewReaderSize(r, readAhead), h: h}
	tok, err := p.token(len(magic), "the first string")
	switch {
	case errors.Is(err, ErrMalformed) || err == nil && string(tok) != magic:
		return malformed("the input does not begin as a Nix archive")
	case err != nil:
		return err
	}
	if err := p.node("", 0); err != nil {
		return err
	}
	switch _, err := p.r.ReadByte(); err {
	case io.EOF:
		return nil
	case nil:
		return malformed("there are bytes after the root node")
	default:
		return err
	}
}

// parser is the state of one Parse.
type parser struct {
	r *bufio.Reader
	h Handler
	// buf holds the string last read, with its padding. The longest
	// string kept is a symlink target.
	buf [MaxTargetLen + 1]byte
}

// malformed returns an error that wraps ErrMalformed.
func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}

// errTruncated is the archive ending before its grammar does.
func errTruncated() error {
	return malformed("the archive ends early")
}

// fill reads len(b) bytes. The end of the input is the archive's fault;
// any other error is the reader's and is returned as it is.
func (p *parser) fill(b []byte) error {
	for stalls := 0; len(b) > 0; {
		n, err := p.r.Read(b)
		b = b[n:]
		switch {
		case err != nil && err != io.EOF:
			return err
		case len(b) == 0:
			return nil
		case err == io.EOF:
			return errTruncated()
		case n > 0:
			stalls = 0
		default:
			if stalls++; stalls == maxStalls {
				return io.ErrNoProgress
			}
		}
	}
	return nil
}

// length reads the 64-bit number that begins a string.
func (p *parser) length() (uint64, error) {
	if err := p.fill(p.buf[:8]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(p.buf[:8]), nil
}

// zeroPadding checks the padding of a string.
func zeroPadding(pad []byte) error {
	for _, c := range pad {
		if c != 0 {
			return malformed("the padding of a string is not zero")
		}
	}
	return nil
}

// token reads a string of at most most bytes; what names it in the error
// that refuses a longer one, which is made before any of the string is
// read. The slice returned is valid until the next read.
func (p *parser) token(most int, what string) ([]byte, error) {
	n, err := p.length()
	if err != nil {
		return nil, err
	}
	if n > uint64(most) {
		return nil, malformed("%s has %d bytes, the limit is %d", what, n, most)
	}
	padded := int(n) + padding(n)
	if err := p.fill(p.buf[:padded]); err != nil {
		return nil, err
	}
	if err := zeroPadding(p.buf[n:padded]); err != nil {
		return nil, err
	}
	return p.buf[:n], nil
}

// keyword reads a string where the grammar has a keyword.
func (p *parser) keyword() ([]byte, error) {
	return p.token(maxKeyword, "a keyword")
}

// expect reads the keyword want.
func (p *parser) expect(want string) error {
	tok, err := p.keyword()
	if err != nil {
		return err
	}
	if string(tok) != want {
		return malformed("expected %q, got %q", want, tok)
	}
	return nil
}

// node parses a node from its opening parenthesis to its closing one.
// depth is the number of directories the node is in.
func (p *parser) node(name string, depth int) error {
	if depth >= MaxDepth {
		return malformed("%s", tooDeep())
	}
	if err := p.expect("("); err != nil {
		return err
	}
	if err := p.expect("type"); err != nil {
		return err
	}
	kind, err := p.keyword()
	if err != nil {
		return err
	}
	switch string(kind) {
	case "regular":
		return p.regular(name)
	case "symlink":
		return p.symlink(name)
	case "directory":
		return p.directory(name, depth)
	}
	return malformed("unknown node type %q", kind)
}

func (p *parser) regular(name string) error {
	tag, err := p.keyword()
	if err != nil {
		return err
	}
	executable := false
	if string(tag) == "executable" {
		// The marker's value is the empty string: a length of zero.
		n, err := p.length()
		if err != nil {
			return err
		}
		if n != 0 {
			return malformed("the executable marker has a value of %d bytes, it must be empty", n)
		}
		executable = true
		if tag, err = p.keyword(); err != nil {
			return err
		}
	}
	if string(tag) != "contents" {
		return malformed("expected %q, got %q", "contents", tag)
	}
	size, err := p.length()
	if err != nil {
		return err
	}

	body := &body{r: p.r, left: size}
	if err := p.h.Regular(name, executable, size, body); err != nil {
		return err
	}
	// A failed read ends the parse even if the handler made nothing of it.
	if body.err != nil {
		return body.err
	}
	if body.left > 0 {
		return fmt.Errorf("nar: the handler left %d of the %d bytes of %s unread", body.left, size, show(name))
	}
	pad := p.buf[:padding(size)]
	if err := p.fill(pad); err != nil {
		return err
	}
	if err := zeroPadding(pad); err != nil {
		return err
	}
	return p.expect(")")
}

func (p *parser) symlink(name string) error {
	if err := p.expect("target"); err != nil {
		return err
	}
	tok, err := p.token(MaxTargetLen, "a symlink target")
	if err != nil {
		return err
	}
	target := string(tok)
	if bad := checkTarget(target); bad != "" {
		return malformed("%s", bad)
	}
	if err := p.expect(")"); err != nil {
		return err
	}
	return p.h.Symlink(name, target)
}

func (p *parser) directory(name string, depth int) error {
	if err := p.h.Directory(name); err != nil {
		return err
	}
	prev := ""
	for {
		tag, err := p.keyword()
		if err != nil {
			return err
		}
		if string(tag) == ")" {
			return p.h.Leave()
		}
		if string(tag) != "entry" {
			return malformed("expected %q or %q, got %q", "entry", ")", tag)
		}
		if err := p.expect("("); err != nil {
			return err
		}
		if err := p.expect("name"); err != nil {
			return err
		}
		tok, err := p.token(MaxNameLen, "an entry name")
		if err != nil {
			return err
		}
		entry := string(tok)
		if bad := checkName(entry); bad != "" {
			return malformed("%s", bad)
		}
		if bad := checkOrder(prev, entry); bad != "" {
			return malformed("%s", bad)
		}
		prev = entry
		if err := p.expect("node"); err != nil {
			return err
		}
		if err := p.node(entry, depth+1); err != nil {
			return err
		}
		if err := p.expect(")"); err != nil {
			return err
		}
	}
}

// body is the contents of one file, read from the archive as the handler
// asks for them.
type body struct {
	r    *bufio.Reader
	left uint64
	// err is what ended the reading before the file's end, if anything
	// did. It is kept because a handler may drop it.
	err error
}

func (b *body) Read(p []byte) (int, error) {
	switch {
	case b.err != nil:
		return 0, b.err
	case b.left == 0:
		return 0, io.EOF
	case len(p) == 0:
		return 0, nil
	}
	if uint64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.r.Read(p)
	// A reader that gives nothing and no error would have the handler
	// read for ever.
	for stalls := 0; n == 0 && err == nil; n, err = b.r.Read(p) {
		if stalls++; stalls == maxStalls {
			err = io.ErrNoProgress
			break
		}
	}
	b.left -= uint64(n)
	switch {
	case err == nil:
		return n, nil
	case err != io.EOF:
		b.err = err
	case b.left == 0:
		// A reader may give its last bytes together with its end; the
		// end is then met again by whoever reads next.
		return n, nil
	default:
		b.err = errTruncated()
	}
	return n, b.err
}
