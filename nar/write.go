package nar

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Writer writes a canonical archive. Calls mirror Handler; the caller
// gives entries in byte order of name and exactly size bytes per file.
//
// The root node is given the empty name. A call that would make an
// archive Parse refuses fails with an error wrapping ErrMisuse; an error
// of the destination is returned as it is. After a failure every call
// returns that failure.
//
// A Writer keeps no buffer of its own: each call writes its strings
// before it returns, except the few bytes that end a file, which go out
// with the next call.
type Writer struct {
	w   io.Writer
	buf []byte // the strings of the call under way
	err error  // the first failure

	// last has one name per open directory, that of its latest entry, or
	// the empty string when it has none yet.
	last []string
	// file is the file whose body is being written, if one is.
	file *fileBody
	// rooted is set once the root node has begun.
	rooted bool
	closed bool
}

// NewWriter returns a Writer of one archive to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Directory begins a directory. Its entries follow, then Leave.
func (w *Writer) Directory(name string) error {
	if err := w.begin(name); err != nil {
		return err
	}
	w.put("(", "type", "directory")
	w.last = append(w.last, "")
	return w.flush()
}

// Leave ends the directory begun last and not yet left.
func (w *Writer) Leave() error {
	if err := w.ready(); err != nil {
		return err
	}
	if len(w.last) == 0 {
		return w.misuse("Leave was called with no directory open")
	}
	w.last = w.last[:len(w.last)-1]
	w.endNode()
	return w.flush()
}

// Regular begins a file and returns the writer of its body, which takes
// exactly size bytes and is of no use after the next call on w. The next
// call checks that size bytes were written.
func (w *Writer) Regular(name string, executable bool, size uint64) (io.Writer, error) {
	if err := w.begin(name); err != nil {
		return nil, err
	}
	w.put("(", "type", "regular")
	if executable {
		w.put("executable", "")
	}
	w.put("contents")
	w.buf = binary.LittleEndian.AppendUint64(w.buf, size)
	if err := w.flush(); err != nil {
		return nil, err
	}
	w.file = &fileBody{w: w, size: size}
	return w.file, nil
}

// Symlink writes a symlink.
func (w *Writer) Symlink(name, target string) error {
	if err := w.begin(name); err != nil {
		return err
	}
	if bad := checkTarget(target); bad != "" {
		return w.misuse("%s", bad)
	}
	w.put("(", "type", "symlink", "target", target)
	w.endNode()
	return w.flush()
}

// Close ends the archive. It fails unless exactly one root node was
// written and closed. It does not close the destination.
func (w *Writer) Close() error {
	if w.closed || w.err != nil {
		return w.err
	}
	if err := w.ready(); err != nil {
		return err
	}
	switch {
	case !w.rooted:
		return w.misuse("the archive was closed without a root node")
	case len(w.last) > 0:
		return w.misuse("the archive was closed with %d directories open", len(w.last))
	}
	if err := w.flush(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

// misuse fails the writer with an error that wraps ErrMisuse.
func (w *Writer) misuse(format string, args ...any) error {
	w.err = misused(format, args...)
	return w.err
}

// misused returns an error that wraps ErrMisuse.
func misused(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMisuse, fmt.Sprintf(format, args...))
}

// put queues strings of the format.
func (w *Writer) put(strs ...string) {
	for _, s := range strs {
		w.buf = appendString(w.buf, s)
	}
}

// flush writes what is queued.
func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	n, err := w.w.Write(w.buf)
	if err == nil && n < len(w.buf) {
		err = io.ErrShortWrite
	}
	w.buf = w.buf[:0]
	w.err = err
	return err
}

// ready is the start of every call: it returns the failure there was, or
// that of a file left short, and queues the end of a file written whole.
func (w *Writer) ready() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return misused("the writer is closed")
	}
	if f := w.file; f != nil {
		w.file = nil
		if f.written != f.size {
			return w.misuse("a file of %d bytes was given %d", f.size, f.written)
		}
		w.buf = appendPadding(w.buf, f.size)
		w.endNode()
	}
	return nil
}

// begin starts a node called name where the archive stands: as the root,
// or as the next entry of the open directory.
func (w *Writer) begin(name string) error {
	if err := w.ready(); err != nil {
		return err
	}
	depth := len(w.last)
	if depth == 0 {
		switch {
		case w.rooted:
			return w.misuse("an archive has one root node, a second was begun")
		case name != "":
			return w.misuse("the root node was named %s, its name is the empty one", show(name))
		}
		w.rooted = true
		w.put(magic)
		return nil
	}
	if depth >= MaxDepth {
		return w.misuse("%s", tooDeep())
	}
	if bad := checkName(name); bad != "" {
		return w.misuse("%s", bad)
	}
	if bad := checkOrder(w.last[depth-1], name); bad != "" {
		return w.misuse("%s", bad)
	}
	w.last[depth-1] = name
	w.put("entry", "(", "name", name, "node")
	return nil
}

// endNode queues the end of a node, and of the entry it is if it is one.
func (w *Writer) endNode() {
	w.put(")")
	if len(w.last) > 0 {
		w.put(")")
	}
}

// fileBody takes the contents of one file.
type fileBody struct {
	w       *Writer
	size    uint64
	written uint64
}

func (f *fileBody) Write(p []byte) (int, error) {
	w := f.w
	switch {
	case w.file != f:
		// Refused without failing the writer: the archive is not harmed.
		return 0, misused("a file's body was written to after the call that followed the file")
	case w.err != nil:
		return 0, w.err
	case uint64(len(p)) > f.size-f.written:
		return 0, w.misuse("a file of %d bytes was given %d", f.size, f.written+uint64(len(p)))
	}
	n, err := w.w.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	f.written += uint64(n)
	w.err = err
	return n, err
}
