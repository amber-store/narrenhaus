// Package decompress undoes the compression of an uploaded NAR. Nix names
// the compression in the URL's extension, and for gzip not even there, so
// the body is taken for what its first bytes say it is.
package decompress

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// ErrUnsupported is returned by Reader for a body that begins like none of
// the forms it knows, an empty one included.
var ErrUnsupported = errors.New("decompress: unsupported compression")

// The first bytes of the forms Reader knows.
var (
	magicXz    = []byte{0xfd, '7', 'z', 'X', 'Z', 0x00}
	magicZstd  = []byte{0x28, 0xb5, 0x2f, 0xfd}
	magicGzip  = []byte{0x1f, 0x8b}
	magicBzip2 = []byte{'B', 'Z', 'h'}
	// An uncompressed NAR begins with the string "nix-archive-1": its
	// length as eight bytes, lowest first, and its text.
	magicNAR = []byte("\x0d\x00\x00\x00\x00\x00\x00\x00nix-archive-1")
)

const (
	// bufferSize is how much of the body is read at a time. The
	// decompressors ask for a few bytes at a time.
	bufferSize = 64 << 10

	// maxZstdWindow is the largest window a zstd frame may ask for: what
	// the zstd program itself reads without being told to read more. The
	// window is memory the frame makes its reader hold.
	maxZstdWindow = 128 << 20
)

// Reader recognises r's compression by its first bytes (xz, zstd, gzip,
// bzip2, or an uncompressed NAR's header) and returns the decompressed
// stream. zstd input may be several frames. compressed says whether r
// was compressed at all.
//
// A body that begins like none of them is ErrUnsupported; an error of r
// met while looking at its first bytes is returned as it is. A compressed
// body that is cut short, is not what its first bytes promise, or is
// followed by bytes that are no further stream fails when out is read,
// with the decompressor's error. Streams of one kind that follow one
// another are read as one, as the programs of those formats read them.
//
// out reads from r through a buffer and may have read more of r than it
// has given out. It holds nothing that has to be closed.
func Reader(r io.Reader) (out io.Reader, compressed bool, err error) {
	br := bufio.NewReaderSize(r, bufferSize)
	// Peek returns what there is when the body is shorter, with the
	// reason; whether that matters depends on what the bytes are.
	head, peekErr := br.Peek(len(magicNAR))
	switch {
	case bytes.HasPrefix(head, magicXz):
		x, err := xz.NewReader(br)
		if err != nil {
			return nil, false, fmt.Errorf("decompress: xz: %w", err)
		}
		return x, true, nil
	case bytes.HasPrefix(head, magicZstd):
		// One decoder at a time decodes on the caller's goroutine: the
		// decoder starts none of its own, so a reader that is dropped
		// half read leaves nothing running.
		z, err := zstd.NewReader(br, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxZstdWindow))
		if err != nil {
			return nil, false, fmt.Errorf("decompress: zstd: %w", err)
		}
		return &zstdReader{d: z}, true, nil
	case bytes.HasPrefix(head, magicGzip):
		g, err := gzip.NewReader(br)
		if err != nil {
			return nil, false, fmt.Errorf("decompress: gzip: %w", err)
		}
		return g, true, nil
	case bytes.HasPrefix(head, magicBzip2):
		return bzip2.NewReader(br), true, nil
	case bytes.Equal(head, magicNAR):
		return br, false, nil
	case peekErr != nil && peekErr != io.EOF:
		return nil, false, peekErr
	}
	return nil, false, ErrUnsupported
}

// zstdReader reads a zstd stream and gives the decoder's buffers back when
// the stream has ended, one way or the other.
type zstdReader struct {
	d   *zstd.Decoder
	err error
}

func (z *zstdReader) Read(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}
	n, err := z.d.Read(p)
	if err != nil {
		z.err = err
		z.d.Close()
	}
	return n, err
}
