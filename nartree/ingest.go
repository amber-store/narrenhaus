// Package nartree turns a Nix archive into core objects and back.
//
// A content reference is a directory of two entries: nar, the archive's
// root node, and recipe, a short text that says how the archive is made of
// the node and what must come out. Ingest and Root build one from an
// archive; Open and WriteNAR give the archive back.
//
// Every key depends on how the objects are built: file bytes are cut by
// chunkers.SplitBytes with its default sizes, file indexes and directories
// by the item chunker of ingest.DefaultItemBits, and each entry carries the
// fixed metadata below, since an archive has none. All of it is frozen:
// a change makes other keys of the same archive, and so ends the sharing
// with every reference already stored.
package nartree

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/amber-store/core/chunkers"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"

	"github.com/amber-store/narrenhaus/nar"
)

// The metadata of an entry. Modes are POSIX st_mode values as core keeps
// them, written out rather than taken from a Go fs.FileMode, whose type
// bits differ.
const (
	modeType       = 0o170000 // the bits of a mode that say the kind
	modeRegular    = 0o100444
	modeExecutable = 0o100555
	modeDirectory  = 0o040555
	modeSymlink    = 0o120777

	typeRegular   = 0o100000
	typeDirectory = 0o040000
	typeSymlink   = 0o120000

	// anyExecute is the bit that makes a file executable when read.
	anyExecute = 0o100

	// mtime is one second after the epoch, in nanoseconds.
	mtime = 1_000_000_000
)

const (
	// nodeName is the name of the archive's root node in the root
	// directory of a content reference.
	nodeName = "nar"
	// recipeName is the name of the recipe beside it.
	recipeName = "recipe"
)

// MaxNARSize is the longest archive narrenhaus takes, in bytes: 64 GiB.
// It bounds what the check of a reference can cost, which is the hashing
// of its archive: it is the limit an upload gives to Ingest, and a
// reference whose recipe states more is not rebuilt.
const MaxNARSize = 64 << 30

// ErrTooLarge is an archive of more bytes than the limit given to Ingest.
var ErrTooLarge = errors.New("nartree: the archive is too large")

// Node is the NAR's root node as an entry named "nar".
type Node = fstree.Entry

// itemChunker returns the chunker of file indexes and directories.
func itemChunker() chunkers.ItemChunker {
	return chunkers.NewItemChunker(ingest.DefaultItemBits)
}

// entry returns an entry with the fixed metadata.
func entry(name string, mode uint64) fstree.Entry {
	return fstree.Entry{Name: []byte(name), Mode: mode, UID: 0, GID: 0, Mtime: mtime}
}

// Ingest reads one NAR of at most limit bytes from r, emits its objects
// and returns its root node with the SHA-256 and length of the bytes read.
// Errors of the archive wrap nar.ErrMalformed, or ErrTooLarge as soon as
// more than limit bytes of it were read; an error of r and an error of
// emit are returned as they are. On an error the node, hash and length are
// zero. The limit of what narrenhaus takes is MaxNARSize.
//
// Objects are emitted children before parents, an object once for each
// place it has in the tree, and each with bytes of its own, which emit
// may keep. Memory in use does not grow with the archive: it is one chunk
// of a file, the chunker's buffer, and the open directories' entries that
// do not yet fill an object.
func Ingest(r io.Reader, emit fstree.Emit, limit uint64) (node Node, narHash [32]byte, narSize uint64, err error) {
	read := &tally{r: r, sum: sha256.New(), limit: limit}
	in := &ingester{emit: emit}
	if err := nar.Parse(read, in); err != nil {
		return Node{}, [32]byte{}, 0, err
	}
	// Parse read r to its end and refused any byte after the root node,
	// so what was read is the archive, no more and no less.
	read.sum.Sum(narHash[:0])
	return in.root, narHash, read.n, nil
}

// tally hashes and counts what is read through it, and fails with
// ErrTooLarge at the read that takes the count beyond limit.
type tally struct {
	r     io.Reader
	sum   hash.Hash
	n     uint64
	limit uint64
}

func (t *tally) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if uint64(n) > t.limit-t.n {
		return 0, fmt.Errorf("%w: it has more than %d bytes", ErrTooLarge, t.limit)
	}
	t.sum.Write(p[:n])
	t.n += uint64(n)
	return n, err
}

// ingester is the nar.Handler of Ingest.
type ingester struct {
	emit fstree.Emit
	// open holds the directories begun and not yet left, the innermost
	// last. The parser bounds their number by nar.MaxDepth.
	open []openDirectory
	root Node
}

type openDirectory struct {
	name    string
	builder *fstree.DirBuilder
}

// add puts a finished node where it belongs: into the innermost open
// directory, or, for the root node, into the result under its fixed name.
// The parser gives the entries of a directory in byte order of name,
// which is the order a DirBuilder wants.
func (in *ingester) add(e fstree.Entry) error {
	if len(in.open) == 0 {
		e.Name = []byte(nodeName)
		in.root = e
		return nil
	}
	return in.open[len(in.open)-1].builder.AddEntry(in.emit, e)
}

func (in *ingester) Directory(name string) error {
	in.open = append(in.open, openDirectory{name: name, builder: fstree.NewDirBuilder(itemChunker())})
	return nil
}

func (in *ingester) Leave() error {
	last := in.open[len(in.open)-1]
	in.open = in.open[:len(in.open)-1]
	k, err := last.builder.Finish(in.emit)
	if err != nil {
		return err
	}
	e := entry(last.name, modeDirectory)
	e.ContentKey = k[:]
	return in.add(e)
}

func (in *ingester) Regular(name string, executable bool, size uint64, body io.Reader) error {
	var k key.Key
	var err error
	if size <= chunkers.DefaultMinSize {
		// The chunker never cuts a file this short, so it is one Blob,
		// and building it directly spares the chunker's buffer, which is
		// many times the file. Most files of a store path are this short.
		data := make([]byte, size)
		if _, err := io.ReadFull(body, data); err != nil {
			return err
		}
		k, err = blob(data, in.emit)
	} else {
		k, err = File(body, in.emit)
	}
	if err != nil {
		return err
	}
	mode := uint64(modeRegular)
	if executable {
		mode = modeExecutable
	}
	e := entry(name, mode)
	e.ContentKey = k[:]
	return in.add(e)
}

func (in *ingester) Symlink(name, target string) error {
	e := entry(name, modeSymlink)
	e.LinkTarget = []byte(target)
	return in.add(e)
}

// blob emits data as one Blob and returns its key. The object keeps data.
func blob(data []byte, emit fstree.Emit) (key.Key, error) {
	obj, err := fstree.EncodeBlob(data)
	if err != nil {
		return key.Key{}, err
	}
	if err := emit(obj); err != nil {
		return key.Key{}, err
	}
	return obj.Key, nil
}

// File emits the content objects of a file and returns its content key,
// as core's ingest builds one: the Blob of a file of one chunk, the empty
// Blob for an empty file, FileNodes over the Blobs of a longer one. r is
// read to its end; an error of r and an error of emit are returned as
// they are.
func File(r io.Reader, emit fstree.Emit) (key.Key, error) {
	index := fstree.NewFileIndexBuilder(itemChunker())
	chunks := 0
	err := chunkers.SplitBytes(r, nil, func(chunk []byte) error {
		chunks++
		k, err := blob(chunk, emit)
		if err != nil {
			return err
		}
		return index.AddChild(emit, k, nil)
	})
	if err != nil {
		return key.Key{}, err
	}
	if chunks == 0 {
		k, err := blob([]byte{}, emit)
		if err != nil {
			return key.Key{}, err
		}
		if err := index.AddChild(emit, k, nil); err != nil {
			return key.Key{}, err
		}
	}
	return index.Finish(emit)
}
