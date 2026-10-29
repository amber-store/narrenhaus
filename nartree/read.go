package nartree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"strconv"
	"strings"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"

	"github.com/amber-store/narrenhaus/nar"
)

// Getter reads an object by key; it must be safe for concurrent calls.
type Getter func(key.Key) ([]byte, error)

// Content is an opened content reference.
type Content struct {
	Node    Node
	NarHash [32]byte
	NarSize uint64
}

var (
	// ErrUnknownRecipe is a recipe of another format or version: the
	// reference is not to be served, and is not broken either, since a
	// later narrenhaus may have written it.
	ErrUnknownRecipe = errors.New("nartree: unknown recipe")
	// ErrBroken is a reference that is not the wrapper of spec 4.2, has
	// an unreadable recipe, or holds a tree no archive can be written of.
	// An object that could not be read is not this: the getter's error
	// is returned in its place.
	ErrBroken = errors.New("nartree: broken reference")
)

// maxIndexDepth bounds the levels of a file's index, and of a
// directory's. Core's builders make a level at least twice as small as
// the one below, so a file of 2^63 bytes stays far under it; a deeper
// index was built to exhaust the reader.
const maxIndexDepth = 64

// brokenf returns an error that wraps ErrBroken.
func brokenf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrBroken, fmt.Sprintf(format, args...))
}

// unread is the error of an object the getter did not give. It wraps the
// getter's error and not ErrBroken.
func unread(k key.Key, err error) error {
	return fmt.Errorf("nartree: reading %s %s: %w", k.Type(), k, err)
}

// show quotes a name for an error, cut so that the error stays short.
func show(s string) string {
	const most = 48
	if len(s) > most {
		return strconv.Quote(s[:most]) + "..."
	}
	return strconv.Quote(s)
}

// kind is what an entry is to an archive.
type kind int

const (
	kindRegular kind = iota + 1
	kindDirectory
	kindSymlink
)

// kindOf returns what e is to an archive and, for a file or a directory,
// the key of its content. Owner, time and the permissions other than the
// executable bit are not looked at: an archive holds none of them. The
// error says why e is no node of an archive.
func kindOf(e fstree.Entry) (kind, key.Key, error) {
	switch e.Mode & modeType {
	case typeRegular:
		k, err := contentKey(e, key.Blob, key.FileNode)
		return kindRegular, k, err
	case typeDirectory:
		// Core reads a Commit here as its tree. No archive is built so.
		k, err := contentKey(e, key.DirLeaf, key.DirNode)
		return kindDirectory, k, err
	case typeSymlink:
		if len(e.LinkTarget) == 0 {
			return 0, key.Key{}, errors.New("it is a symlink without a target")
		}
		return kindSymlink, key.Key{}, nil
	}
	return 0, key.Key{}, fmt.Errorf("its mode %#o is not that of a regular file, a directory or a symlink", e.Mode)
}

// contentKey returns the content key of e, which must be of type a or b.
func contentKey(e fstree.Entry, a, b key.Type) (key.Key, error) {
	k, err := key.Parse(e.ContentKey)
	if err != nil {
		return key.Key{}, fmt.Errorf("its content key is none (%v)", err)
	}
	if t := k.Type(); t != a && t != b {
		return key.Key{}, fmt.Errorf("its content key is a %s, not a %s or a %s", t, a, b)
	}
	return k, nil
}

// Open reads the wrapper directory and recipe at root, two objects, and
// nothing of the store path.
//
// The error wraps ErrUnknownRecipe for a recipe of another format or
// version, ErrBroken for a root that is not the wrapper or a recipe that
// cannot be read as one, and the getter's error for an object that was
// not there to read. A root that is not a DirLeaf is refused without a
// read: a directory of two entries is never anything else.
func Open(root key.Key, get Getter) (Content, error) {
	if root.Type() != key.DirLeaf {
		return Content{}, brokenf("the root %s is a %s, the directory of a content reference is a DirLeaf", root, root.Type())
	}
	data, err := get(root)
	if err != nil {
		return Content{}, unread(root, err)
	}
	entries, err := fstree.DecodeDirLeaf(data)
	if err != nil {
		return Content{}, brokenf("the root %s does not decode: %v", root, err)
	}
	if len(entries) != 2 || string(entries[0].Name) != nodeName || string(entries[1].Name) != recipeName {
		return Content{}, brokenf("the root %s is not a directory of the two entries %q and %q", root, nodeName, recipeName)
	}

	what, rk, err := kindOf(entries[1])
	switch {
	case err != nil:
		return Content{}, brokenf("the recipe of %s: %v", root, err)
	case what != kindRegular:
		return Content{}, brokenf("the recipe of %s is not a regular file", root)
	case rk.Length() > maxRecipe:
		return Content{}, fmt.Errorf("%w: a recipe of %d bytes is not of version %s", ErrUnknownRecipe, rk.Length(), recipeVersion)
	}
	var text bytes.Buffer
	if err := writeContent(&text, rk, get, 0); err != nil {
		return Content{}, err
	}
	narHash, narSize, err := readRecipe(text.Bytes())
	if err != nil {
		return Content{}, err
	}

	node := entries[0]
	if _, _, err := kindOf(node); err != nil {
		return Content{}, brokenf("the node of %s: %v", root, err)
	}
	return Content{Node: node, NarHash: narHash, NarSize: narSize}, nil
}

// WriteNAR writes the canonical NAR of node to w: directories with their
// entries in stored order, a file executable when its mode has the
// owner's execute bit, of the length its content key states, a symlink
// with its entry's target. The name of node itself is not looked at.
//
// It fails, wrapping ErrBroken, if the tree is no archive's: a file's
// bytes differ in number from its content key's length, entries are out
// of order or named as no entry can be, an entry is neither file,
// directory nor symlink, the nesting is deeper than an archive's, or an
// object does not decode. An error of get is returned wrapped and is not
// ErrBroken; an error of w is returned as it is. In each case w has been
// given the beginning of an archive and not its end.
//
// Objects are read one at a time, a file's Blobs in order, so memory in
// use does not grow with the archive. Each object's bytes go to w in one
// Write; w is given many small writes for a tree of many small nodes.
func WriteNAR(w io.Writer, node Node, get Getter) error {
	out := &archiver{w: nar.NewWriter(w), get: get}
	err := out.node("", node)
	if err == nil {
		err = out.w.Close()
	}
	if errors.Is(err, nar.ErrMisuse) {
		// The writer refuses what no archive holds, and only the tree
		// decides what it is given here.
		return brokenf("%s", strings.TrimPrefix(err.Error(), nar.ErrMisuse.Error()+": "))
	}
	return err
}

// archiver writes a tree to a nar.Writer.
type archiver struct {
	w   *nar.Writer
	get Getter
}

// node writes the entry e as a node called name. The writer checks the
// name, its order among its siblings and the depth.
func (a *archiver) node(name string, e fstree.Entry) error {
	what, k, err := kindOf(e)
	if err != nil {
		return brokenf("the entry %s is no node of an archive: %v", show(string(e.Name)), err)
	}
	switch what {
	case kindDirectory:
		if err := a.w.Directory(name); err != nil {
			return err
		}
		if err := a.directory(k, 0); err != nil {
			return err
		}
		return a.w.Leave()
	case kindRegular:
		body, err := a.w.Regular(name, e.Mode&anyExecute != 0, k.Length())
		if err != nil {
			return err
		}
		return writeContent(body, k, a.get, 0)
	default:
		return a.w.Symlink(name, string(e.LinkTarget))
	}
}

// directory writes the entries of the directory object k, a DirLeaf or a
// DirNode at the given level of the directory's index, one leaf at a
// time.
func (a *archiver) directory(k key.Key, level int) error {
	if level >= maxIndexDepth {
		return brokenf("the index of a directory is more than %d levels deep at %s", maxIndexDepth, k)
	}
	data, err := a.get(k)
	if err != nil {
		return unread(k, err)
	}
	switch k.Type() {
	case key.DirLeaf:
		entries, err := fstree.DecodeDirLeaf(data)
		if err != nil {
			return brokenf("the DirLeaf %s does not decode: %v", k, err)
		}
		for _, e := range entries {
			if err := a.node(string(e.Name), e); err != nil {
				return err
			}
		}
		return nil
	case key.DirNode:
		pairs, err := fstree.DecodeDirNode(data)
		if err != nil {
			return brokenf("the DirNode %s does not decode: %v", k, err)
		}
		for _, p := range pairs {
			ck, err := key.Parse(p.ChildKey)
			if err != nil {
				return brokenf("the DirNode %s has a child that is no key (%v)", k, err)
			}
			if t := ck.Type(); t != key.DirLeaf && t != key.DirNode {
				return brokenf("the DirNode %s has a %s as a child", k, t)
			}
			if err := a.directory(ck, level+1); err != nil {
				return err
			}
		}
		return nil
	}
	return brokenf("%s is a %s, not a directory", k, k.Type())
}

// writeContent writes the bytes of the file content k, a Blob or a
// FileNode at the given level of the file's index, to w: exactly
// k.Length() bytes, or an error.
//
// It is core's fstree.WriteContent with the lengths checked on the way
// down: a Blob must have the length its key states and a FileNode the sum
// of its children's, so a file that would come out longer or shorter
// than its key says is refused where the difference is, before the bytes
// in question are written. An error of w is returned as it is.
func writeContent(w io.Writer, k key.Key, get Getter, level int) error {
	if level >= maxIndexDepth {
		return brokenf("the index of a file is more than %d levels deep at %s", maxIndexDepth, k)
	}
	data, err := get(k)
	if err != nil {
		return unread(k, err)
	}
	switch k.Type() {
	case key.Blob:
		if uint64(len(data)) != k.Length() {
			return brokenf("the Blob %s has %d bytes, its key says %d", k, len(data), k.Length())
		}
		if len(data) == 0 {
			return nil
		}
		_, err := w.Write(data)
		return err
	case key.FileNode:
		children, err := fstree.DecodeFileNode(data)
		if err != nil {
			return brokenf("the FileNode %s does not decode: %v", k, err)
		}
		var sum, carry uint64
		for _, c := range children {
			if t := c.Type(); t != key.Blob && t != key.FileNode {
				return brokenf("the FileNode %s has a %s as a child", k, t)
			}
			if sum, carry = bits.Add64(sum, c.Length(), 0); carry != 0 {
				return brokenf("the lengths of the children of the FileNode %s exceed 64 bits", k)
			}
		}
		if sum != k.Length() {
			return brokenf("the children of the FileNode %s have %d bytes, its key says %d", k, sum, k.Length())
		}
		for _, c := range children {
			if err := writeContent(w, c, get, level+1); err != nil {
				return err
			}
		}
		return nil
	}
	return brokenf("%s is a %s, not the content of a file", k, k.Type())
}
