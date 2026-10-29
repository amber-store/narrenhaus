// Package nar reads and writes Nix archives, and only canonical ones.
//
// An archive is a sequence of strings, each a 64-bit little-endian length,
// the bytes, and zero bytes up to a multiple of 8:
//
//	archive   = "nix-archive-1" node
//	node      = "(" "type" ( regular | symlink | directory ) ")"
//	regular   = "regular" [ "executable" "" ] "contents" <bytes>
//	symlink   = "symlink" "target" <target>
//	directory = "directory" { "entry" "(" "name" <name> "node" node ")" }
//
// Entries of a directory ascend strictly by the bytes of their names. A
// tree of files has exactly one such archive, which is why a Nix store
// path can be named by the hash of its archive, and why Parse followed by
// Writer gives back the bytes read.
//
// Parse refuses whatever is not that one archive: it is as strict as the
// parser of current Nix (libutil/archive.cc) and refuses bytes after the
// root node besides. Writer refuses to write what Parse would refuse.
package nar

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Limits of the format, those of Nix's parser.
const (
	// MaxNameLen is the longest name of a directory entry, in bytes.
	MaxNameLen = 255
	// MaxTargetLen is the longest target of a symlink, in bytes.
	MaxTargetLen = 4095
	// MaxDepth is the deepest nesting, counted in nodes from the root: the
	// root is level 1, an entry of a root directory level 2, and a node at
	// level MaxDepth+1 is refused.
	MaxDepth = 64
)

const (
	// magic is the first string of an archive.
	magic = "nix-archive-1"
	// maxKeyword bounds a string read where the grammar has a keyword.
	// The longest keyword has 13 bytes; the slack lets an error show a
	// short wrong one.
	maxKeyword = 32
)

// ErrMalformed wraps every error of Parse that is the archive's fault.
var ErrMalformed = errors.New("nar: malformed archive")

// ErrMisuse wraps every error of a Writer that is its caller's fault: a
// node the format has no place for, entries out of order, a file whose
// body is not of the size announced, calls that do not make one whole
// archive. Errors of the destination are returned as they are instead.
var ErrMisuse = errors.New("nar: writer misused")

// Handler receives the nodes of an archive in order. The root node has
// the empty name. An error from a method ends Parse and is returned as is.
type Handler interface {
	// Directory is called when a directory begins; Leave when it ends.
	Directory(name string) error
	Leave() error
	// Regular must read body to its end, size bytes, before returning.
	// The body is not to be read after that.
	Regular(name string, executable bool, size uint64, body io.Reader) error
	Symlink(name, target string) error
}

// padding returns the number of zero bytes that follow a string of n
// bytes.
func padding(n uint64) int {
	return int(-n & 7)
}

// appendString appends s as a string of the format.
func appendString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(len(s)))
	b = append(b, s...)
	return appendPadding(b, uint64(len(s)))
}

// appendPadding appends the zero bytes that follow a string of n bytes.
func appendPadding(b []byte, n uint64) []byte {
	var zeros [8]byte
	return append(b, zeros[:padding(n)]...)
}

// checkName returns what is wrong with the name of a directory entry, or
// the empty string.
func checkName(name string) string {
	switch {
	case name == "":
		return "an entry name is empty"
	case len(name) > MaxNameLen:
		return fmt.Sprintf("an entry name has %d bytes, the limit is %d", len(name), MaxNameLen)
	case name == "." || name == "..":
		return fmt.Sprintf("an entry is named %q", name)
	case strings.IndexByte(name, '/') >= 0:
		return fmt.Sprintf("the entry name %s contains a slash", show(name))
	case strings.IndexByte(name, 0) >= 0:
		return fmt.Sprintf("the entry name %s contains a NUL byte", show(name))
	}
	return ""
}

// checkOrder returns what is wrong with name following prev in one
// directory, or the empty string. prev is empty for the first entry.
func checkOrder(prev, name string) string {
	switch {
	case prev == "" || prev < name:
		return ""
	case prev == name:
		return fmt.Sprintf("the entry name %s is there twice", show(name))
	}
	return fmt.Sprintf("the entry %s comes after %s: names must ascend by their bytes", show(name), show(prev))
}

// checkTarget returns what is wrong with the target of a symlink, or the
// empty string.
func checkTarget(target string) string {
	switch {
	case target == "":
		return "a symlink target is empty"
	case len(target) > MaxTargetLen:
		return fmt.Sprintf("a symlink target has %d bytes, the limit is %d", len(target), MaxTargetLen)
	case strings.IndexByte(target, 0) >= 0:
		return fmt.Sprintf("the symlink target %s contains a NUL byte", show(target))
	}
	return ""
}

// tooDeep is the complaint about a node below MaxDepth levels.
func tooDeep() string {
	return fmt.Sprintf("directories are nested deeper than %d levels", MaxDepth)
}

// show quotes a string of the archive for an error message, cut short so
// that the message stays one that fits an HTTP error body.
func show(s string) string {
	const most = 48
	if len(s) <= most {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%q... (%d bytes)", s[:most], len(s))
}
