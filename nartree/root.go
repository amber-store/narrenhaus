package nartree

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"

	"github.com/amber-store/narrenhaus/nix32"
)

// The fields of a recipe that every version has, and their values in
// version 1.
const (
	recipeFormat  = "narrenhaus-recipe"
	recipeVersion = "1"
	hashPrefix    = "sha256:"
)

// maxRecipe is the longest recipe Open reads to learn its format and
// version. A recipe of version 1 has at most 158 bytes; the rest is room
// for a later version to say what it is. A longer one is not read and
// counts as unknown: it cannot be version 1.
const maxRecipe = 64 << 10

// Recipe returns version 1 of the recipe for a NAR (spec 4.3): the
// archive is the canonical serialisation of the entry "nar", has narSize
// bytes and the SHA-256 narHash. The text is written exactly so, and Open
// takes no other writing of version 1.
func Recipe(narHash [32]byte, narSize uint64) []byte {
	b := make([]byte, 0, 160)
	b = append(b, `{"format":"`+recipeFormat+`","narHash":"`+hashPrefix...)
	b = append(b, nix32.Encode(narHash[:])...)
	b = append(b, `","narSize":`...)
	b = strconv.AppendUint(b, narSize, 10)
	b = append(b, `,"node":"`+nodeName+`","version":`+recipeVersion+`}`...)
	return b
}

// readRecipe returns the hash and size a recipe of version 1 states.
//
// A text that is a JSON object with a format and a version, of which
// either is not version 1's, gives ErrUnknownRecipe whatever else it
// holds. One that is no such object, or says version 1 and is not written
// as Recipe writes it, gives ErrBroken.
func readRecipe(text []byte) (narHash [32]byte, narSize uint64, err error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(text, &fields); err != nil || fields == nil {
		return narHash, 0, brokenf("the recipe is not a JSON object")
	}
	rawFormat, rawVersion := fields["format"], fields["version"]
	if rawFormat == nil || rawVersion == nil {
		return narHash, 0, brokenf("the recipe does not say its format and version")
	}
	var format string
	switch err := json.Unmarshal(rawFormat, &format); {
	case err != nil || rawFormat[0] != '"':
		return narHash, 0, fmt.Errorf("%w: a format that is no text", ErrUnknownRecipe)
	case format != recipeFormat:
		return narHash, 0, fmt.Errorf("%w: format %s", ErrUnknownRecipe, show(format))
	}
	if string(rawVersion) != recipeVersion {
		return narHash, 0, fmt.Errorf("%w: version %s of %s", ErrUnknownRecipe, show(string(rawVersion)), recipeFormat)
	}

	// Version 1 has one writing. The fields are read leniently and the
	// text is then held to what Recipe makes of them, byte for byte.
	var stated string
	if json.Unmarshal(fields["narHash"], &stated) != nil || !strings.HasPrefix(stated, hashPrefix) {
		return narHash, 0, brokenf("the recipe has no SHA-256 as its narHash")
	}
	sum, err := nix32.Decode(stated[len(hashPrefix):])
	if err != nil || len(sum) != len(narHash) {
		return narHash, 0, brokenf("the narHash of the recipe is not a SHA-256 in Nix's base 32")
	}
	narSize, err = strconv.ParseUint(string(fields["narSize"]), 10, 64)
	if err != nil {
		return narHash, 0, brokenf("the narSize of the recipe is not a size")
	}
	copy(narHash[:], sum)
	if !bytes.Equal(text, Recipe(narHash, narSize)) {
		return [32]byte{}, 0, brokenf("the recipe says version 1 and is not written as version 1 is")
	}
	return narHash, narSize, nil
}

// Root emits the recipe and the wrapper directory {nar, recipe} and
// returns the content reference's root key. node is what Ingest returned.
// An error of emit is returned as it is.
func Root(node Node, narHash [32]byte, narSize uint64, emit fstree.Emit) (key.Key, error) {
	if string(node.Name) != nodeName {
		return key.Key{}, fmt.Errorf("nartree: the node is named %s, not %q", show(string(node.Name)), nodeName)
	}
	if _, _, err := kindOf(node); err != nil {
		return key.Key{}, fmt.Errorf("nartree: the node cannot be the root of an archive: %v", err)
	}
	// A recipe is far below the size at which the chunker first cuts, so
	// its content is one Blob.
	rk, err := blob(Recipe(narHash, narSize), emit)
	if err != nil {
		return key.Key{}, err
	}
	recipe := entry(recipeName, modeRegular)
	recipe.ContentKey = rk[:]

	// The two names are in byte order as they stand.
	wrapper := fstree.NewDirBuilder(itemChunker())
	for _, e := range []fstree.Entry{node, recipe} {
		if err := wrapper.AddEntry(emit, e); err != nil {
			return key.Key{}, err
		}
	}
	return wrapper.Finish(emit)
}
