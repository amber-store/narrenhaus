package nartree

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
)

// The keys below are frozen. They change when the chunking library, the
// encoding of an object, the recipe or the metadata convention changes,
// and any such change breaks sharing with every reference already
// stored: a failure here is not to be answered by pasting new keys.
const (
	// frozenRoot is the root key of the content reference of
	// frozenArchive.
	frozenRoot = "0d5b725a3aef862921c8ac3f718074a37106a6d4a41a098fd02a45900b0d0221"
	// frozenFile is the content key of the first 8 MiB of the noise of
	// seed 2026.
	frozenFile = "764507c89d251a2d8a7bad6fb9e61cc2b203a1f2d3fee34be6734cef00008012"
	// frozenNoise is the first 16 bytes of that noise: SplitMix64 from
	// the state 2026, each word little-endian.
	frozenNoise = "238d949198559cdb5d4535ed7d92bc78"
)

// frozenArchive is the fixed small archive: a directory holding a plain
// file, an executable, a subdirectory with one file, and a symlink.
var frozenArchive = []event{
	dir(""),
	file("file", "plain\n"),
	link("link", "file"),
	script("run", "#!/bin/sh\n"),
	dir("sub"),
	file("inner", "x"),
	leave(),
	leave(),
}

func TestFrozenKeys(t *testing.T) {
	archive := write(t, frozenArchive)
	in := ingestArchive(t, archive)
	if got := in.root.String(); got != frozenRoot {
		t.Errorf("the root of the fixed archive is\n %s, frozen as\n %s", got, frozenRoot)
	}

	// The tree behind the key is the one described, as core reads it.
	get := in.store.get
	content := func(e fstree.Entry) string {
		t.Helper()
		k, err := key.Parse(e.ContentKey)
		if err != nil {
			t.Fatalf("%s: content key: %v", e.Name, err)
		}
		var b bytes.Buffer
		if err := fstree.WriteContent(&b, k, get); err != nil {
			t.Fatalf("%s: %v", e.Name, err)
		}
		return b.String()
	}
	list := func(k []byte) []fstree.Entry {
		t.Helper()
		dk, err := key.Parse(k)
		if err != nil {
			t.Fatalf("directory key: %v", err)
		}
		entries, err := fstree.CollectEntries(dk, get)
		if err != nil {
			t.Fatal(err)
		}
		return entries
	}
	names := func(entries []fstree.Entry) string {
		var b bytes.Buffer
		for _, e := range entries {
			b.Write(e.Name)
			b.WriteByte(' ')
		}
		return b.String()
	}
	top := list(in.root[:])
	if names(top) != "nar recipe " {
		t.Fatalf("the root holds %q", names(top))
	}
	sum := sha256.Sum256(archive)
	if got, want := content(top[1]), string(Recipe(sum, uint64(len(archive)))); got != want {
		t.Errorf("recipe %s, want %s", got, want)
	}
	if top[0].Mode != 0o040555 {
		t.Errorf("nar has mode %#o", top[0].Mode)
	}
	entries := list(top[0].ContentKey)
	if names(entries) != "file link run sub " {
		t.Fatalf("the store path holds %q", names(entries))
	}
	if entries[0].Mode != 0o100444 || content(entries[0]) != "plain\n" {
		t.Errorf("file: mode %#o, contents %q", entries[0].Mode, content(entries[0]))
	}
	if entries[1].Mode != 0o120777 || string(entries[1].LinkTarget) != "file" {
		t.Errorf("link: mode %#o, target %q", entries[1].Mode, entries[1].LinkTarget)
	}
	if entries[2].Mode != 0o100555 || content(entries[2]) != "#!/bin/sh\n" {
		t.Errorf("run: mode %#o, contents %q", entries[2].Mode, content(entries[2]))
	}
	if entries[3].Mode != 0o040555 {
		t.Errorf("sub: mode %#o", entries[3].Mode)
	}
	sub := list(entries[3].ContentKey)
	if names(sub) != "inner " || sub[0].Mode != 0o100444 || content(sub[0]) != "x" {
		t.Errorf("sub holds %q", names(sub))
	}

	// A file of several chunks.
	const size = 8 << 20
	o := newObjects()
	k, err := File(io.LimitReader(newNoise(2026), size), o.emit)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	if got := k.String(); got != frozenFile {
		t.Errorf("the key of the seeded file is\n %s, frozen as\n %s", got, frozenFile)
	}
	if k.Type() != key.FileNode || k.Length() != size {
		t.Errorf("the seeded file's key is a %s of %d bytes, want a FileNode of %d", k.Type(), k.Length(), size)
	}
	// The noise itself is pinned, so that the key above is that of known
	// bytes.
	if got := hex.EncodeToString(noiseBytes(2026, 16)); got != frozenNoise {
		t.Errorf("the noise of seed 2026 begins %s, frozen as %s", got, frozenNoise)
	}
	var back bytes.Buffer
	if err := fstree.WriteContent(&back, k, o.get); err != nil {
		t.Fatalf("reading the seeded file back: %v", err)
	}
	if !bytes.Equal(back.Bytes(), noiseBytes(2026, size)) {
		t.Errorf("the seeded file read back with core differs from what was ingested")
	}
}
