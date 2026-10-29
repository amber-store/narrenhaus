package refs

import (
	"context"
	"errors"
	"fmt"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"

	"github.com/amber-store/narrenhaus/narinfo"
)

// maxTextObjects is the most objects that are read for the text of one
// narinfo. The chunker cuts a megabyte into some thirty chunks at most; an
// index that names more objects than this was built to keep its reader
// busy.
const maxTextObjects = 1024

// Narinfo returns the stored narinfo of the store path with hash part
// storeHash (spec 6.2). ctx bounds the wait, not the work: when it ends
// while the narinfo's pack is being fetched, the fetch goes on for the
// next request.
//
// The server is asked where the name points, one round trip, and the
// narinfo is read from the cache if its pack is there, and fetched if
// not. When the server cannot be asked, the narinfo is given as it was
// last fetched, if its pack is still cached. No content is fetched and
// the content reference is not looked at.
//
// Every error wraps ErrNotFound. The Info is the caller's own.
func (s *Store) Narinfo(ctx context.Context, storeHash string) (*narinfo.Info, error) {
	if !isStoreHash(storeHash) {
		return nil, notFoundf("%s is not the hash of a store path", show(storeHash))
	}
	name := s.NarinfoName(storeHash)
	root, packs, found, err := s.lookup(ctx, name)
	switch {
	case err == nil && !found:
		return nil, notFoundf("the server has no reference %s", name)
	case err != nil && ctx.Err() != nil:
		return nil, gaveUp(ctx, name)
	case err != nil:
		return s.narinfoOffline(name, storeHash, err)
	}

	r := ref{kind: kindNarinfo, name: name, hash: storeHash, root: root}
	hold, err := s.obtain(ctx, r, packs)
	if err != nil {
		return nil, err
	}
	defer hold.Release()
	info, err := readNarinfo(r, hold.Get)
	if err != nil {
		return nil, s.unreadable(r, err)
	}
	return info, nil
}

// narinfoOffline answers for a narinfo when the server could not be
// asked, which cause says: from the record of the name and its packs, if
// they are there.
func (s *Store) narinfoOffline(name, storeHash string, cause error) (*narinfo.Info, error) {
	root, hold, ok := s.fromRecord(name, false)
	if !ok {
		s.log.Debug("the server could not be asked and the narinfo is not cached", "name", name, "error", cause.Error())
		return nil, fmt.Errorf("%w: %s is not cached and the server could not be asked: %w", ErrNotFound, name, cause)
	}
	defer hold.Release()
	r := ref{kind: kindNarinfo, name: name, hash: storeHash, root: root}
	info, err := readNarinfo(r, hold.Get)
	if err != nil {
		return nil, s.unreadable(r, err)
	}
	s.log.Debug("the server could not be asked; the narinfo is answered from the cache", "name", name, "error", cause.Error())
	return info, nil
}

// HasNarinfo answers HEAD on a narinfo: a lookup and the pre-checks of
// spec 6.2 step 3, no download. It is true when the server has the
// reference and its root can be a narinfo's, and false when it has none,
// when the root cannot be one, or when the reference was found broken
// before. When the server cannot be asked, it is true if the narinfo is
// cached as it was last fetched.
//
// The error is not nil only when ctx ended before there was an answer; it
// wraps ErrNotFound then.
func (s *Store) HasNarinfo(ctx context.Context, storeHash string) (bool, error) {
	if !isStoreHash(storeHash) {
		return false, nil
	}
	name := s.NarinfoName(storeHash)
	root, packs, found, err := s.lookup(ctx, name)
	switch {
	case err == nil && !found:
		return false, nil
	case err != nil && ctx.Err() != nil:
		return false, gaveUp(ctx, name)
	case err != nil:
		_, hold, ok := s.fromRecord(name, false)
		hold.Release()
		s.log.Debug("the server could not be asked whether it has a narinfo", "name", name, "cached", ok, "error", err.Error())
		return ok, nil
	}

	r := ref{kind: kindNarinfo, name: name, hash: storeHash, root: root}
	if _, refused := s.refusedFor(r); refused {
		return false, nil
	}
	if err := admit(r.kind, root, packs); err != nil {
		s.settle(r, nil, err)
		return false, nil
	}
	return true, nil
}

// readNarinfo reads the narinfo r from its objects and holds it to what
// narrenhaus stores for the store path of r's name (spec 6.2 step 4): the
// text parses, its StorePath has the hash asked for, its URL is that of
// the uncompressed NAR and its compression is none. An error wraps
// errBroken when the text is not that, and errDamaged when an object
// could not be read.
func readNarinfo(r ref, get func(key.Key) ([]byte, error)) (*narinfo.Info, error) {
	text, err := readText(r.root, get)
	if err != nil {
		return nil, err
	}
	info, err := narinfo.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errBroken, err)
	}
	switch {
	case info.HashPart() != r.hash:
		return nil, brokenf("it is the narinfo of %s, not of a store path with the hash %s", show(info.StorePath), r.hash)
	case info.URL != info.NarURL():
		return nil, brokenf("its URL is %s, narrenhaus stores %s", show(info.URL), show(info.NarURL()))
	case info.Compression != "none":
		return nil, brokenf("its compression is %s, narrenhaus stores none", show(info.Compression))
	}
	return info, nil
}

// Sentinels of readText's own, for what fstree.WriteContent is cut short
// with.
var (
	errTooLong        = errors.New("more bytes than announced")
	errTooManyObjects = errors.New("too many objects")
)

// readText returns the bytes of the file whose content key is root. It
// reads no more bytes than the key says the file has, nor more than a
// narinfo may have, and no more than maxTextObjects objects: the length
// in the key of a file index is its writer's word, and nothing that was
// checked.
func readText(root key.Key, get func(key.Key) ([]byte, error)) ([]byte, error) {
	limit := min(root.Length(), narinfo.MaxStored)
	text := &capped{limit: limit, b: make([]byte, 0, limit)}
	var unread error
	reads := 0
	// WriteContent reads one object after the other.
	counted := func(k key.Key) ([]byte, error) {
		if reads++; reads > maxTextObjects {
			return nil, errTooManyObjects
		}
		data, err := get(k)
		if err != nil && unread == nil {
			unread = err
		}
		return data, err
	}
	err := fstree.WriteContent(text, root, counted)
	switch {
	case unread != nil:
		return nil, fmt.Errorf("%w: %w", errDamaged, unread)
	case err != nil:
		return nil, brokenf("the text of %s cannot be read: %v", root, err)
	case uint64(len(text.b)) != root.Length():
		return nil, brokenf("the text of %s has %d bytes, its key says %d", root, len(text.b), root.Length())
	}
	return text.b, nil
}

// capped collects what is written to it and refuses what would take it
// beyond limit.
type capped struct {
	limit uint64
	b     []byte
}

func (c *capped) Write(p []byte) (int, error) {
	if uint64(len(p)) > c.limit-uint64(len(c.b)) {
		return 0, errTooLong
	}
	c.b = append(c.b, p...)
	return len(p), nil
}
