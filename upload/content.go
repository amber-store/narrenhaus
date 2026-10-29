package upload

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"time"

	"github.com/amber-store/core/amberpack"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"

	"github.com/amber-store/narrenhaus/decompress"
	"github.com/amber-store/narrenhaus/nar"
	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/remote"
)

// state is where the content work for a NAR hash stands.
type state int

const (
	// stateQueued: the work waits for a place among the content pushes.
	stateQueued state = iota + 1
	// stateRunning: the push is under way.
	stateRunning
	// stateFailedForNow: the push failed for a reason that may pass. The
	// NAR stays staged, and the next request that needs it starts the
	// work again.
	stateFailedForNow
	// stateFailedForGood: the server refused the push for a reason a
	// repeat does not cure. The staged NAR is gone and the reason stays,
	// until the NAR is sent again or the server is found to have the
	// content after all, pushed by someone else.
	stateFailedForGood
)

func (s state) String() string {
	switch s {
	case stateQueued:
		return "queued"
	case stateRunning:
		return "running"
	case stateFailedForNow:
		return "failed for now"
	case stateFailedForGood:
		return "failed for good"
	}
	return fmt.Sprintf("state %d", int(s))
}

// work is the entry of a NAR hash: the content work for it and the staged
// NAR the work pushes. The uploader's lock guards it. An entry is there
// from the NAR's upload until its push has succeeded or the staged NAR was
// swept away.
type work struct {
	hash   string            // the NAR's SHA-256 in Nix's base 32
	name   string            // of the content reference
	root   key.Key           // of the content reference as staged
	size   uint64            // of the NAR
	staged *packcache.Sealed // nil once the push failed for good

	state state
	// attempt is the run that is queued or under way, or the last one.
	attempt *attempt

	// What the queue keeps of it. asked: a narinfo has asked for this
	// content, at any time. arrived and askedAt are its places in the two
	// orders; in and at say where it is queued, if it is.
	asked   bool
	arrived uint64
	askedAt uint64
	in      *list.List
	at      *list.Element
}

// attempt is one run of a piece of content work, as those who wait for it
// see it: done is closed when it has ended, and then err says how. A
// failure is for now unless forGood.
type attempt struct {
	done    chan struct{}
	err     error
	forGood bool
}

// PutNAR handles PUT /nar/<urlHash>.nar[.<ext>] (spec 5.1 steps 1-7).
// urlHash is the hash in the URL; it is compared with the NAR's hash
// only when the body turns out to be uncompressed.
//
// It returns when the NAR is staged and its content work has begun, and
// does not wait for the push. Nothing of body is read when the server
// cannot be reached. Every error is an *Error.
func (u *Uploader) PutNAR(ctx context.Context, urlHash string, body io.Reader) error {
	leave, err := u.enter()
	if err != nil {
		return err
	}
	defer leave()

	// A copy is to fail at its first NAR, not after a closure was staged.
	if err := u.rem.Reachable(ctx); err != nil {
		return unreachable(err, "the server cannot be reached, so the NAR was not read")
	}
	st, err := u.stage(urlHash, body)
	if err != nil {
		return err
	}
	u.submit(st)
	return nil
}

// stagedNAR is a NAR as PutNAR staged it.
type stagedNAR struct {
	hash string // the NAR's SHA-256 in Nix's base 32
	size uint64
	root key.Key
	file *packcache.Sealed
}

// stage reads the NAR from body, undoing its compression, and writes its
// objects, the recipe and the root directory to a file under staging/
// (spec 5.1 steps 2-6). On an error nothing is left there.
func (u *Uploader) stage(urlHash string, body io.Reader) (*stagedNAR, error) {
	in, compressed, err := decompress.Reader(body)
	switch {
	case errors.Is(err, decompress.ErrUnsupported):
		return nil, failf(http.StatusUnsupportedMediaType, nil,
			"the body is neither a NAR nor one compressed with xz, zstd, gzip or bzip2")
	case err != nil:
		return nil, failf(http.StatusBadRequest, err, "the body could not be read: %v", err)
	}

	w, err := u.cache.NewStaged()
	if err != nil {
		return nil, u.unwritable(err)
	}
	defer w.Discard() // does nothing once the file is sealed

	// What ends the staging from this side is told apart from a NAR that
	// is malformed by being noted here: Ingest returns either as it got
	// it. objects counts what the file took, an object that came again
	// included.
	var stop *Error
	objects := 0
	emit := func(o fstree.Object) error {
		if u.ctx.Err() != nil {
			// Nothing more is read or written for an uploader that is
			// closed.
			stop = u.stopped()
			return stop
		}
		if u.tamper != nil {
			var keep bool
			if o, keep = u.tamper(o); !keep {
				return nil
			}
		}
		if err := w.Add(o.Key, o.Bytes); err != nil {
			stop = u.notStaged(err, o, objects)
			return stop
		}
		objects++
		return nil
	}
	node, sum, size, err := nartree.Ingest(in, emit)
	switch {
	case stop != nil:
		return nil, stop
	case errors.Is(err, nar.ErrMalformed):
		return nil, failf(http.StatusBadRequest, err, "the body is no NAR as Nix writes one: %v", err)
	case err != nil:
		// The decompressor's complaint, or the end of the connection.
		return nil, failf(http.StatusBadRequest, err, "the body could not be read to its end: %v", err)
	}
	hash := nix32.Encode(sum[:])
	if !compressed && urlHash != hash {
		// Nix names an uncompressed upload by the NAR's hash.
		return nil, failf(http.StatusBadRequest, nil,
			"the NAR has the SHA-256 %s and was sent uncompressed to the URL of %s", hash, urlHash)
	}
	root, err := nartree.Root(node, sum, size, emit)
	switch {
	case stop != nil:
		return nil, stop
	case err != nil:
		return nil, u.unwritable(err)
	}
	file, err := w.Seal()
	if err != nil {
		return nil, u.unwritable(err)
	}
	if err := u.reproduce(file, root, sum, size); err != nil {
		file.Discard()
		return nil, err
	}
	u.log.Debug("a NAR is staged", "NAR", hash, "bytes", size, "root", root.String(), "compressed", compressed)
	return &stagedNAR{hash: hash, size: size, root: root, file: file}, nil
}

// notStaged is the answer when the staged file did not take the object o
// and said err, after it had taken objects others: 507 for a write that
// failed, and 422 when it is the file that takes no such object or no
// more of them, which no repeat cures.
func (u *Uploader) notStaged(err error, o fstree.Object, objects int) *Error {
	limit := beyond(err, o, objects)
	if limit == "" {
		return u.unwritable(err)
	}
	u.log.Error("a NAR is beyond what a staged file holds; the upload is refused", "reason", limit, "error", err.Error())
	return failf(http.StatusUnprocessableEntity, err, "narrenhaus cannot keep this NAR: %s", limit)
}

// beyond names the limit of a staged file that the object o ran into when
// the file refused it with err, after objects others; "" when err is a
// write that failed. The cache has no error of its own for its limits, so
// they are looked for here: in the object, and in the count together with
// err not being what the operating system says of a write. The count takes
// an object that came again for a new one, so it is never too low.
func beyond(err error, o fstree.Object, objects int) string {
	var system *fs.PathError
	switch {
	case len(o.Bytes) > amberpack.MaxPayload:
		return fmt.Sprintf("one of its objects has %d bytes, and a pack takes none above %d", len(o.Bytes), amberpack.MaxPayload)
	case o.Key.Validate() != nil:
		return fmt.Sprintf("the key %s of one of its objects is not canonical; this is a defect of narrenhaus", o.Key)
	case objects >= packfile.MaxEntries && !errors.As(err, &system):
		return fmt.Sprintf("it makes more than the %d objects a pack holds", packfile.MaxEntries)
	}
	return ""
}

// unwritable is the answer when the staging directory did not take what
// was written to it.
func (u *Uploader) unwritable(err error) *Error {
	u.log.Error("an upload could not be staged", "error", err.Error())
	return failf(http.StatusInsufficientStorage, err, "the upload could not be written to the staging directory: %v", err)
}

// reproduce rebuilds the NAR from the staged objects as a download does
// (spec 5.1 step 6, 6.3) and compares it with what was received: sum and
// size. What narrenhaus could not give back it does not take.
func (u *Uploader) reproduce(file *packcache.Sealed, root key.Key, sum [32]byte, size uint64) error {
	defect := func(format string, args ...any) *Error {
		why := fmt.Sprintf(format, args...)
		u.log.Error("defect: a NAR does not rebuild from the objects made of it; the upload is refused",
			"NAR", nix32.Encode(sum[:]), "bytes", size, "root", root.String(), "reason", why)
		return failf(http.StatusUnprocessableEntity, nil,
			"narrenhaus cannot reproduce this NAR from the objects it made of it (%s); this is a defect of narrenhaus", why)
	}
	// A failed read of the staged file is the disk's doing; anything else
	// is a tree that does not hold what was put into it.
	broken := func(err error) bool {
		return errors.Is(err, nartree.ErrBroken) || errors.Is(err, nartree.ErrUnknownRecipe) || errors.Is(err, packcache.ErrAbsent)
	}

	opened, err := nartree.Open(root, file.Get)
	switch {
	case err != nil && broken(err):
		return defect("%v", err)
	case err != nil:
		return u.unwritable(err)
	case opened.NarHash != sum || opened.NarSize != size:
		return defect("the recipe states %d bytes with the SHA-256 %s", opened.NarSize, nix32.Encode(opened.NarHash[:]))
	}
	hash := sha256.New()
	out := &rebuilt{ctx: u.ctx, w: hash, limit: size}
	err = nartree.WriteNAR(out, opened.Node, file.Get)
	switch {
	case errors.Is(out.err, errTooLong):
		return defect("the rebuilt NAR is longer than the %d bytes received", size)
	case out.err != nil:
		return u.stopped()
	case err != nil && broken(err):
		return defect("%v", err)
	case err != nil:
		return u.unwritable(err)
	case out.n != size:
		return defect("the rebuilt NAR has %d bytes, %d were received", out.n, size)
	}
	if got := [32]byte(hash.Sum(nil)); got != sum {
		return defect("the rebuilt NAR has the SHA-256 %s", nix32.Encode(got[:]))
	}
	return nil
}

var errTooLong = errors.New("the NAR is longer than was received")

// rebuilt is what a NAR is rebuilt into: it passes on what is written to
// it, counts it, refuses the write that would take the count beyond limit,
// and fails once ctx is done, which is the way to end nartree.WriteNAR.
// err is the error it failed with.
type rebuilt struct {
	ctx   context.Context
	w     io.Writer
	limit uint64
	n     uint64
	err   error
}

func (r *rebuilt) Write(p []byte) (int, error) {
	if r.err == nil {
		r.err = r.ctx.Err()
	}
	if r.err == nil && uint64(len(p)) > r.limit-r.n {
		r.err = errTooLong
	}
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.w.Write(p)
	r.n += uint64(n)
	r.err = err
	return n, err
}

// submit begins the content work for a NAR that was staged (spec 5.1 step
// 7), or leaves it to the work there is for its hash.
func (u *Uploader) submit(st *stagedNAR) {
	u.mu.Lock()
	drop := st.file
	w := u.entries[st.hash]
	switch {
	case w == nil:
		w = &work{hash: st.hash, name: u.store.ContentName(st.hash), root: st.root, size: st.size, staged: st.file}
		u.entries[st.hash] = w
		drop = nil
		u.startLocked(w)
	case w.state == stateFailedForGood:
		// The entry is replaced: the work starts on the new copy. That a
		// narinfo asked for this content is not forgotten.
		w.root, w.size, w.staged = st.root, st.size, st.file
		drop = nil
		u.startLocked(w)
	case w.state == stateFailedForNow:
		// The NAR that is staged is the same one: the work starts again
		// on it.
		u.startLocked(w)
	}
	u.mu.Unlock()
	if drop != nil {
		drop.Discard()
	}
}

// startLocked queues w for a new attempt and gives it a place if one is
// free.
func (u *Uploader) startLocked(w *work) {
	w.state = stateQueued
	w.attempt = &attempt{done: make(chan struct{})}
	u.queue.push(w)
	u.dispatchLocked()
}

// dispatchLocked starts queued work while places are free.
func (u *Uploader) dispatchLocked() {
	for u.running < u.contentPushes && !u.closed && u.ctx.Err() == nil {
		w := u.queue.next()
		if w == nil {
			return
		}
		w.state = stateRunning
		u.running++
		u.jobs.Add(1)
		go u.run(w, w.attempt, w.name, w.root, w.staged)
	}
}

// run is one attempt at the content work of w: it pushes file, the staged
// NAR with root, as the reference name, and settles the entry by how that
// ended.
func (u *Uploader) run(w *work, a *attempt, name string, root key.Key, file *packcache.Sealed) {
	defer u.jobs.Done()
	err := u.pushContent(name, root, file)

	u.mu.Lock()
	a.err = err
	switch {
	case err == nil:
		// From here on the server says what there is.
		w.staged = nil
		delete(u.entries, w.hash)
	case remote.ForGood(err):
		a.forGood = true
		w.state = stateFailedForGood
		w.staged = nil
	default:
		w.state = stateFailedForNow
	}
	u.running--
	u.dispatchLocked()
	u.restLocked()
	u.mu.Unlock()

	switch {
	case err == nil:
	case a.forGood:
		file.Discard()
		u.log.Error("the server refuses this content for good; its staged NAR is dropped", "name", name, "root", root.String(), "error", err.Error())
	case u.ctx.Err() != nil:
		u.log.Debug("a content push was given up", "name", name, "error", err.Error())
	default:
		u.log.Warn("a content push failed for now; its NAR stays staged", "name", name, "root", root.String(), "error", err.Error())
	}
	close(a.done)
}

// pushContent is the content work (spec 5.1): unless the server has the
// reference name already, push it from file and move file into the cache
// as the complete pack of root. On an error file is left as it is.
func (u *Uploader) pushContent(name string, root key.Key, file *packcache.Sealed) error {
	started := time.Now()
	there, _, err := u.rem.Lookup(u.ctx, name)
	switch {
	case err == nil:
		// First writer wins.
		if there != root {
			u.log.Warn("the server has this content reference with another root; it is left as it is, and whether it can be served shows when it is fetched",
				"name", name, "root on the server", there.String(), "root staged", root.String())
		} else {
			u.log.Info("content not pushed: the server has it already", "name", name, "root", root.String())
		}
		file.Discard()
		return nil
	case !errors.Is(err, client.ErrNotFound):
		return fmt.Errorf("asking the server for %s: %w", name, err)
	}

	res, err := u.rem.Push(u.ctx, file, name, root, client.PushOptions{MinDedup: u.minDedup, TempDir: u.cache.TempDir()})
	if err != nil {
		return err
	}
	u.place(name, root, file)
	u.logPush("content pushed", name, res, started)
	return nil
}

// logPush writes the one line an upload gets in the log.
func (u *Uploader) logPush(msg, name string, res client.PushResult, started time.Time) {
	args := []any{"name", name, "root", res.Root.String(), "took", time.Since(started).Round(time.Millisecond)}
	switch {
	case res.Stored:
		args = append(args, "pack", "the server had one of this root")
	case res.Parent != nil:
		args = append(args, "pack", "patch", "parent", res.Parent.String(), "objects", res.Objects, "bytes", res.Bytes, "uploaded", res.DataSize)
	default:
		args = append(args, "pack", "base", "objects", res.Objects, "bytes", res.Bytes, "uploaded", res.DataSize)
	}
	u.log.Info(msg, args...)
}

// HeadNAR answers HEAD /nar/<narHash>.nar for an extension-less URL:
// true when content work for the hash is queued or running or the
// server has the reference. An *Error with Status 502 when the server
// could not be asked.
//
// It downloads nothing. A hash that is no SHA-256 in Nix's base 32 names
// no reference: false, and the server is not asked. When the server has
// the reference of a NAR whose push it had refused for good, the entry
// with the refusal is dropped: the narinfo that follows is not held to it.
func (u *Uploader) HeadNAR(ctx context.Context, narHash string) (bool, error) {
	leave, err := u.enter()
	if err != nil {
		return false, err
	}
	defer leave()

	u.mu.Lock()
	w := u.entries[narHash]
	busy := w != nil && (w.state == stateQueued || w.state == stateRunning)
	var refused *attempt
	if w != nil && w.state == stateFailedForGood {
		refused = w.attempt
	}
	u.mu.Unlock()
	switch {
	case busy:
		return true, nil
	case !isNarHash(narHash):
		return false, nil
	}
	_, _, err = u.rem.Lookup(ctx, u.store.ContentName(narHash))
	switch {
	case err == nil:
		if refused != nil {
			// Nix sends no NAR after this answer, so the narinfo that
			// follows must not meet the refusal of a push that is past.
			u.forget(w, refused)
		}
		return true, nil
	case errors.Is(err, client.ErrNotFound):
		return false, nil
	}
	return false, unreachable(err, "the server could not be asked whether it has this NAR")
}
