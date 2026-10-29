// Package upload takes what Nix uploads, a NAR and then its narinfo, and
// writes the two references of the store path on the server.
//
// A NAR is turned into core objects as it arrives and staged in the cache
// directory; the request is answered when the staged copy has been shown
// to rebuild to the NAR that was sent, and the push of the content
// reference goes on after it. A narinfo is answered when both references
// are on the server: it waits for the push of its content and then pushes
// the narinfo reference.
//
// The work belongs to the uploader and not to the request that began it:
// it runs under the uploader's context, goes on when the client leaves,
// and is joined by every later request for the same NAR hash or store
// path. Per NAR hash there is an entry while its content work is queued or
// running, after it failed for now (the staged NAR stays, and the next
// request that needs it starts the work again), and after it failed for
// good (the reason stays, until the NAR is sent again or the server is
// found to have the content after all). A finished push leaves no entry:
// from then on the server says what there is.
//
// A few content pushes run at once and the rest wait, those a narinfo has
// asked for before the others. Narinfo pushes have places of their own.
package upload

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"

	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/refs"
	"github.com/amber-store/narrenhaus/remote"
)

const (
	defaultContentPushes = 4
	defaultNarinfoPushes = 8
	defaultMinDedup      = 0.5
	defaultIdleExpiry    = 15 * time.Minute

	// narinfoMinDedup is above any share a pack can hold, so a narinfo is
	// always pushed as a base pack (spec 4.4).
	narinfoMinDedup = 2

	// maxMsg is the most bytes of an Error's message. Nix shows the body
	// of an answer only when it is below 1024 bytes.
	maxMsg = 1000

	// narHashLen is the length of a SHA-256 in Nix's base 32.
	narHashLen = 52
)

// Error is a failed upload request with the HTTP status to answer.
type Error struct {
	Status int    // 400, 415, 422, 502, 507
	Msg    string // plain text, under 1024 bytes

	cause error
}

func (e *Error) Error() string { return e.Msg }

// Unwrap returns what made the request fail, when that was an error of
// its own: the context's, for a request that ended before its work.
func (e *Error) Unwrap() error { return e.cause }

// failf returns an Error with status whose message is one line of text
// short enough for the body of an answer.
func failf(status int, cause error, format string, args ...any) *Error {
	return &Error{Status: status, Msg: oneLine(fmt.Sprintf(format, args...)), cause: cause}
}

// oneLine makes s a line of printable text of at most maxMsg bytes.
func oneLine(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == utf8.RuneError:
			r = '?'
		case unicode.IsSpace(r) || !unicode.IsPrint(r):
			r = ' '
		}
		if b.Len()+utf8.RuneLen(r) > maxMsg-len("...") {
			b.WriteString("...")
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Options configure an Uploader.
type Options struct {
	ContentPushes int           // 0 means 4
	NarinfoPushes int           // 0 means 8
	MinDedup      float64       // 0 means 0.5
	IdleExpiry    time.Duration // 0 means 15 minutes
	Log           *slog.Logger
}

// Uploader stages and pushes what Nix uploads. It is safe for concurrent
// use.
type Uploader struct {
	ctx    context.Context
	cancel context.CancelFunc
	cache  *packcache.Cache
	rem    *remote.Remote
	store  *refs.Store
	log    *slog.Logger

	contentPushes int
	minDedup      float64
	idleExpiry    time.Duration
	// narinfoPlaces are the places of the narinfo pushes that may run at
	// once.
	narinfoPlaces chan struct{}

	mu     sync.Mutex
	closed bool
	// entries are the NARs this process knows of, by their hashes in
	// Nix's base 32: see work.
	entries map[string]*work
	// queue is the content work that waits; running counts the content
	// pushes under way.
	queue   queue
	running int
	// narinfos is the narinfo work under way, by the store path's hash.
	narinfos map[string]*flight
	// requests counts the upload requests being handled.
	requests int
	// idleSince is when the uploader last had nothing to do; it means
	// something only while that is so.
	idleSince time.Time
	// jobs counts the goroutines of content and narinfo work.
	jobs sync.WaitGroup

	// tamper, which only tests set, is shown each object on its way into
	// a staged NAR and returns what is written in its place, or false to
	// leave it out: the defects and the limits staging has to answer for.
	tamper func(o fstree.Object) (fstree.Object, bool)
}

// New returns an uploader whose work runs under ctx.
func New(ctx context.Context, cache *packcache.Cache, rem *remote.Remote, store *refs.Store, opts Options) *Uploader {
	u := &Uploader{
		cache:         cache,
		rem:           rem,
		store:         store,
		log:           opts.Log,
		contentPushes: opts.ContentPushes,
		minDedup:      opts.MinDedup,
		idleExpiry:    opts.IdleExpiry,
		entries:       make(map[string]*work),
		narinfos:      make(map[string]*flight),
		idleSince:     time.Now(),
	}
	u.ctx, u.cancel = context.WithCancel(ctx)
	if u.log == nil {
		u.log = slog.Default()
	}
	if u.contentPushes <= 0 {
		u.contentPushes = defaultContentPushes
	}
	narinfos := opts.NarinfoPushes
	if narinfos <= 0 {
		narinfos = defaultNarinfoPushes
	}
	u.narinfoPlaces = make(chan struct{}, narinfos)
	if u.minDedup == 0 {
		u.minDedup = defaultMinDedup
	}
	if u.idleExpiry <= 0 {
		u.idleExpiry = defaultIdleExpiry
	}
	return u
}

// Close ends the uploader's work: the pushes under way are given up, as
// when the context given to New ends, and Close returns when they have
// stopped. No work begins after it, and requests are answered 502. What
// is staged stays where it is; the cache removes it when it is next
// opened. Close belongs before the store's and the cache's.
//
// Close does not wait for the requests that are being handled. A PutNAR
// that is staging stops at the next object it would write, drops what it
// staged and answers 502, but one that waits for more of its body is not
// woken: it stops when the body gives it bytes or an error. So the caller
// first ends the requests, which for the command is shutting down the
// HTTP server and cutting the connections that remain, then calls Close,
// and closes the cache only when no handler runs any more.
func (u *Uploader) Close() {
	u.mu.Lock()
	u.closed = true
	u.mu.Unlock()
	u.cancel()
	u.jobs.Wait()
}

// Idle reports whether no upload request is being handled and no push is
// queued or running.
func (u *Uploader) Idle() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.idleLocked()
}

func (u *Uploader) idleLocked() bool {
	return u.requests == 0 && u.running == 0 && u.queue.len() == 0 && len(u.narinfos) == 0
}

// restLocked notes the time when the end of a request or of a piece of
// work has left the uploader with nothing to do.
func (u *Uploader) restLocked() {
	if u.idleLocked() {
		u.idleSince = time.Now()
	}
}

// Sweep deletes the staged NARs, and their entries with them, once the
// uploader has been idle for IdleExpiry at the time now: those of pushes
// that failed for now and that no request came back for. Age alone
// deletes none: while requests arrive or pushes run, everything stays.
// The command calls Sweep on a timer.
func (u *Uploader) Sweep(now time.Time) {
	u.mu.Lock()
	if !u.idleLocked() || now.Sub(u.idleSince) < u.idleExpiry {
		u.mu.Unlock()
		return
	}
	var files []*packcache.Sealed
	for hash, w := range u.entries {
		if w.state == stateFailedForNow {
			files = append(files, w.staged)
			w.staged = nil
			delete(u.entries, hash)
		}
	}
	idle := now.Sub(u.idleSince)
	u.mu.Unlock()

	for _, f := range files {
		f.Discard()
	}
	if len(files) > 0 {
		u.log.Info("staged NARs whose pushes had failed were deleted: no upload came back for them",
			"NARs", len(files), "idle for", idle.Round(time.Second))
	}
}

// enter counts an upload request in. It returns the function that counts
// it out again, or the answer to a request that comes after Close.
func (u *Uploader) enter() (leave func(), err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return nil, u.stopped()
	}
	u.requests++
	return func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		u.requests--
		u.restLocked()
	}, nil
}

// stopped is the answer to a request whose work the end of the uploader
// cut off. Nix repeats a request that was answered so.
func (u *Uploader) stopped() *Error {
	return failf(http.StatusBadGateway, context.Cause(u.ctx), "narrenhaus is shutting down")
}

// gaveUp is the answer to a request whose context ended while it waited
// for its work, which goes on.
func gaveUp(ctx context.Context) *Error {
	return failf(http.StatusBadGateway, ctx.Err(), "the request ended before its upload was done; the upload goes on")
}

// unreachable is the answer to a request for which the server could not
// be asked or did not take a push for now.
func unreachable(err error, what string) *Error {
	return failf(http.StatusBadGateway, err, "%s: %v", what, err)
}

// place moves the file of a reference that was pushed into the cache as
// the complete pack of root and records it as the local copy of name. The
// reference is on the server either way; a cache that does not take the
// pack costs a download later and no more.
func (u *Uploader) place(name string, root key.Key, file *packcache.Sealed) {
	// The root is held so that the pack is not evicted before it is
	// recorded.
	hold := u.cache.Reserve(root)
	defer hold.Release()
	if err := u.cache.Place(file, root, nil); err != nil {
		u.log.Warn("a pushed reference could not be kept in the cache", "name", name, "root", root.String(), "error", err.Error())
		return
	}
	if err := u.store.PlaceUploaded(name, root); err != nil {
		u.log.Warn("a pushed reference could not be recorded in the cache", "name", name, "root", root.String(), "error", err.Error())
	}
}

// isNarHash reports whether s is a SHA-256 in Nix's base 32.
func isNarHash(s string) bool {
	if len(s) != narHashLen {
		return false
	}
	b, err := nix32.Decode(s)
	return err == nil && len(b) == 32
}
