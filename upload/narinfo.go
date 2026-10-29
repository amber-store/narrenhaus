package upload

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/jaccard-store/client"

	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nartree"
	"github.com/amber-store/narrenhaus/remote"
)

// flight is the narinfo work under way for one store path. done is closed
// when it has ended, and then err is its outcome: nil, or an *Error.
type flight struct {
	text string // the narinfo it writes, as stored
	done chan struct{}
	err  error
}

// PutNarinfo handles PUT /<storeHash>.narinfo (spec 5.3).
//
// It returns nil when both references of the store path are on the
// server: it waits for the content push of the narinfo's NAR, starting it
// again if it had failed for now, and then pushes the narinfo reference
// unless the server has one. The work goes on when ctx ends; the answer
// to the request is a 502 then. Every error is an *Error.
func (u *Uploader) PutNarinfo(ctx context.Context, storeHash string, body []byte) error {
	leave, err := u.enter()
	if err != nil {
		return err
	}
	defer leave()

	if len(body) > narinfo.MaxStored {
		return failf(http.StatusBadRequest, nil, "the narinfo has more than %d bytes", narinfo.MaxStored)
	}
	info, err := narinfo.Parse(body)
	if err != nil {
		return failf(http.StatusBadRequest, err, "%v", err)
	}
	if got := info.HashPart(); got != storeHash {
		return failf(http.StatusBadRequest, nil, "the narinfo is of the store path %s and was sent to the URL of %s", info.StorePath, storeHash)
	}
	stored := info.Stored()
	if len(stored) > narinfo.MaxStored {
		return failf(http.StatusBadRequest, nil, "the narinfo would be stored as %d bytes, more than the %d a narinfo may have", len(stored), narinfo.MaxStored)
	}

	for {
		u.mu.Lock()
		if u.closed {
			u.mu.Unlock()
			return u.stopped()
		}
		f := u.narinfos[storeHash]
		if f == nil {
			f = &flight{text: string(stored), done: make(chan struct{})}
			u.narinfos[storeHash] = f
			u.jobs.Add(1)
			go u.runNarinfo(f, storeHash, info, stored)
		}
		u.mu.Unlock()

		select {
		case <-f.done:
		case <-ctx.Done():
			return gaveUp(ctx)
		}
		if f.text == string(stored) {
			return f.err
		}
		// The work that ended was for another narinfo of this store path.
		// Its outcome is not this one's: once more, which finds its
		// reference on the server if it wrote one.
	}
}

// runNarinfo is the narinfo work of one store path.
func (u *Uploader) runNarinfo(f *flight, storeHash string, info *narinfo.Info, stored []byte) {
	defer u.jobs.Done()
	err := u.pushNarinfo(storeHash, info, stored)

	u.mu.Lock()
	f.err = err
	delete(u.narinfos, storeHash)
	u.restLocked()
	u.mu.Unlock()
	close(f.done)
}

// pushNarinfo makes sure that the content info names is on the server and
// then writes the narinfo reference, unless the server has one (spec 5.3
// steps 2-4). Its errors are *Error.
func (u *Uploader) pushNarinfo(storeHash string, info *narinfo.Info, stored []byte) error {
	if err := u.awaitContent(info); err != nil {
		return err
	}

	// A narinfo push has a place of its own and waits behind no content.
	select {
	case u.narinfoPlaces <- struct{}{}:
	case <-u.ctx.Done():
		return u.stopped()
	}
	defer func() { <-u.narinfoPlaces }()

	started := time.Now()
	name := u.store.NarinfoName(storeHash)
	_, _, err := u.rem.Lookup(u.ctx, name)
	switch {
	case err == nil:
		// First writer wins.
		u.log.Info("narinfo not pushed: the server has one of this store path already", "name", name)
		return nil
	case !errors.Is(err, client.ErrNotFound):
		return u.notAsked(err, name)
	}

	w, err := u.cache.NewStaged()
	if err != nil {
		return u.unwritable(err)
	}
	defer w.Discard() // does nothing once the file is sealed
	root, err := nartree.File(bytes.NewReader(stored), func(o fstree.Object) error { return w.Add(o.Key, o.Bytes) })
	if err != nil {
		return u.unwritable(err)
	}
	file, err := w.Seal()
	if err != nil {
		return u.unwritable(err)
	}
	res, err := u.rem.Push(u.ctx, file, name, root, client.PushOptions{MinDedup: narinfoMinDedup, TempDir: u.cache.TempDir()})
	switch {
	case err == nil:
	case remote.ForGood(err):
		file.Discard()
		u.log.Error("the server refuses this narinfo for good", "name", name, "root", root.String(), "error", err.Error())
		return failf(http.StatusUnprocessableEntity, err, "the server refuses the narinfo: %v", err)
	case u.ctx.Err() != nil:
		file.Discard()
		return u.stopped()
	default:
		file.Discard()
		u.log.Warn("a narinfo push failed for now", "name", name, "root", root.String(), "error", err.Error())
		return unreachable(err, "the narinfo could not be pushed for now")
	}
	u.place(name, root, file)
	u.logPush("narinfo pushed", name, res, started)
	return nil
}

// awaitContent returns nil when the content reference of the NAR that
// info names is on the server (spec 5.3 step 2). Work for it that is
// queued or running is marked as asked for by a narinfo and waited for;
// work that failed for now is started again first. Without an entry the
// server is asked.
func (u *Uploader) awaitContent(info *narinfo.Info) error {
	hash := info.NarHashNix32()
	u.mu.Lock()
	w := u.entries[hash]
	if w == nil {
		u.mu.Unlock()
		name := u.store.ContentName(hash)
		_, _, err := u.rem.Lookup(u.ctx, name)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, client.ErrNotFound):
			return failf(http.StatusBadRequest, nil,
				"no NAR with the SHA-256 %s was received and the server has none: the NAR of a store path comes before its narinfo", hash)
		}
		return u.notAsked(err, name)
	}
	if w.size != info.NarSize {
		size := w.size
		u.mu.Unlock()
		return failf(http.StatusBadRequest, nil,
			"the narinfo states a NarSize of %d, and the NAR %s that was received has %d bytes", info.NarSize, hash, size)
	}
	// Asked for, from now on: also when the work is started again later.
	u.queue.promote(w)
	if w.state == stateFailedForGood {
		reason := w.attempt.err
		u.mu.Unlock()
		return refusedContent(reason)
	}
	if w.state == stateFailedForNow {
		u.startLocked(w)
	}
	a := w.attempt
	u.mu.Unlock()

	select {
	case <-a.done:
	case <-u.ctx.Done():
		return u.stopped()
	}
	switch {
	case a.err == nil:
		return nil
	case a.forGood:
		return refusedContent(a.err)
	case u.ctx.Err() != nil:
		return u.stopped()
	}
	return unreachable(a.err, "the NAR of this store path could not be pushed for now and stays staged")
}

// refusedContent is the answer to a narinfo whose content the server
// refused for good.
func refusedContent(reason error) *Error {
	return failf(http.StatusUnprocessableEntity, reason, "the server refuses the NAR of this store path: %v", reason)
}

// notAsked is the answer when the server could not be asked for name.
func (u *Uploader) notAsked(err error, name string) *Error {
	if u.ctx.Err() != nil {
		return u.stopped()
	}
	return unreachable(err, "the server could not be asked for "+name)
}
