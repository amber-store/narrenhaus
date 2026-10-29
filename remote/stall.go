package remote

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// stallLimit is how long a bucket transfer of the default HTTP client may
// go without moving a byte. It is a variable so that tests can shorten it.
var stallLimit = 30 * time.Second

// stalled is the failure of a transfer that moved no byte for the limit.
type stalled struct{ limit time.Duration }

func (e *stalled) Error() string {
	return fmt.Sprintf("remote: the transfer moved no byte for %s", e.limit)
}

// newHTTPClient returns an HTTP client whose transfers fail once they have
// moved no byte for limit: no answer to a request, a body that stops
// arriving while it is read, or an upload the other side stops taking.
// Time the caller spends between two reads of a body does not count, and
// neither does the time a request body takes to produce its bytes.
func newHTTPClient(limit time.Duration) *http.Client {
	base, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		base = base.Clone()
	} else {
		base = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	// Packs come from one host, several at a time.
	base.MaxIdleConnsPerHost = 16
	return &http.Client{Transport: &stallTransport{base: base, limit: limit}}
}

// stallTransport gives every request of base the stall limit.
type stallTransport struct {
	base  http.RoundTripper
	limit time.Duration
}

func (t *stallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(req.Context())
	stall := func() { cancel(&stalled{limit: t.limit}) }

	// The watch over the request runs from now until the answer's header
	// is here. It rests only while the body is asked for more.
	sending := newWatch(t.limit, stall)
	sending.arm()
	req = req.Clone(ctx)
	if req.Body != nil && req.Body != http.NoBody {
		req.Body = &upload{ReadCloser: req.Body, watch: sending}
		if get := req.GetBody; get != nil {
			req.GetBody = func() (io.ReadCloser, error) {
				body, err := get()
				if err != nil {
					return nil, err
				}
				return &upload{ReadCloser: body, watch: sending}, nil
			}
		}
	}

	res, err := t.base.RoundTrip(req)
	sending.end()
	if err != nil {
		if s, ok := context.Cause(ctx).(*stalled); ok {
			err = s
		}
		cancel(nil)
		return nil, err
	}
	res.Body = &download{body: res.Body, watch: newWatch(t.limit, stall), ctx: ctx, cancel: cancel}
	return res, nil
}

// CloseIdleConnections closes the idle connections of base: it is what
// http.Client.CloseIdleConnections looks for in its transport.
func (t *stallTransport) CloseIdleConnections() {
	if base, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		base.CloseIdleConnections()
	}
}

// watch runs stall when it has been armed for limit without being armed
// again or put to rest.
type watch struct {
	limit time.Duration

	mu    sync.Mutex
	timer *time.Timer
	stall func()
	over  bool
}

func newWatch(limit time.Duration, stall func()) *watch {
	return &watch{limit: limit, stall: stall}
}

// arm starts the limit anew.
func (w *watch) arm() {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.over:
	case w.timer == nil:
		w.timer = time.AfterFunc(w.limit, w.stall)
	default:
		w.timer.Reset(w.limit)
	}
}

// rest stops the limit until the watch is armed again.
func (w *watch) rest() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timer != nil {
		w.timer.Stop()
	}
}

// end stops the limit for good: arming the watch does nothing after it.
func (w *watch) end() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.over = true
	if w.timer != nil {
		w.timer.Stop()
	}
}

// upload is a request body under a watch. The transport asks it for bytes
// and writes them out before it asks again, so the time between two reads
// is the time the other side took to take what was read.
type upload struct {
	io.ReadCloser
	watch *watch
}

func (u *upload) Read(p []byte) (int, error) {
	u.watch.rest()
	n, err := u.ReadCloser.Read(p)
	u.watch.arm()
	return n, err
}

// download is a response body under a watch that runs while a read of it
// is under way.
type download struct {
	body   io.ReadCloser
	watch  *watch
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func (d *download) Read(p []byte) (int, error) {
	d.watch.arm()
	n, err := d.body.Read(p)
	d.watch.rest()
	if err != nil && err != io.EOF {
		if s, ok := context.Cause(d.ctx).(*stalled); ok {
			err = s
		}
	}
	return n, err
}

func (d *download) Close() error {
	d.watch.end()
	err := d.body.Close()
	d.cancel(nil)
	return err
}
