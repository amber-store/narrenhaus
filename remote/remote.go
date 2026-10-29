// Package remote is narrenhaus's side of the jaccard-store server: one
// iroh connection, dialled on first need and shared by every lookup and
// push, and the time limits that keep a server which is slow or gone from
// holding up the requests of Nix.
//
// A connection is retired when it has shown itself broken: a stream could
// not be opened on it, a frame could not be written or read, or a lookup
// got no answer within its limit. What the server answers never retires
// it, whether a name it does not know or a request it refuses, and neither
// does anything that happens between a call and the bucket. A retired
// connection takes no new call; it is closed when the last call that runs
// on it has returned, so a push in flight is not cut by the failure of
// another request. Of the calls that fail on one connection the first
// retires it and the others find it retired: none of them touches the
// connection that was dialled since.
//
// Dials are serialised. Every call that needs a connection while one is
// being dialled waits for that dial and shares its outcome. A dial runs
// under the Remote's own context and limit, not under the context of the
// call that began it, so it is not given up with that call. After a dial
// has failed, calls fail at once for a while instead of dialling again.
package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/wire"
)

const (
	defaultLookupTimeout = 10 * time.Second
	defaultDialTimeout   = 30 * time.Second
	defaultFailFast      = 30 * time.Second
)

// Conn is one connection to the server. *node.Conn of jaccard-store is one.
type Conn interface {
	client.Opener
	Close() error
}

// Config configures a Remote. Dial is required.
type Config struct {
	// Dial connects to the server. It must return when ctx is done; the
	// connection it returns outlives ctx.
	Dial func(ctx context.Context) (Conn, error)
	// HTTP carries the bucket transfers; nil means one whose transfers
	// fail after 30 s without a byte.
	HTTP *http.Client
	// LookupTimeout bounds a lookup, the wait for a connection and the
	// round trip together; 0 means 10 s.
	LookupTimeout time.Duration
	// DialTimeout bounds a dial; 0 means 30 s.
	DialTimeout time.Duration
	// FailFast is how long after a failed dial calls fail without
	// dialling again; 0 means 30 s.
	FailFast time.Duration
	// Log receives what happens to the connection. Nil means
	// slog.Default().
	Log *slog.Logger
}

// ErrUnreachable is returned, wrapped, when the server could not be asked.
var ErrUnreachable = errors.New("remote: server unreachable")

// errClosed is what a call on a closed Remote failed of.
var errClosed = errors.New("the remote is closed")

// Remote is the shared connection to the server. It is safe for
// concurrent use.
type Remote struct {
	dial          func(ctx context.Context) (Conn, error)
	http          *http.Client
	lookupTimeout time.Duration
	dialTimeout   time.Duration
	failFast      time.Duration
	log           *slog.Logger
	// bucket fetches packs: that takes the HTTP client and no connection.
	bucket *client.Client

	// ctx is what dials run under. Close ends it.
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	// current is the connection new calls go to, nil when there is none.
	current *link
	// dialling is the dial under way, nil when there is none.
	dialling *dialling
	// failedAt and failure are the end of the last dial, if it failed.
	failedAt time.Time
	failure  error
	// links counts the connections dialled, for the log.
	links int
}

// link is one connection and the calls that run on it.
type link struct {
	conn Conn
	id   int
	// calls and retired are guarded by Remote.mu.
	calls   int
	retired bool
}

// dialling is one dial and its outcome, which is set before done is closed.
type dialling struct {
	done chan struct{}
	err  error
}

// New returns a Remote. Nothing is dialled before the first call that
// needs the server.
func New(cfg Config) *Remote {
	r := &Remote{
		dial:          cfg.Dial,
		http:          cfg.HTTP,
		lookupTimeout: cfg.LookupTimeout,
		dialTimeout:   cfg.DialTimeout,
		failFast:      cfg.FailFast,
		log:           cfg.Log,
	}
	if r.dial == nil {
		r.dial = func(context.Context) (Conn, error) { return nil, errors.New("remote: no Dial was configured") }
	}
	if r.http == nil {
		r.http = newHTTPClient(stallLimit)
	}
	if r.lookupTimeout <= 0 {
		r.lookupTimeout = defaultLookupTimeout
	}
	if r.dialTimeout <= 0 {
		r.dialTimeout = defaultDialTimeout
	}
	if r.failFast <= 0 {
		r.failFast = defaultFailFast
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	r.bucket = client.New(noServer{}, r.http)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r
}

// noServer is the connection of the client that only fetches packs.
type noServer struct{}

func (noServer) OpenStreamConn(context.Context) (net.Conn, error) {
	return nil, errors.New("remote: a fetch asks nothing of the server")
}

// Close ends the dial under way and retires the connection: it is closed
// now if no call runs on it, and otherwise when the last of them returns.
// Calls made after Close fail with ErrUnreachable.
func (r *Remote) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	l := r.current
	r.current = nil
	idle := false
	if l != nil {
		l.retired = true
		idle = l.calls == 0
	}
	r.mu.Unlock()

	r.cancel()
	if idle {
		r.closeLink(l)
	}
}

// Reachable returns nil when a connection is up or can be dialled within
// LookupTimeout, and an error wrapping ErrUnreachable otherwise.
//
// A connection that was up already is tried: a stream is opened on it and
// closed, which the server takes for no request. That fails at once on a
// connection the transport knows to be closed, and then a fresh one is
// dialled within the same limit. A connection that died without a word is
// not found out this way before the transport gives it up.
func (r *Remote) Reachable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.lookupTimeout)
	defer cancel()
	for repeated := false; ; repeated = true {
		c, err := r.begin(ctx)
		if err != nil {
			return err
		}
		if c.fresh {
			r.end(c, nil)
			return nil
		}
		err = c.try(ctx)
		lost := r.end(c, err)
		if err == nil {
			return nil
		}
		if !lost || repeated || ctx.Err() != nil {
			return unreachable(err)
		}
	}
}

// Lookup is client.Lookup within LookupTimeout. client.ErrNotFound for an
// unknown name; ErrUnreachable when the server could not be asked. A
// lookup that fails on a connection that was up is repeated once on a
// fresh one within the same limit.
//
// A refusal of the server is returned as the *wire.Error it sent.
func (r *Remote) Lookup(ctx context.Context, name string) (key.Key, []client.Pack, error) {
	limited, cancel := context.WithTimeout(ctx, r.lookupTimeout)
	defer cancel()
	for repeated := false; ; repeated = true {
		c, err := r.begin(limited)
		if err != nil {
			return key.Key{}, nil, err
		}
		root, packs, err := client.New(c, r.http).Lookup(limited, name)
		if err == nil || answered(err) {
			r.end(c, nil)
			return root, packs, err
		}
		// No answer within the limit retires the connection as a broken
		// stream does. A caller that left says nothing about it.
		var silence error
		if limited.Err() != nil && ctx.Err() == nil {
			silence = fmt.Errorf("no answer to a lookup within %s", r.lookupTimeout)
			c.lost.Store(true)
		}
		lost := r.end(c, errors.Join(silence, err))
		switch {
		case lost && !c.fresh && !repeated && limited.Err() == nil:
			// The connection was up before this call and may have died
			// since it was last used, as it does while the machine
			// sleeps: once more on a fresh one.
			continue
		case lost || limited.Err() != nil:
			return key.Key{}, nil, unreachable(err)
		default:
			// The server answered, with something that is no answer.
			return key.Key{}, nil, err
		}
	}
}

// Fetch is client.Fetch; it has no time limit of its own. It asks nothing
// of the server: the transfers go through the HTTP client.
func (r *Remote) Fetch(ctx context.Context, p client.Pack, each func(packfile.Object) error) (*packfile.Index, error) {
	return r.bucket.Fetch(ctx, p, each)
}

// Push is client.PushFrom; it has no time limit of its own.
//
// It runs from its first request to its last on one connection, the one
// that was in use or could be dialled when it began. Without one it fails
// with ErrUnreachable, and so does a push whose connection broke under it.
func (r *Remote) Push(ctx context.Context, objects client.ObjectSource, name string, root key.Key, opts client.PushOptions) (client.PushResult, error) {
	c, err := r.begin(ctx)
	if err != nil {
		return client.PushResult{}, err
	}
	res, err := client.New(c, r.http).PushFrom(ctx, objects, name, root, opts)
	if lost := r.end(c, err); lost && err != nil && !answered(err) {
		err = unreachable(err)
	}
	return res, err
}

// ForGood reports whether a push failed for a reason a repeat cannot
// cure: the server answered bad_request or malformed_pack.
func ForGood(err error) bool {
	var refusal *wire.Error
	if !errors.As(err, &refusal) {
		return false
	}
	return refusal.Code == wire.CodeBadRequest || refusal.Code == wire.CodeMalformedPack
}

// answered reports whether err is an answer of the server: a name it does
// not know or a request it refuses.
func answered(err error) bool {
	var refusal *wire.Error
	return errors.Is(err, client.ErrNotFound) || errors.As(err, &refusal)
}

// unreachable is err as a failure to ask the server.
func unreachable(err error) error {
	return fmt.Errorf("%w: %w", ErrUnreachable, err)
}

// begin returns a call on the connection in use. Without one it waits for
// the dial under way, or begins one, until ctx is done. Every error it
// returns wraps ErrUnreachable. The call must be given to end.
func (r *Remote) begin(ctx context.Context) (*call, error) {
	fresh := false
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, unreachable(errClosed)
		}
		if l := r.current; l != nil {
			l.calls++
			r.mu.Unlock()
			return &call{link: l, fresh: fresh}, nil
		}
		d := r.dialling
		if d == nil {
			if left := r.failFast - time.Since(r.failedAt); r.failure != nil && left > 0 {
				err := r.failure
				r.mu.Unlock()
				return nil, fmt.Errorf("%w: the last dial failed, the next is not tried for %s: %w",
					ErrUnreachable, left.Round(time.Second), err)
			}
			d = r.startDial()
		}
		r.mu.Unlock()

		select {
		case <-d.done:
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: no connection yet: %w", ErrUnreachable, ctx.Err())
		}
		if d.err != nil {
			return nil, unreachable(d.err)
		}
		// The connection the dial made is the current one, unless a call
		// that got to it first has retired it already.
		fresh = true
	}
}

// startDial begins a dial. The caller holds r.mu and has seen that there
// is neither a connection nor a dial.
func (r *Remote) startDial() *dialling {
	d := &dialling{done: make(chan struct{})}
	r.dialling = d
	go func() {
		ctx, cancel := context.WithTimeout(r.ctx, r.dialTimeout)
		started := time.Now()
		conn, err := r.dial(ctx)
		cancel()
		if err == nil && conn == nil {
			err = errors.New("remote: Dial returned no connection")
		}

		r.mu.Lock()
		r.dialling = nil
		switch {
		case err != nil:
			d.err = fmt.Errorf("dialing the server: %w", err)
			r.failure, r.failedAt = d.err, time.Now()
		case r.closed:
			d.err = errClosed
		default:
			r.links++
			r.current = &link{conn: conn, id: r.links}
			r.failure, r.failedAt = nil, time.Time{}
		}
		l, closed := r.current, r.closed
		r.mu.Unlock()

		switch {
		case err != nil:
			if !closed {
				r.log.Warn("the server cannot be reached", "error", err, "took", time.Since(started).Round(time.Millisecond),
					"next dial after", r.failFast)
			}
		case closed:
			conn.Close()
		default:
			r.log.Info("connected to the server", "connection", l.id, "took", time.Since(started).Round(time.Millisecond))
		}
		close(d.done)
	}()
	return d
}

// end takes c off its connection. The connection is retired if c lost it
// and no call did so before; a retired connection is closed with its last
// call. end reports whether c lost the connection; cause is what the log
// says of it.
func (r *Remote) end(c *call, cause error) (lost bool) {
	lost = c.lost.Load()
	l := c.link

	r.mu.Lock()
	retiring := lost && !l.retired
	if retiring {
		l.retired = true
		if r.current == l {
			r.current = nil
		}
	}
	l.calls--
	last := l.retired && l.calls == 0
	running := l.calls
	r.mu.Unlock()

	if retiring {
		r.log.Warn("the connection to the server is retired", "connection", l.id, "cause", cause, "calls still on it", running)
	}
	if last {
		// Closing shuts an endpoint down, which may take its time, and
		// nobody waits for it.
		go r.closeLink(l)
	}
	return lost
}

func (r *Remote) closeLink(l *link) {
	if err := l.conn.Close(); err != nil {
		r.log.Debug("closing a connection to the server", "connection", l.id, "error", err)
	}
}

// call is one call's use of a connection: what the jaccard-store client
// opens its streams through. It notes whether the connection failed the
// call, which is the one thing that retires a connection.
type call struct {
	link *link
	// fresh: the connection was dialled while this call waited for it.
	fresh bool
	// lost: a stream could not be opened, or a frame not written or read,
	// while the call still wanted it.
	lost atomic.Bool
}

// OpenStreamConn opens a stream on the call's connection.
func (c *call) OpenStreamConn(ctx context.Context) (net.Conn, error) {
	s, err := c.link.conn.OpenStreamConn(ctx)
	if err != nil {
		// A stream that is not opened because its caller left says
		// nothing about the connection.
		if ctx.Err() == nil {
			c.lost.Store(true)
		}
		return nil, err
	}
	return &stream{Conn: s, ctx: ctx, call: c}, nil
}

// try opens a stream and closes it again, to see that the connection
// takes one.
func (c *call) try(ctx context.Context) error {
	s, err := c.OpenStreamConn(ctx)
	if err != nil {
		return fmt.Errorf("opening a stream: %w", err)
	}
	// Nothing comes back on this stream; saying so lets both ends forget it.
	if r, ok := s.(*stream).Conn.(interface{ CancelRead(code uint64) }); ok {
		r.CancelRead(0)
	}
	s.Close()
	return nil
}

// stream is one request's stream. The client writes one frame to it and
// reads one from it, and the stream notes for its call when either fails.
//
// The client closes a stream when the context of its request ends, and
// what the stream returns after that is no failure of the connection. An
// answer that ends early is one, and to know it the stream follows the
// frame as it is read: four bytes of length, then that many bytes.
type stream struct {
	net.Conn
	ctx  context.Context
	call *call

	// header counts the bytes of the frame's length that were read, size
	// is the length as far as it was read, and body counts the bytes read
	// after it.
	header int
	size   uint32
	body   uint64
}

func (s *stream) Write(p []byte) (int, error) {
	n, err := s.Conn.Write(p)
	if err != nil && s.ctx.Err() == nil {
		s.call.lost.Store(true)
	}
	return n, err
}

func (s *stream) Read(p []byte) (int, error) {
	n, err := s.Conn.Read(p)
	read := p[:n]
	for len(read) > 0 && s.header < 4 {
		s.size = s.size<<8 | uint32(read[0])
		s.header++
		read = read[1:]
	}
	s.body += uint64(len(read))
	if err != nil && s.ctx.Err() == nil && !(err == io.EOF && s.whole()) {
		s.call.lost.Store(true)
	}
	return n, err
}

// whole reports whether a frame was read to its end.
func (s *stream) whole() bool {
	return s.header == 4 && s.body >= uint64(s.size)
}

// CloseWrite closes the sending side of the stream, if the stream under
// it can do that: the client asks for it after its request.
func (s *stream) CloseWrite() error {
	if half, ok := s.Conn.(interface{ CloseWrite() error }); ok {
		return half.CloseWrite()
	}
	return nil
}
