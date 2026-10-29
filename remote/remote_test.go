package remote_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/packfile"
	"github.com/amber-store/jaccard-store/wire"

	"github.com/amber-store/narrenhaus/remote"
	"github.com/amber-store/narrenhaus/testserver"
)

// quiet drops what a Remote logs.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// objects is the object source of a push, held in memory.
type objects map[key.Key][]byte

func (o objects) Get(k key.Key) ([]byte, error) {
	b, ok := o[k]
	if !ok {
		return nil, fmt.Errorf("no object %s", k)
	}
	return bytes.Clone(b), nil
}

// blob returns the objects of a small reference, one Blob whose bytes
// follow from seed, and its root.
func blob(t *testing.T, seed string) (key.Key, objects) {
	t.Helper()
	data := bytes.Repeat([]byte(seed+"\n"), 64)
	k, err := key.New(key.Blob, uint64(len(data)), data)
	if err != nil {
		t.Fatalf("the key of a blob: %v", err)
	}
	return k, objects{k: data}
}

// dialer wraps a Dial: it counts the dials and keeps the connections.
type dialer struct {
	dial  func(ctx context.Context) (remote.Conn, error)
	delay time.Duration
	calls atomic.Int32

	mu    sync.Mutex
	conns []*conn
}

func (d *dialer) Dial(ctx context.Context) (remote.Conn, error) {
	d.calls.Add(1)
	if d.delay > 0 {
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c, err := d.dial(ctx)
	if err != nil {
		return nil, err
	}
	w := &conn{Conn: c}
	d.mu.Lock()
	d.conns = append(d.conns, w)
	d.mu.Unlock()
	return w, nil
}

// conn returns the i-th connection dialled.
func (d *dialer) conn(t *testing.T, i int) *conn {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if i >= len(d.conns) {
		t.Fatalf("connection %d was asked for, %d were dialled", i, len(d.conns))
	}
	return d.conns[i]
}

// conn is a connection that tells whether it was closed and that can be
// made mute: the streams opened while it is mute never answer.
type conn struct {
	remote.Conn
	mute   atomic.Bool
	closed atomic.Bool

	mu      sync.Mutex
	silents []*silent
}

func (c *conn) OpenStreamConn(ctx context.Context) (net.Conn, error) {
	s, err := c.Conn.OpenStreamConn(ctx)
	if err != nil || !c.mute.Load() {
		return s, err
	}
	mute := &silent{Conn: s, done: make(chan struct{}), broken: make(chan struct{})}
	c.mu.Lock()
	c.silents = append(c.silents, mute)
	c.mu.Unlock()
	return mute, nil
}

func (c *conn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// silent returns the i-th stream that was opened while c was mute, once
// there is one.
func (c *conn) silent(t *testing.T, i int) *silent {
	t.Helper()
	var s *silent
	eventually(t, fmt.Sprintf("stream %d is opened on the mute connection", i), func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if i < len(c.silents) {
			s = c.silents[i]
		}
		return s != nil
	})
	return s
}

// silent is a stream whose peer says nothing: what is written goes
// nowhere, and a read lasts until the stream is closed or breaks.
type silent struct {
	net.Conn
	done   chan struct{}
	broken chan struct{}
	once   sync.Once
}

func (s *silent) Write(p []byte) (int, error) { return len(p), nil }

func (s *silent) Read([]byte) (int, error) {
	select {
	case <-s.done:
		return 0, net.ErrClosed
	case <-s.broken:
		return 0, errors.New("the stream was reset")
	}
}

func (s *silent) Close() error {
	s.once.Do(func() { close(s.done) })
	return s.Conn.Close()
}

// reset makes the read under way on s fail, as a connection that died does.
func (s *silent) reset() { close(s.broken) }

// newRemote returns a Remote that is closed when the test ends.
func newRemote(t *testing.T, cfg remote.Config) *remote.Remote {
	t.Helper()
	if cfg.Log == nil {
		cfg.Log = quiet
	}
	r := remote.New(cfg)
	t.Cleanup(r.Close)
	return r
}

// watched returns a Remote for srv and the dialer under it.
func watched(t *testing.T, srv *testserver.Server, cfg remote.Config) (*remote.Remote, *dialer) {
	t.Helper()
	d := &dialer{dial: srv.Dial()}
	cfg.Dial = d.Dial
	return newRemote(t, cfg), d
}

// push pushes a small reference under name and returns its root.
func push(t *testing.T, r *remote.Remote, name, seed string) key.Key {
	t.Helper()
	root, objs := blob(t, seed)
	res, err := r.Push(context.Background(), objs, name, root, pushOptions(t))
	if err != nil {
		t.Fatalf("pushing %q: %v", name, err)
	}
	if res.Root != root {
		t.Fatalf("pushing %q: the result names root %s, want %s", name, res.Root, root)
	}
	return root
}

func pushOptions(t *testing.T) client.PushOptions {
	return client.PushOptions{MinDedup: 0.5, TempDir: t.TempDir()}
}

// eventually waits for cond, at most five seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

// arrivals makes the bucket of srv tell of every request with method that
// reaches it.
func arrivals(srv *testserver.Server, method string) <-chan string {
	seen := make(chan string, 64)
	srv.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == method {
			select {
			case seen <- r.URL.Path:
			default:
			}
		}
		return false
	})
	return seen
}

func waitFor[T any](t *testing.T, what string, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("never happened: %s", what)
		panic("unreachable")
	}
}

func TestLookupNotFound(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()

	for range 2 {
		_, packs, err := r.Lookup(ctx, "acme/absent")
		if !errors.Is(err, client.ErrNotFound) {
			t.Fatalf("Lookup of an unknown name: %v, want client.ErrNotFound", err)
		}
		if errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("an unknown name counts as an unreachable server: %v", err)
		}
		if packs != nil {
			t.Fatalf("an unknown name has packs: %v", packs)
		}
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: an unknown name must not retire the connection", n)
	}
}

func TestLookupFindsWhatWasPushed(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()

	want := push(t, r, "acme/one", "one")
	root, packs, err := r.Lookup(ctx, "acme/one")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if root != want || len(packs) != 1 || packs[0].Root != want || packs[0].Objects != 1 {
		t.Fatalf("Lookup: root %s, packs %+v; want root %s in one pack of one object", root, packs, want)
	}

	// The same reference through a plain client of the server.
	got, _, err := srv.Client(t).Lookup(ctx, "acme/one")
	if err != nil || got != want {
		t.Fatalf("the server's client: root %s, %v; want %s", got, err, want)
	}

	var seen []packfile.Object
	index, err := r.Fetch(ctx, packs[0], func(o packfile.Object) error {
		seen = append(seen, packfile.Object{Key: o.Key, Data: bytes.Clone(o.Data)})
		return nil
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	_, objs := blob(t, "one")
	if index.Len() != 1 || len(seen) != 1 || seen[0].Key != want || !bytes.Equal(seen[0].Data, objs[want]) {
		t.Fatalf("Fetch handed over %d objects of an index of %d, not the blob that was pushed", len(seen), index.Len())
	}

	// A failure of each is the failure of the fetch, as it is.
	sentinel := errors.New("no room")
	if _, err := r.Fetch(ctx, packs[0], func(packfile.Object) error { return sentinel }); err != sentinel {
		t.Fatalf("Fetch with a failing each: %v, want the error of each as it is", err)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1", n)
	}
}

func TestLookupAfterTheConnectionDied(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()

	want := push(t, r, "acme/one", "one")
	if _, _, err := r.Lookup(ctx, "acme/one"); err != nil {
		t.Fatalf("Lookup before the connection died: %v", err)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials before the connection died, want 1", n)
	}

	srv.DropConnections()

	root, _, err := r.Lookup(ctx, "acme/one")
	if err != nil {
		t.Fatalf("Lookup after the connection died: %v", err)
	}
	if root != want {
		t.Fatalf("Lookup after the connection died: root %s, want %s", root, want)
	}
	if n := d.calls.Load(); n != 2 {
		t.Fatalf("%d dials, want 2: one redial", n)
	}
	// The dead connection has no call left and is closed.
	old := d.conn(t, 0)
	eventually(t, "the dead connection is closed", old.closed.Load)
	if d.conn(t, 1).closed.Load() {
		t.Fatal("the fresh connection was closed")
	}
}

func TestLookupWhenTheServerIsDown(t *testing.T) {
	t.Run("the dial fails", func(t *testing.T) {
		const lookupTimeout = 500 * time.Millisecond
		srv := testserver.Start(t, testserver.Options{})
		r, d := watched(t, srv, remote.Config{LookupTimeout: lookupTimeout, DialTimeout: 300 * time.Millisecond})
		ctx := context.Background()
		push(t, r, "acme/one", "one")

		srv.Stop()

		started := time.Now()
		_, _, err := r.Lookup(ctx, "acme/one")
		took := time.Since(started)
		if !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("Lookup with the server down: %v, want ErrUnreachable", err)
		}
		if errors.Is(err, client.ErrNotFound) {
			t.Fatalf("a server that is down counts as an unknown name: %v", err)
		}
		if took > lookupTimeout+250*time.Millisecond {
			t.Fatalf("Lookup with the server down took %v, the limit is %v", took, lookupTimeout)
		}

		// Inside the window after the failed dial nothing is dialled.
		started = time.Now()
		_, _, err = r.Lookup(ctx, "acme/one")
		took = time.Since(started)
		if !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("second Lookup with the server down: %v, want ErrUnreachable", err)
		}
		if took > 50*time.Millisecond {
			t.Fatalf("second Lookup took %v, want under 50ms", took)
		}
		if n := d.calls.Load(); n != 2 {
			t.Fatalf("%d dials, want 2: the first connection and the one that failed", n)
		}
	})

	t.Run("the dial outlasts the lookup", func(t *testing.T) {
		const lookupTimeout = 200 * time.Millisecond
		var dials atomic.Int32
		ended := make(chan error, 1)
		r := newRemote(t, remote.Config{
			LookupTimeout: lookupTimeout,
			DialTimeout:   time.Minute,
			Dial: func(ctx context.Context) (remote.Conn, error) {
				dials.Add(1)
				<-ctx.Done()
				ended <- ctx.Err()
				return nil, ctx.Err()
			},
		})
		for i := range 2 {
			started := time.Now()
			_, _, err := r.Lookup(context.Background(), "acme/one")
			took := time.Since(started)
			if !errors.Is(err, remote.ErrUnreachable) {
				t.Fatalf("Lookup %d: %v, want ErrUnreachable", i, err)
			}
			if took < lookupTimeout || took > lookupTimeout+250*time.Millisecond {
				t.Fatalf("Lookup %d took %v, the limit is %v", i, took, lookupTimeout)
			}
		}
		// The dial was not given up with the lookups, and not begun twice.
		if n := dials.Load(); n != 1 {
			t.Fatalf("%d dials, want 1", n)
		}
		select {
		case err := <-ended:
			t.Fatalf("the dial ended with its callers: %v", err)
		default:
		}
		// It runs under the Remote's context, which Close ends.
		r.Close()
		if err := waitFor(t, "the dial ends when the Remote is closed", ended); !errors.Is(err, context.Canceled) {
			t.Fatalf("the dial ended with %v, want context.Canceled", err)
		}
	})

	t.Run("the dial has a limit", func(t *testing.T) {
		ended := make(chan time.Duration, 1)
		r := newRemote(t, remote.Config{
			LookupTimeout: 2 * time.Second,
			DialTimeout:   100 * time.Millisecond,
			Dial: func(ctx context.Context) (remote.Conn, error) {
				started := time.Now()
				<-ctx.Done()
				ended <- time.Since(started)
				return nil, ctx.Err()
			},
		})
		started := time.Now()
		_, _, err := r.Lookup(context.Background(), "acme/one")
		if !errors.Is(err, remote.ErrUnreachable) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Lookup: %v, want ErrUnreachable with the dial's context.DeadlineExceeded", err)
		}
		if took := time.Since(started); took > time.Second {
			t.Fatalf("Lookup took %v: it did not end with the dial, which has 100ms", took)
		}
		if took := waitFor(t, "the dial ends", ended); took < 100*time.Millisecond || took > 500*time.Millisecond {
			t.Fatalf("the dial was given %v, want 100ms", took)
		}
	})
}

func TestRecoversAfterFailFast(t *testing.T) {
	const failFast = 200 * time.Millisecond
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{
		LookupTimeout: 2 * time.Second,
		DialTimeout:   300 * time.Millisecond,
		FailFast:      failFast,
	})
	ctx := context.Background()
	want := push(t, r, "acme/one", "one")

	srv.Stop()
	if _, _, err := r.Lookup(ctx, "acme/one"); !errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("Lookup with the server down: %v, want ErrUnreachable", err)
	}
	if n := d.calls.Load(); n != 2 {
		t.Fatalf("%d dials with the server down, want 2", n)
	}

	srv.Restart()
	time.Sleep(failFast + 50*time.Millisecond)

	root, _, err := r.Lookup(ctx, "acme/one")
	if err != nil {
		t.Fatalf("Lookup after the restart and the window: %v", err)
	}
	if root != want {
		t.Fatalf("Lookup after the restart: root %s, want %s: the server lost its data", root, want)
	}
	if n := d.calls.Load(); n != 3 {
		t.Fatalf("%d dials, want 3", n)
	}
}

func TestRefusalDoesNotRetire(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()
	if err := r.Reachable(ctx); err != nil {
		t.Fatalf("Reachable: %v", err)
	}

	// A push that is under way, held up in the bucket.
	puts := arrivals(srv, http.MethodPut)
	srv.SlowBucket(300 * time.Millisecond)
	type result struct {
		res client.PushResult
		err error
	}
	slowRoot, slowObjects := blob(t, "slow")
	slow := make(chan result, 1)
	go func() {
		res, err := r.Push(ctx, slowObjects, "acme/slow", slowRoot, pushOptions(t))
		slow <- result{res, err}
	}()
	waitFor(t, "the slow push reaches the bucket", puts)

	// A push the server refuses: no reference name holds an '@'.
	root, objs := blob(t, "refused")
	_, err := r.Push(ctx, objs, "acme/re@fused", root, pushOptions(t))
	var refusal *wire.Error
	if !errors.As(err, &refusal) || refusal.Code != wire.CodeBadRequest {
		t.Fatalf("Push of a name with '@': %v, want the server's bad_request", err)
	}
	if !remote.ForGood(err) {
		t.Fatalf("ForGood(%v) = false, want true", err)
	}
	if errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("a refusal counts as an unreachable server: %v", err)
	}
	select {
	case got := <-slow:
		t.Fatalf("the slow push ended before the refusal was seen: %+v", got)
	default:
	}

	got := waitFor(t, "the slow push ends", slow)
	if got.err != nil {
		t.Fatalf("the slow push failed after another push was refused: %v", got.err)
	}
	if got.res.Root != slowRoot || got.res.Stored {
		t.Fatalf("the slow push: %+v, want an upload of %s", got.res, slowRoot)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: a refusal must not retire the connection", n)
	}
	if d.conn(t, 0).closed.Load() {
		t.Fatal("the connection was closed after a refusal")
	}
	srv.SlowBucket(0)
	if root, _, err := r.Lookup(ctx, "acme/slow"); err != nil || root != slowRoot {
		t.Fatalf("Lookup of what the slow push wrote: root %s, %v", root, err)
	}
}

func TestRetiredConnectionOutlivesItsCalls(t *testing.T) {
	const lookupTimeout = 200 * time.Millisecond
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{LookupTimeout: lookupTimeout})
	ctx := context.Background()
	if err := r.Reachable(ctx); err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	first := d.conn(t, 0)

	// A push in flight: it has asked the server where to upload and is
	// held up in the bucket for longer than a lookup may take.
	puts := arrivals(srv, http.MethodPut)
	srv.SlowBucket(time.Second)
	root, objs := blob(t, "slow")
	pushed := make(chan error, 1)
	go func() {
		_, err := r.Push(ctx, objs, "acme/slow", root, pushOptions(t))
		pushed <- err
	}()
	waitFor(t, "the push reaches the bucket", puts)

	// A lookup on the same connection that gets no answer.
	first.mute.Store(true)
	started := time.Now()
	_, _, err := r.Lookup(ctx, "acme/slow")
	took := time.Since(started)
	first.mute.Store(false)
	if !errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("Lookup without an answer: %v, want ErrUnreachable", err)
	}
	if took < lookupTimeout || took > lookupTimeout+250*time.Millisecond {
		t.Fatalf("Lookup without an answer took %v, the limit is %v", took, lookupTimeout)
	}

	// The connection is retired, and it is not closed under the push.
	if first.closed.Load() {
		t.Fatal("the connection was closed with a push running on it")
	}
	select {
	case err := <-pushed:
		t.Fatalf("the push ended before the lookup timed out: %v", err)
	default:
	}
	if err := r.Reachable(ctx); err != nil {
		t.Fatalf("Reachable after the lookup timed out: %v", err)
	}
	if n := d.calls.Load(); n != 2 {
		t.Fatalf("%d dials, want 2: the retired connection takes no new call", n)
	}
	if first.closed.Load() {
		t.Fatal("the retired connection was closed with a push running on it")
	}

	if err := waitFor(t, "the push ends", pushed); err != nil {
		t.Fatalf("the push on the retired connection failed: %v", err)
	}
	eventually(t, "the retired connection is closed after its last call", first.closed.Load)
	if d.conn(t, 1).closed.Load() {
		t.Fatal("the connection in use was closed")
	}
	srv.SlowBucket(0)
	if got, _, err := r.Lookup(ctx, "acme/slow"); err != nil || got != root {
		t.Fatalf("Lookup of what the push wrote: root %s, %v", got, err)
	}
}

func TestOnlyTheFirstFailureRetires(t *testing.T) {
	t.Run("a late failure on the old connection", func(t *testing.T) {
		srv := testserver.Start(t, testserver.Options{})
		r, d := watched(t, srv, remote.Config{})
		ctx := context.Background()
		if err := r.Reachable(ctx); err != nil {
			t.Fatalf("Reachable: %v", err)
		}
		first := d.conn(t, 0)

		// Two pushes wait for the server's first answer on one connection.
		first.mute.Store(true)
		failed := make(chan error, 2)
		for _, name := range []string{"acme/a", "acme/b"} {
			root, objs := blob(t, name)
			go func() {
				_, err := r.Push(ctx, objs, name, root, pushOptions(t))
				failed <- err
			}()
		}
		one, two := first.silent(t, 0), first.silent(t, 1)
		first.mute.Store(false)

		// The first of them loses its stream: the connection is retired,
		// and it stays open for the other.
		one.reset()
		if err := waitFor(t, "the first push fails", failed); !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("a push whose stream broke: %v, want ErrUnreachable", err)
		}
		if first.closed.Load() {
			t.Fatal("the connection was closed with a push waiting on it")
		}
		if err := r.Reachable(ctx); err != nil {
			t.Fatalf("Reachable after the first failure: %v", err)
		}
		if n := d.calls.Load(); n != 2 {
			t.Fatalf("%d dials after the first failure, want 2", n)
		}

		// The second fails on the connection that is retired already. The
		// one dialled since is not its business.
		two.reset()
		if err := waitFor(t, "the second push fails", failed); !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("a push whose stream broke: %v, want ErrUnreachable", err)
		}
		eventually(t, "the retired connection is closed", first.closed.Load)
		for range 3 {
			if _, _, err := r.Lookup(ctx, "acme/absent"); !errors.Is(err, client.ErrNotFound) {
				t.Fatalf("Lookup on the fresh connection: %v, want client.ErrNotFound", err)
			}
		}
		if n := d.calls.Load(); n != 2 {
			t.Fatalf("%d dials, want 2: the late failure retired the fresh connection", n)
		}
		if d.conn(t, 1).closed.Load() {
			t.Fatal("the fresh connection was closed")
		}
	})

	t.Run("lookups that time out together", func(t *testing.T) {
		srv := testserver.Start(t, testserver.Options{})
		r, d := watched(t, srv, remote.Config{LookupTimeout: 300 * time.Millisecond})
		ctx := context.Background()
		if err := r.Reachable(ctx); err != nil {
			t.Fatalf("Reachable: %v", err)
		}

		d.conn(t, 0).mute.Store(true)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				if _, _, err := r.Lookup(ctx, "acme/absent"); !errors.Is(err, remote.ErrUnreachable) {
					t.Errorf("Lookup without an answer: %v, want ErrUnreachable", err)
				}
			})
		}
		wg.Wait()
		for range 3 {
			if _, _, err := r.Lookup(ctx, "acme/absent"); !errors.Is(err, client.ErrNotFound) {
				t.Fatalf("Lookup on the fresh connection: %v, want client.ErrNotFound", err)
			}
		}
		if n := d.calls.Load(); n != 2 {
			t.Fatalf("%d dials, want 2", n)
		}
		eventually(t, "the retired connection is closed", d.conn(t, 0).closed.Load)
	})
}

func TestBucketFailureDoesNotRetire(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()
	want := push(t, r, "acme/one", "one")
	_, packs, err := r.Lookup(ctx, "acme/one")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	nothing := func(packfile.Object) error {
		t.Error("an object was handed over")
		return nil
	}

	// The bucket refuses, as it does when a URL has expired.
	srv.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		http.Error(w, "expired", http.StatusForbidden)
		return true
	})
	root, objs := blob(t, "two")
	_, err = r.Push(ctx, objs, "acme/two", root, pushOptions(t))
	if err == nil || client.StatusCode(err) != http.StatusForbidden {
		t.Fatalf("Push with a bucket that refuses: %v, want the bucket's 403", err)
	}
	if remote.ForGood(err) || errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("a refusal of the bucket counts as the server's, or as no server: %v", err)
	}
	index, err := r.Fetch(ctx, packs[0], nothing)
	if index != nil || client.StatusCode(err) != http.StatusForbidden || errors.Is(err, client.ErrBadPack) {
		t.Fatalf("Fetch with a bucket that refuses: index %v, %v; want the bucket's 403 and no index", index, err)
	}

	// The bucket holds something else than the server announced.
	srv.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		io.WriteString(w, "not an index")
		return true
	})
	index, err = r.Fetch(ctx, packs[0], nothing)
	if index != nil || !errors.Is(err, client.ErrBadPack) || client.StatusCode(err) != 0 {
		t.Fatalf("Fetch of an index that is none: index %v, %v; want client.ErrBadPack and no index", index, err)
	}

	// None of it was the connection's fault.
	srv.Intercept(nil)
	if got, _, err := r.Lookup(ctx, "acme/one"); err != nil || got != want {
		t.Fatalf("Lookup after the bucket's failures: root %s, %v", got, err)
	}
	if _, _, err := r.Lookup(ctx, "acme/two"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Lookup of the reference whose upload was refused: %v, want client.ErrNotFound", err)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: a bucket failure must not retire the connection", n)
	}
	if d.conn(t, 0).closed.Load() {
		t.Fatal("the connection was closed after a bucket failure")
	}
}

func TestACallerThatLeavesDoesNotRetire(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	if err := r.Reachable(context.Background()); err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	first := d.conn(t, 0)

	// A lookup and a push wait for an answer when their callers give up.
	first.mute.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	failed := make(chan error, 2)
	go func() {
		_, _, err := r.Lookup(ctx, "acme/one")
		failed <- err
	}()
	go func() {
		root, objs := blob(t, "one")
		_, err := r.Push(ctx, objs, "acme/one", root, pushOptions(t))
		failed <- err
	}()
	first.silent(t, 1)
	first.mute.Store(false)
	cancel()
	for range 2 {
		if err := waitFor(t, "a call ends when its caller leaves", failed); !errors.Is(err, context.Canceled) {
			t.Fatalf("a call whose caller left: %v, want context.Canceled", err)
		}
	}

	if _, _, err := r.Lookup(context.Background(), "acme/one"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Lookup after two callers left: %v, want client.ErrNotFound", err)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: a caller that leaves must not retire the connection", n)
	}
	if first.closed.Load() {
		t.Fatal("the connection was closed after a caller left")
	}
}

// canned is a connection whose every stream reads as the same bytes and
// then ends.
type canned struct {
	answer []byte
	// atOnce: the end of the stream comes with its last bytes, not after.
	atOnce bool
}

func (c *canned) OpenStreamConn(context.Context) (net.Conn, error) {
	return &cannedStream{left: c.answer, atOnce: c.atOnce}, nil
}

func (c *canned) Close() error { return nil }

type cannedStream struct {
	net.Conn
	left   []byte
	atOnce bool
}

func (s *cannedStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *cannedStream) Close() error                { return nil }

func (s *cannedStream) Read(p []byte) (int, error) {
	n := copy(p, s.left)
	s.left = s.left[n:]
	if len(s.left) == 0 && (n == 0 || s.atOnce) {
		return n, io.EOF
	}
	return n, nil
}

func TestAnAnswerThatEndsEarlyRetires(t *testing.T) {
	var frame bytes.Buffer
	if err := wire.WriteFrame(&frame, wire.Errorf(wire.CodeNotFound, "no such reference")); err != nil {
		t.Fatal(err)
	}
	whole := frame.Bytes()

	for _, c := range []struct {
		name   string
		answer []byte
		atOnce bool
		// lost: the answer is none, and the connection is retired for it.
		lost bool
	}{
		{name: "a whole answer", answer: whole},
		{name: "a whole answer that ends with its last byte", answer: whole, atOnce: true},
		{name: "no answer", answer: nil, lost: true},
		{name: "half a length", answer: whole[:2], lost: true},
		{name: "a length and no more", answer: whole[:4], atOnce: true, lost: true},
		{name: "an answer cut short", answer: whole[:len(whole)-1], atOnce: true, lost: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var dials atomic.Int32
			r := newRemote(t, remote.Config{Dial: func(context.Context) (remote.Conn, error) {
				dials.Add(1)
				return &canned{answer: c.answer, atOnce: c.atOnce}, nil
			}})
			ctx := context.Background()
			if err := r.Reachable(ctx); err != nil {
				t.Fatalf("Reachable: %v", err)
			}
			_, _, err := r.Lookup(ctx, "acme/one")
			if !c.lost {
				if !errors.Is(err, client.ErrNotFound) {
					t.Fatalf("Lookup: %v, want client.ErrNotFound", err)
				}
				if n := dials.Load(); n != 1 {
					t.Fatalf("%d dials, want 1: an answer read to its end must not retire the connection", n)
				}
				return
			}
			if !errors.Is(err, remote.ErrUnreachable) {
				t.Fatalf("Lookup: %v, want ErrUnreachable", err)
			}
			// The connection was up, so the lookup was repeated on a
			// fresh one, once.
			if n := dials.Load(); n != 2 {
				t.Fatalf("%d dials, want 2", n)
			}
		})
	}
}

func TestDialsAreSerialised(t *testing.T) {
	t.Run("one dial serves every caller", func(t *testing.T) {
		srv := testserver.Start(t, testserver.Options{})
		d := &dialer{dial: srv.Dial(), delay: 100 * time.Millisecond}
		r := newRemote(t, remote.Config{Dial: d.Dial})

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				if _, _, err := r.Lookup(context.Background(), "acme/absent"); !errors.Is(err, client.ErrNotFound) {
					t.Errorf("Lookup: %v, want client.ErrNotFound", err)
				}
			})
		}
		wg.Wait()
		if n := d.calls.Load(); n != 1 {
			t.Fatalf("Dial was called %d times by 20 concurrent lookups, want once", n)
		}
	})

	t.Run("one failure fails every caller", func(t *testing.T) {
		refused := errors.New("nobody home")
		var dials atomic.Int32
		r := newRemote(t, remote.Config{Dial: func(ctx context.Context) (remote.Conn, error) {
			dials.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil, refused
		}})

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				_, _, err := r.Lookup(context.Background(), "acme/absent")
				if !errors.Is(err, remote.ErrUnreachable) || !errors.Is(err, refused) {
					t.Errorf("Lookup: %v, want ErrUnreachable with the dial's failure", err)
				}
			})
		}
		wg.Wait()
		if n := dials.Load(); n != 1 {
			t.Fatalf("Dial was called %d times, want once", n)
		}
	})
}

func TestReachable(t *testing.T) {
	t.Run("up, dropped, down", func(t *testing.T) {
		srv := testserver.Start(t, testserver.Options{})
		r, d := watched(t, srv, remote.Config{LookupTimeout: 2 * time.Second, DialTimeout: 300 * time.Millisecond})
		ctx := context.Background()

		for range 2 {
			if err := r.Reachable(ctx); err != nil {
				t.Fatalf("Reachable with the server up: %v", err)
			}
		}
		if n := d.calls.Load(); n != 1 {
			t.Fatalf("%d dials, want 1", n)
		}

		// A connection the server closed is not one that is up: a fresh
		// one is dialled.
		srv.DropConnections()
		eventually(t, "a fresh connection is dialled", func() bool {
			if err := r.Reachable(ctx); err != nil {
				t.Fatalf("Reachable after the connection was dropped: %v", err)
			}
			return d.calls.Load() == 2
		})

		srv.Stop()
		var err error
		eventually(t, "the server is found to be down", func() bool {
			err = r.Reachable(ctx)
			return err != nil
		})
		if !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("Reachable with the server down: %v, want ErrUnreachable", err)
		}
		started := time.Now()
		if err := r.Reachable(ctx); !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("Reachable after a failed dial: %v, want ErrUnreachable", err)
		}
		if took := time.Since(started); took > 50*time.Millisecond {
			t.Fatalf("Reachable after a failed dial took %v, want under 50ms", took)
		}
	})

	t.Run("no dial within the limit", func(t *testing.T) {
		const lookupTimeout = 200 * time.Millisecond
		r := newRemote(t, remote.Config{
			LookupTimeout: lookupTimeout,
			Dial: func(ctx context.Context) (remote.Conn, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})
		started := time.Now()
		err := r.Reachable(context.Background())
		took := time.Since(started)
		if !errors.Is(err, remote.ErrUnreachable) {
			t.Fatalf("Reachable: %v, want ErrUnreachable", err)
		}
		if took < lookupTimeout || took > lookupTimeout+250*time.Millisecond {
			t.Fatalf("Reachable took %v, the limit is %v", took, lookupTimeout)
		}
	})

	t.Run("the caller gives up", func(t *testing.T) {
		r := newRemote(t, remote.Config{Dial: func(ctx context.Context) (remote.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}})
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		started := time.Now()
		err := r.Reachable(ctx)
		if !errors.Is(err, remote.ErrUnreachable) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Reachable: %v, want ErrUnreachable with context.Canceled", err)
		}
		if took := time.Since(started); took > time.Second {
			t.Fatalf("Reachable took %v after its caller gave up at 50ms", took)
		}
	})
}

func TestPushWhenTheServerIsDown(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, _ := watched(t, srv, remote.Config{DialTimeout: 300 * time.Millisecond})
	srv.Stop()

	root, objs := blob(t, "one")
	started := time.Now()
	_, err := r.Push(context.Background(), objs, "acme/one", root, pushOptions(t))
	if !errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("Push with the server down: %v, want ErrUnreachable", err)
	}
	if remote.ForGood(err) {
		t.Fatalf("ForGood(%v) = true: a server that is down is no failure for good", err)
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Fatalf("Push with the server down took %v, the dial has 300ms", took)
	}
}

func TestPushOverTheServersLimit(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{MaxPackBytes: 16})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()

	root, objs := blob(t, "too large")
	_, err := r.Push(ctx, objs, "acme/large", root, pushOptions(t))
	if !remote.ForGood(err) {
		t.Fatalf("Push of a pack over the server's limit: %v, want a failure for good", err)
	}
	if _, _, err := r.Lookup(ctx, "acme/large"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Lookup of the refused reference: %v, want client.ErrNotFound", err)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1", n)
	}
}

func TestForGood(t *testing.T) {
	refusal := func(code string) error {
		return fmt.Errorf("push-commit: %w", &wire.Error{Code: code, Message: "said the server"})
	}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"no error", nil, false},
		{"bad_request", refusal(wire.CodeBadRequest), true},
		{"malformed_pack", refusal(wire.CodeMalformedPack), true},
		{"internal", refusal(wire.CodeInternal), false},
		{"unknown_upload", refusal(wire.CodeUnknownUpload), false},
		{"parent_gone", refusal(wire.CodeParentGone), false},
		{"not_found", refusal(wire.CodeNotFound), false},
		{"unreachable", fmt.Errorf("%w: no route", remote.ErrUnreachable), false},
		{"a bucket error", errors.New("data: the bucket answered 500 Internal Server Error"), false},
		{"cancelled", context.Canceled, false},
	} {
		if got := remote.ForGood(c.err); got != c.want {
			t.Errorf("ForGood(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestClose(t *testing.T) {
	srv := testserver.Start(t, testserver.Options{})
	r, d := watched(t, srv, remote.Config{})
	ctx := context.Background()
	if err := r.Reachable(ctx); err != nil {
		t.Fatalf("Reachable: %v", err)
	}

	r.Close()
	r.Close()
	if !d.conn(t, 0).closed.Load() {
		t.Fatal("Close left the connection open")
	}
	started := time.Now()
	if _, _, err := r.Lookup(ctx, "acme/one"); !errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("Lookup after Close: %v, want ErrUnreachable", err)
	}
	if err := r.Reachable(ctx); !errors.Is(err, remote.ErrUnreachable) {
		t.Fatalf("Reachable after Close: %v, want ErrUnreachable", err)
	}
	if took := time.Since(started); took > 50*time.Millisecond {
		t.Fatalf("calls after Close took %v", took)
	}
	if n := d.calls.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: nothing is dialled after Close", n)
	}
}

func TestStalledTransferFails(t *testing.T) {
	const limit = 300 * time.Millisecond

	// fetch pushes a reference, puts stall in front of the bucket and
	// fetches the reference's pack with the default HTTP client.
	fetch := func(t *testing.T, stall func(w http.ResponseWriter, r *http.Request) bool) (time.Duration, error) {
		remote.SetStallLimit(t, limit)
		srv := testserver.Start(t, testserver.Options{})
		r, _ := watched(t, srv, remote.Config{})
		ctx := context.Background()
		push(t, r, "acme/one", "one")
		_, packs, err := r.Lookup(ctx, "acme/one")
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		srv.Intercept(stall)
		started := time.Now()
		index, err := r.Fetch(ctx, packs[0], func(packfile.Object) error {
			t.Error("an object was handed over")
			return nil
		})
		took := time.Since(started)
		if err != nil && index != nil {
			t.Error("Fetch failed and returned an index")
		}
		return took, err
	}
	check := func(t *testing.T, took time.Duration, err error) {
		if err == nil {
			t.Fatal("Fetch of a stalled transfer succeeded")
		}
		if !strings.Contains(err.Error(), "no byte for") {
			t.Fatalf("Fetch of a stalled transfer: %v, want an error that names the stall", err)
		}
		if errors.Is(err, client.ErrBadPack) || client.StatusCode(err) != 0 {
			t.Fatalf("a stalled transfer counts as a bad pack or a refusal: %v", err)
		}
		if took < limit || took > limit+time.Second {
			t.Fatalf("Fetch of a stalled transfer took %v, the stall limit is %v", took, limit)
		}
	}

	t.Run("headers and then nothing", func(t *testing.T) {
		took, err := fetch(t, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, ".data") {
				return false
			}
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return true
		})
		check(t, took, err)
	})

	t.Run("a body that stops", func(t *testing.T) {
		took, err := fetch(t, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, ".idx") {
				return false
			}
			w.Header().Set("Content-Length", "1000")
			w.Write([]byte("only the beginning"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return true
		})
		check(t, took, err)
	})

	t.Run("no answer", func(t *testing.T) {
		took, err := fetch(t, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet {
				return false
			}
			<-r.Context().Done()
			return true
		})
		check(t, took, err)
	})
}

func TestStalledUploadFails(t *testing.T) {
	const limit = 300 * time.Millisecond
	release := make(chan struct{})
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Nothing of the body is read.
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer bucket.Close()
	defer close(release)

	// More than the sockets between the two take in.
	const size = 256 << 20
	req, err := http.NewRequest(http.MethodPut, bucket.URL, io.LimitReader(zeros{}, size))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = size
	started := time.Now()
	res, err := remote.NewHTTPClient(limit).Do(req)
	took := time.Since(started)
	if err == nil {
		res.Body.Close()
		t.Fatal("an upload nobody reads succeeded")
	}
	if !strings.Contains(err.Error(), "no byte for") {
		t.Fatalf("an upload nobody reads: %v, want an error that names the stall", err)
	}
	if took < limit || took > limit+2*time.Second {
		t.Fatalf("an upload nobody reads took %v, the stall limit is %v", took, limit)
	}
}

// zeros reads as zero bytes without end.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestSlowTransferIsNotStalled(t *testing.T) {
	const (
		limit = 300 * time.Millisecond
		pause = 100 * time.Millisecond
		parts = 8
	)
	part := bytes.Repeat([]byte("x"), 1000)
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			// The upload is read as slowly as the download is written.
			buf := make([]byte, len(part))
			for {
				if _, err := io.ReadFull(r.Body, buf); err != nil {
					return
				}
				time.Sleep(pause)
			}
		}
		w.Header().Set("Content-Length", fmt.Sprint(parts*len(part)))
		for range parts {
			w.Write(part)
			w.(http.Flusher).Flush()
			time.Sleep(pause)
		}
	}))
	defer bucket.Close()
	hc := remote.NewHTTPClient(limit)

	// A download that takes longer than the limit and never rests that long.
	started := time.Now()
	res, err := hc.Get(bucket.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || len(body) != parts*len(part) {
		t.Fatalf("a slow download: %d bytes, %v; want %d bytes", len(body), err, parts*len(part))
	}
	if took := time.Since(started); took < 2*limit {
		t.Fatalf("the download took %v, too short to show anything", took)
	}

	// A reader that rests between its reads does not make a transfer stall.
	res, err = hc.Get(bucket.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	buf := make([]byte, 100)
	if _, err := io.ReadFull(res.Body, buf); err != nil {
		t.Fatalf("reading the beginning: %v", err)
	}
	time.Sleep(2 * limit)
	rest, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || len(buf)+len(rest) != parts*len(part) {
		t.Fatalf("a download read with a rest: %d bytes, %v; want %d bytes", len(buf)+len(rest), err, parts*len(part))
	}

	// An upload the bucket takes slowly, written by a slow source.
	req, err := http.NewRequest(http.MethodPut, bucket.URL, &dripping{left: parts, part: part, pause: pause})
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(parts * len(part))
	res, err = hc.Do(req)
	if err != nil {
		t.Fatalf("a slow upload: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("a slow upload: %s", res.Status)
	}
}

// dripping is a request body that hands out a part at a time, with a pause
// before each.
type dripping struct {
	left  int
	part  []byte
	pause time.Duration
}

func (d *dripping) Read(p []byte) (int, error) {
	if d.left == 0 {
		return 0, io.EOF
	}
	time.Sleep(d.pause)
	d.left--
	return copy(p, d.part), nil
}
