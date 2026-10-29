// Package testserver is a jaccard-store server in one process, for the
// tests of the packages that talk to one: the server with a database in a
// temporary directory and a bucket in memory, reachable over iroh on
// loopback. It is a package and no test file so that the tests of several
// packages can import it.
//
// The server can be stopped and restarted and its connections dropped or
// forgotten, and the bucket can be made slow or be answered for, so that a
// test can show what its package does when the server or the bucket lets
// it down.
//
// The bucket (jaccard-store's buckettest) sets the environment of the test
// with t.Setenv, so a test that calls Start cannot be parallel.
package testserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amber-store/jaccard-store/bucket"
	"github.com/amber-store/jaccard-store/bucket/buckettest"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"

	"github.com/amber-store/narrenhaus/remote"
)

// requestTimeout bounds the wait for a request frame on a stream, as the
// server's own serving does.
const requestTimeout = 30 * time.Second

// quiet drops what the server and the endpoints log.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// Options configure the server.
type Options struct {
	// NoVerify makes the server record packs without verifying them.
	NoVerify bool
	// MaxPackBytes is the largest uncompressed size a pack may have; 0
	// means the server's default.
	MaxPackBytes uint64
}

// Server is a jaccard-store server in this process: a database in a
// temporary directory, a bucket in memory, iroh on loopback. A test that
// calls Start cannot be parallel (the bucket sets environment variables).
//
// It lives as long as the test it was started for. Its methods are safe
// for concurrent use.
type Server struct {
	t   testing.TB
	srv *server.Server
	key irohkey.SecretKey

	// front is what clients reach the bucket through: every pre-signed
	// URL the server hands out leads here.
	front     *httptest.Server
	bucket    *fronted
	slow      atomic.Int64
	intercept atomic.Pointer[func(http.ResponseWriter, *http.Request) bool]
	// ended is closed when the test ends: nothing waits in front of the
	// bucket after that.
	ended chan struct{}

	mu sync.Mutex
	// addr is where the endpoint was bound first and is bound again by
	// every restart.
	addr netip.AddrPort
	// run is the serving under way, nil while the server is stopped.
	run *serving
}

// serving is the server answering on one endpoint, from a start to the
// stop that follows.
type serving struct {
	ep   *iroh.Endpoint
	stop context.CancelFunc
	done chan struct{}

	// dialled counts the connections clients were given, accepted those
	// that arrived here. A connection is complete for its client a moment
	// before it is for the server.
	dialled  atomic.Int64
	accepted atomic.Int64

	mu    sync.Mutex
	conns map[*iroh.Conn]struct{}
}

// Start starts a server: its database, its bucket and its endpoint. All of
// it is stopped and removed when the test ends.
func Start(t testing.TB, opts Options) *Server {
	t.Helper()
	b := buckettest.New(t)
	dir := t.TempDir()

	s := &Server{t: t, ended: make(chan struct{})}
	s.front = httptest.NewServer(http.HandlerFunc(s.serveBucket))
	t.Cleanup(func() {
		close(s.ended)
		s.front.CloseClientConnections()
		s.front.Close()
	})
	frontURL, err := url.Parse(s.front.URL)
	if err != nil {
		t.Fatalf("testserver: the bucket's front: %v", err)
	}

	database, err := db.Open(filepath.Join(dir, "store.sqlite"))
	if err != nil {
		t.Fatalf("testserver: opening the database: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	s.bucket = &fronted{Bucket: b, front: frontURL}
	s.srv, err = server.New(server.Config{
		DB:           database,
		Bucket:       s.bucket,
		Scratch:      filepath.Join(dir, "scratch"),
		NoVerify:     opts.NoVerify,
		MaxPackBytes: int64(opts.MaxPackBytes),
		Log:          quiet,
	})
	if err != nil {
		t.Fatalf("testserver: %v", err)
	}
	s.key, err = node.LoadOrCreateKey(filepath.Join(dir, "server.key"))
	if err != nil {
		t.Fatalf("testserver: %v", err)
	}

	s.start()
	// Cleanups run last in, first out: the serving ends before the
	// database under it is closed.
	t.Cleanup(s.Stop)
	return s
}

// start binds the endpoint and begins to answer on it. The first start
// takes a port the system picks; every later one takes that port again.
func (s *Server) start() {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run != nil {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	cfg := node.ServerConfig{Key: s.key, ALPN: wire.ALPN, Local: true, Bind: s.addr, Log: quiet}
	ep, err := node.Bind(ctx, cfg)
	// The port of a restart was this server's a moment ago; the system
	// may need that moment to give it out again.
	for wait := time.Now().Add(2 * time.Second); err != nil && s.addr.IsValid() && time.Now().Before(wait); {
		time.Sleep(20 * time.Millisecond)
		ep, err = node.Bind(ctx, cfg)
	}
	if err != nil {
		stop()
		s.t.Fatalf("testserver: binding the endpoint: %v", err)
	}
	s.addr = ep.LocalAddr()
	run := &serving{ep: ep, stop: stop, done: make(chan struct{}), conns: make(map[*iroh.Conn]struct{})}
	s.run = run
	go func() {
		defer close(run.done)
		run.serve(ctx, s.srv)
	}()
}

// Stop ends the server: its connections are closed and nothing answers at
// its address. The database and the bucket stay as they are, and the
// bucket goes on answering. Stop on a stopped server does nothing.
func (s *Server) Stop() {
	s.mu.Lock()
	run := s.run
	s.run = nil
	s.mu.Unlock()
	if run == nil {
		return
	}
	run.stop()
	<-run.done
	run.ep.Shutdown(context.Background())
}

// Restart brings the server back on the same address and data. A server
// that is running is stopped first.
func (s *Server) Restart() {
	s.t.Helper()
	s.Stop()
	s.start()
}

// DropConnections closes every client connection from the server's side.
// The server goes on answering: a client that dials again is served.
func (s *Server) DropConnections() {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run != nil {
		run.drop()
	}
}

// Forget makes the server lose every client connection without a word to
// its client, as a server does whose idle timeout ran out while the client
// slept: the endpoint goes away with its connections as they are, and a new
// one answers at the same address. A client's side of such a connection
// still takes streams and writes, and nothing comes back on it until the
// client's transport gives it up. Forget on a stopped server does nothing.
func (s *Server) Forget() {
	s.t.Helper()
	s.mu.Lock()
	run := s.run
	s.run = nil
	s.mu.Unlock()
	if run == nil {
		return
	}
	// The socket goes first: when the serving ends and closes its
	// connections, there is nothing left to say so through.
	run.ep.Shutdown(context.Background())
	run.stop()
	<-run.done
	s.start()
}

// SlowBucket makes every bucket transfer wait d before its first byte;
// zero undoes it. A transfer is a request of a client to the bucket, an
// upload as well as a download; a request that is waiting when d changes
// waits as it was told.
func (s *Server) SlowBucket(d time.Duration) {
	s.slow.Store(int64(d))
}

// Intercept puts f in front of the bucket; nil removes it. f sees every
// request of a client to the bucket when it arrives, before the wait of
// SlowBucket, on the goroutine that serves the request. When f returns
// true it has answered the request itself; otherwise the request goes on
// to the bucket. The path of a request ends in ".idx" for a pack's index
// and in ".data" for its data. An f that holds a request up must return
// when the request's context is done.
func (s *Server) Intercept(f func(w http.ResponseWriter, r *http.Request) bool) {
	if f == nil {
		s.intercept.Store(nil)
		return
	}
	s.intercept.Store(&f)
}

// serveBucket answers a client's request to the bucket.
func (s *Server) serveBucket(w http.ResponseWriter, r *http.Request) {
	if f := s.intercept.Load(); f != nil && (*f)(w, r) {
		return
	}
	if d := time.Duration(s.slow.Load()); d > 0 {
		wait := time.NewTimer(d)
		defer wait.Stop()
		select {
		case <-wait.C:
		case <-r.Context().Done():
			return
		case <-s.ended:
			return
		}
	}
	bucket := s.bucket.behind.Load()
	if bucket == nil {
		// A URL that leads here was signed, so this cannot be.
		http.Error(w, "testserver: the bucket has signed no URL yet", http.StatusBadGateway)
		return
	}
	bucket.ServeHTTP(w, r)
}

// Dial is a remote.Config.Dial for this server, with a fresh client key.
// The key is new with every call of Dial and the same for every
// connection the function returned dials.
func (s *Server) Dial() func(ctx context.Context) (remote.Conn, error) {
	s.t.Helper()
	cfg := s.dialConfig()
	return func(ctx context.Context) (remote.Conn, error) {
		conn, err := s.dial(ctx, cfg)
		if err != nil {
			// Not conn: a nil *node.Conn in the interface is no nil.
			return nil, err
		}
		return conn, nil
	}
}

// dial connects to the server and returns once the server has the
// connection as well, so that a DropConnections that follows a dial
// closes what was dialled.
func (s *Server) dial(ctx context.Context, cfg node.DialConfig) (*node.Conn, error) {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	conn, err := node.Dial(ctx, cfg)
	if err != nil || run == nil {
		return conn, err
	}
	// Each connection a client is given arrives at the server once, so
	// the count of arrivals reaches the count of dials.
	n := run.dialled.Add(1)
	for wait := time.Now().Add(5 * time.Second); run.accepted.Load() < n && time.Now().Before(wait); {
		select {
		case <-run.done:
			return conn, nil
		case <-ctx.Done():
			return conn, nil
		case <-time.After(time.Millisecond):
		}
	}
	return conn, nil
}

// Client is a plain jaccard-store client, for tests that write or delete
// references directly. It has a key and a connection of its own; the
// connection is the one dialled now, closed when the test ends and not
// dialled again after a Stop or DropConnections.
func (s *Server) Client(t testing.TB) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := s.dial(ctx, s.dialConfig())
	if err != nil {
		t.Fatalf("testserver: dialing the server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return client.New(conn, nil)
}

// dialConfig returns what a client dials the server with: a key of its
// own, new with every call, and the server's ID and loopback address.
func (s *Server) dialConfig() node.DialConfig {
	s.t.Helper()
	sk, err := node.LoadOrCreateKey(filepath.Join(s.t.TempDir(), "client.key"))
	if err != nil {
		s.t.Fatalf("testserver: %v", err)
	}
	s.mu.Lock()
	addr := s.addr
	s.mu.Unlock()
	return node.DialConfig{
		Key:    sk,
		ALPN:   wire.ALPN,
		Server: s.key.Public().EndpointID(),
		Addrs:  []netip.AddrPort{addr},
		Log:    quiet,
	}
}

// serve accepts connections and answers their streams, one request each,
// until ctx is done, and returns when the last request has been answered
// or given up. It is the server's own Serve but for the list of
// connections it keeps, which is what DropConnections closes.
func (r *serving) serve(ctx context.Context, srv *server.Server) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for ctx.Err() == nil {
		conn, err := r.ep.Accept(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
			case <-r.ep.Closed():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		r.mu.Lock()
		r.conns[conn] = struct{}{}
		r.mu.Unlock()
		r.accepted.Add(1)
		wg.Go(func() {
			defer func() {
				r.mu.Lock()
				delete(r.conns, conn)
				r.mu.Unlock()
			}()
			serveConn(ctx, srv, conn)
		})
	}
}

// drop closes the connections that are open now.
func (r *serving) drop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for conn := range r.conns {
		conn.Close()
	}
}

// serveConn answers the streams of one connection until it is closed or
// ctx is done.
func serveConn(ctx context.Context, srv *server.Server, conn *iroh.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	peer := conn.RemoteID().String()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			return
		}
		wg.Go(func() { serveStream(ctx, srv, peer, stream) })
	}
}

// serveStream reads one request from stream and writes the server's answer.
func serveStream(ctx context.Context, srv *server.Server, peer string, stream net.Conn) {
	defer stream.Close()
	stream.SetReadDeadline(time.Now().Add(requestTimeout))
	var req wire.Request
	if err := wire.ReadRequest(stream, &req); err != nil {
		return
	}
	stream.SetReadDeadline(time.Time{})
	wire.WriteFrame(stream, srv.Handle(ctx, peer, req))
}

// fronted is the bucket as the server sees it, with one difference: the
// pre-signed URLs it hands out lead to the front and not to the bucket
// itself. The fake S3 under it checks no signature, so a request that was
// signed for one host is taken at another.
type fronted struct {
	*bucket.Bucket
	front *url.URL
	// behind passes a request on to the bucket itself. Where that is
	// shows in the first URL the bucket signs, and nowhere before.
	behind atomic.Pointer[httputil.ReverseProxy]
}

// lead returns signed with the front in the place of the bucket's host.
func (f *fronted) lead(signed string, err error) (string, error) {
	if err != nil {
		return "", err
	}
	u, err := url.Parse(signed)
	if err != nil {
		return "", err
	}
	if f.behind.Load() == nil {
		bucket := &url.URL{Scheme: u.Scheme, Host: u.Host}
		f.behind.CompareAndSwap(nil, &httputil.ReverseProxy{
			Rewrite:       func(r *httputil.ProxyRequest) { r.SetURL(bucket) },
			FlushInterval: -1,
			ErrorLog:      slog.NewLogLogger(quiet.Handler(), slog.LevelError),
		})
	}
	u.Scheme, u.Host = f.front.Scheme, f.front.Host
	return u.String(), nil
}

func (f *fronted) PresignGet(ctx context.Context, objectKey string, ttl time.Duration) (string, error) {
	return f.lead(f.Bucket.PresignGet(ctx, objectKey, ttl))
}

func (f *fronted) PresignPut(ctx context.Context, objectKey string, ttl time.Duration) (string, error) {
	return f.lead(f.Bucket.PresignPut(ctx, objectKey, ttl))
}

func (f *fronted) PresignPart(ctx context.Context, objectKey, uploadID string, part int32, ttl time.Duration) (string, error) {
	return f.lead(f.Bucket.PresignPart(ctx, objectKey, uploadID, part, ttl))
}

func (f *fronted) PresignComplete(ctx context.Context, objectKey, uploadID string, ttl time.Duration) (string, error) {
	return f.lead(f.Bucket.PresignComplete(ctx, objectKey, uploadID, ttl))
}
