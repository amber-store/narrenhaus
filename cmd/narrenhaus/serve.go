package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/wire"
	irohkey "github.com/tmc/go-iroh/key"

	"github.com/amber-store/narrenhaus/httpd"
	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nixconf"
	"github.com/amber-store/narrenhaus/packcache"
	"github.com/amber-store/narrenhaus/refs"
	"github.com/amber-store/narrenhaus/remote"
	"github.com/amber-store/narrenhaus/upload"
)

// The key files of the configuration directory.
const (
	// clientKeyFile holds the identity shown to the server.
	clientKeyFile = "client.key"
	// signingKeyFile holds the key that signs the narinfos served.
	signingKeyFile = "signing.key"
)

const (
	// shutdownGrace is how long requests that run when the signal comes
	// get to finish.
	shutdownGrace = 10 * time.Second
	// cutWait is how long serve waits for the requests it cut. They end
	// at once, their connections being closed; the limit is for one that
	// does not.
	cutWait = 5 * time.Second
	// sweepEvery is how often staged uploads nobody came back for are
	// looked for.
	sweepEvery = time.Minute
	// readHeaderTimeout is how long a connection may take to send the
	// head of a request, and idleTimeout how long it may stay open
	// between two. Nothing limits a body in either direction: a NAR takes
	// as long as it is, and the answer to a narinfo waits for a push.
	readHeaderTimeout = 30 * time.Second
	idleTimeout       = 5 * time.Minute
)

// host is what serve takes from the machine besides its options. The
// tests put a server in their own process in its place.
type host struct {
	// connect returns the dial of the server for the client's key.
	connect func(key irohkey.SecretKey, server irohkey.EndpointID, log *slog.Logger) func(context.Context) (remote.Conn, error)
	// grace is how long requests that run when serve is told to stop get
	// to finish.
	grace time.Duration
}

// thisHost is the machine as it is: the server is found by its endpoint
// ID over iroh.
func thisHost() host {
	return host{
		connect: func(key irohkey.SecretKey, server irohkey.EndpointID, log *slog.Logger) func(context.Context) (remote.Conn, error) {
			cfg := node.DialConfig{Key: key, ALPN: wire.ALPN, Server: server, Log: log}
			return func(ctx context.Context) (remote.Conn, error) {
				conn, err := node.Dial(ctx, cfg)
				if err != nil {
					// Not conn: a nil *node.Conn in the interface is no
					// nil.
					return nil, err
				}
				return conn, nil
			}
		},
		grace: shutdownGrace,
	}
}

// serve is the command serve: it says what the Nix configuration lacks and
// then serves the binary cache until ctx is done, which is the signal to
// stop. Everything it says goes to stderr: the advice as it is, the rest
// as log lines.
//
// It takes hold of what can refuse before it says anything: the keys, the
// cache directory and the port. An error names the option at fault.
func serve(ctx context.Context, opts options, stderr io.Writer, h host) error {
	log := slog.New(slog.NewTextHandler(stderr, nil))

	clientKey, err := node.LoadOrCreateKey(filepath.Join(opts.ConfigDir, clientKeyFile))
	if err != nil {
		return fmt.Errorf("--config-dir: %w", err)
	}
	signing, err := narinfo.LoadOrCreateSigningKey(filepath.Join(opts.ConfigDir, signingKeyFile))
	if err != nil {
		return fmt.Errorf("--config-dir: %w", err)
	}
	cache, err := packcache.Open(opts.CacheDir, opts.CacheSize, packcache.Origin{
		Server: opts.Server.String(),
		Prefix: opts.Prefix,
	})
	if err != nil {
		return fmt.Errorf("--cache-dir: %w", err)
	}
	// The directory is given up whichever way serve returns; closing the
	// cache a second time does nothing.
	defer cache.Close()

	ln, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return fmt.Errorf("--listen %q: %w", opts.Listen, err)
	}
	// Until the server below has it, the listener is closed here.
	defer ln.Close()
	address := advertised(opts.Listen, ln.Addr())

	// The advice. Connections that arrive meanwhile wait at the port.
	report := nixconf.Check(ctx, nixconf.System(), nixconf.Input{Listen: address, PublicKey: signing.Public()})
	if _, err := io.WriteString(stderr, report.Text); err != nil {
		return fmt.Errorf("writing the advice: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}

	// The work of the cache, pushes and downloads, belongs to no request
	// and goes on until serve has stopped answering.
	work, stopWork := context.WithCancel(context.Background())
	rem := remote.New(remote.Config{Dial: h.connect(clientKey, opts.Server, log), Log: log})
	store := refs.New(work, cache, rem, refs.Options{Prefix: opts.Prefix, Log: log})
	up := upload.New(work, cache, rem, store, upload.Options{Log: log})
	var sweeper sync.WaitGroup
	sweeper.Go(func() {
		tick := time.NewTicker(sweepEvery)
		defer tick.Stop()
		for {
			select {
			case <-work.Done():
				return
			case now := <-tick.C:
				up.Sweep(now)
			}
		}
	})
	// The way down, once serve has stopped answering: the work, then what
	// the work stood on.
	down := sync.OnceFunc(func() {
		stopWork()
		sweeper.Wait()
		up.Close()
		store.Close()
		cache.Close()
		rem.Close()
	})
	defer down()

	requests := &requestLog{log: log}
	// Requests live under a context of their own, so that the ones cut at
	// the end stop waiting.
	answering, cut := context.WithCancel(context.Background())
	defer cut()
	srv := &http.Server{
		Handler: requests.wrap(httpd.New(httpd.Config{
			Store:    store,
			Uploader: up,
			Key:      signing,
			Priority: opts.Priority,
			Log:      log,
		})),
		BaseContext:       func(net.Listener) context.Context { return answering },
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()
	log.Info("serving", "url", nixconf.URL(address), "server", opts.Server.String(), "prefix", opts.Prefix,
		"cache", opts.CacheDir, "limit", opts.CacheSize)

	select {
	case err = <-failed:
		// The listener gave up, which it does not do by itself.
		log.Error("no more connections are accepted", "error", err.Error())
		err = fmt.Errorf("--listen %q: %w", opts.Listen, err)
	case <-ctx.Done():
		err = nil
		log.Info("stopping: requests that are running get time to finish", "limit", h.grace)
	}

	// No new requests; the running ones get their time, then they are
	// cut. Pushes that still run after that are abandoned.
	grace, cancel := context.WithTimeout(context.Background(), h.grace)
	defer cancel()
	if srv.Shutdown(grace) != nil {
		cut()
		srv.Close()
		if requests.wait(cutWait) {
			log.Warn("requests that were still running have been cut")
		} else {
			log.Warn("requests that were cut are still running")
		}
	}
	down()
	log.Info("stopped")
	return err
}

// advertised is the address Nix is to be told for the option listen: the
// option as it is, unless it left the port to the system (port 0), in which
// case it is the option's host with the port the listener got.
func advertised(listen string, bound net.Addr) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n != 0 {
		return listen
	}
	tcp, ok := bound.(*net.TCPAddr)
	if !ok {
		return listen
	}
	return net.JoinHostPort(host, strconv.Itoa(tcp.Port))
}

// requestLog stands before the handler of the cache: it writes the log's
// line for each NAR served, and knows when no request is running.
//
// It lets a panic of the handler pass: that is how the handler cuts the
// connection of a NAR it cannot send to its end, and it has logged why.
type requestLog struct {
	log     *slog.Logger
	running atomic.Int64
}

func (l *requestLog) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.running.Add(1)
		defer l.running.Add(-1)
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/nar/") {
			next.ServeHTTP(w, r)
			return
		}
		began := time.Now()
		sent := &counted{ResponseWriter: w}
		next.ServeHTTP(sent, r)
		if sent.status == http.StatusOK {
			l.log.Info("NAR served", "path", r.URL.Path, "bytes", sent.bytes,
				"took", time.Since(began).Round(time.Millisecond))
		}
	})
}

// wait waits until no request is running and reports whether that came
// within limit.
func (l *requestLog) wait(limit time.Duration) bool {
	for end := time.Now().Add(limit); l.running.Load() > 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			return false
		}
	}
	return true
}

// counted notes the status and the bytes of an answer.
type counted struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (c *counted) WriteHeader(status int) {
	if c.status == 0 {
		c.status = status
	}
	c.ResponseWriter.WriteHeader(status)
}

func (c *counted) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	n, err := c.ResponseWriter.Write(p)
	c.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the writer below.
func (c *counted) Unwrap() http.ResponseWriter { return c.ResponseWriter }
