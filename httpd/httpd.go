// Package httpd is narrenhaus as Nix sees it: a binary cache over HTTP.
//
//	GET, HEAD  /nix-cache-info           what kind of cache this is
//	GET, HEAD  /<hash>.narinfo           the narinfo of a store path, signed as it is served
//	PUT        /<hash>.narinfo           the narinfo of an upload
//	GET        /nar/<hash>.nar           a NAR, uncompressed, under its own hash
//	HEAD       /nar/<hash>.nar           whether Nix has to send that NAR
//	PUT        /nar/<hash>.nar[.<ext>]   the NAR of an upload, in the compression Nix chose
//
// <hash> is a store path's hash for a narinfo, 32 characters, and a
// SHA-256 for a NAR, 52, both in Nix's base 32. Every other path is 404,
// and a PUT to one is 405.
//
// The two directions fail differently. Nix counts a 5xx or a request that
// is silent for too long as an error, which can fail a build, and survives
// a 404: so serving answers 404 whatever went wrong, and in time. An
// upload answers with the status that says why, so that a copy fails at
// once. The body of an error is one line of plain text under 1024 bytes,
// which is as much as Nix shows.
//
// Nothing of a NAR is sent before its reference is local and checked.
// After the header a failure can only be shown by cutting the connection,
// and that is what happens.
package httpd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/amber-store/narrenhaus/narinfo"
	"github.com/amber-store/narrenhaus/nix32"
	"github.com/amber-store/narrenhaus/refs"
	"github.com/amber-store/narrenhaus/upload"
)

const (
	// defaultServeWait is below the 300 seconds of silence after which
	// Nix gives a request up and counts it as failed.
	defaultServeWait = 240 * time.Second

	storeHashLen = 32 // a store path's hash, 20 bytes
	narHashLen   = 52 // a SHA-256

	// maxExt is the longest extension of a NAR URL. Nix writes xz, bz2,
	// zst, lzip, lz4 and br.
	maxExt = 16

	// maxText is the most bytes of an error's text. Nix shows the body of
	// an answer only when it is below 1024 bytes.
	maxText = 1000

	typeCacheInfo = "text/x-nix-cache-info"
	typeNarinfo   = "text/x-nix-narinfo"
	typeNAR       = "application/x-nix-nar"
)

// Config is what the handler serves from. Store and Uploader are needed.
type Config struct {
	Store     *refs.Store
	Uploader  *upload.Uploader
	Key       narinfo.SigningKey
	Priority  int
	ServeWait time.Duration // 0 means 240 s: how long a narinfo or NAR request waits for its reference
	Log       *slog.Logger
}

// server is the handler's state.
type server struct {
	store     *refs.Store
	up        *upload.Uploader
	key       narinfo.SigningKey
	cacheInfo []byte
	wait      time.Duration
	log       *slog.Logger
}

// New returns the handler of the binary cache.
func New(cfg Config) http.Handler {
	s := &server{
		store: cfg.Store,
		up:    cfg.Uploader,
		key:   cfg.Key,
		cacheInfo: fmt.Appendf(nil, "StoreDir: %s\nWantMassQuery: 1\nPriority: %d\n",
			narinfo.StoreDir, cfg.Priority),
		wait: cfg.ServeWait,
		log:  cfg.Log,
	}
	if s.wait <= 0 {
		s.wait = defaultServeWait
	}
	if s.log == nil {
		s.log = slog.Default()
	}

	// A GET pattern takes HEAD as well. The last two patterns take
	// whatever the others leave, so the mux has no answer of its own.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /nix-cache-info", s.getCacheInfo)
	mux.HandleFunc("GET /{file}", s.getNarinfo)
	mux.HandleFunc("PUT /{file}", s.putNarinfo)
	mux.HandleFunc("GET /nar/{file}", s.getNAR)
	mux.HandleFunc("PUT /nar/{file}", s.putNAR)
	mux.HandleFunc("PUT /", notAllowed)
	mux.HandleFunc("/", notFound)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()
		seen := &observed{ResponseWriter: w}
		defer func() {
			s.log.Debug("request", "method", r.Method, "path", r.URL.Path,
				"status", seen.code(), "bytes", seen.bytes, "took", time.Since(began))
		}()
		// The mux would redirect a path such as //x or /a/../x to its
		// clean form. No path of the cache is written that way.
		if p := r.URL.Path; path.Clean(p) != p {
			unknown(seen, r)
			return
		}
		mux.ServeHTTP(seen, r)
	})
}

// observed notes what a handler answered, for the log.
type observed struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (o *observed) WriteHeader(status int) {
	if o.status == 0 {
		o.status = status
	}
	o.ResponseWriter.WriteHeader(status)
}

func (o *observed) Write(p []byte) (int, error) {
	if o.status == 0 {
		o.status = http.StatusOK
	}
	n, err := o.ResponseWriter.Write(p)
	o.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the writer below.
func (o *observed) Unwrap() http.ResponseWriter { return o.ResponseWriter }

// code is the status the request was answered with: 200 for a handler
// that returned without saying one.
func (o *observed) code() int {
	if o.status == 0 {
		return http.StatusOK
	}
	return o.status
}

// isHash reports whether s is n characters that are a hash in Nix's base
// 32: 32 for 20 bytes, 52 for 32.
func isHash(s string, n int) bool {
	if len(s) != n {
		return false
	}
	_, err := nix32.Decode(s)
	return err == nil
}

// narinfoFile takes the last segment of a narinfo's URL apart:
// <hash>.narinfo.
func narinfoFile(name string) (hash string, ok bool) {
	hash, ok = strings.CutSuffix(name, ".narinfo")
	return hash, ok && isHash(hash, storeHashLen)
}

// narFile takes the last segment of a NAR's URL apart: <hash>.nar, or
// <hash>.nar.<ext> for a compressed file, where ext is not empty.
func narFile(name string) (hash, ext string, ok bool) {
	if len(name) < narHashLen || !isHash(name[:narHashLen], narHashLen) {
		return "", "", false
	}
	hash, rest := name[:narHashLen], name[narHashLen:]
	if rest == ".nar" {
		return hash, "", true
	}
	ext, ok = strings.CutPrefix(rest, ".nar.")
	if !ok || ext == "" || len(ext) > maxExt {
		return "", "", false
	}
	for _, c := range []byte(ext) {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return "", "", false
		}
	}
	return hash, ext, true
}

// waiting returns the context a serving request waits under for its
// reference: the request's own, ended after the wait.
func (s *server) waiting(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.wait)
}

// getCacheInfo answers GET and HEAD /nix-cache-info (spec 6.1).
func (s *server) getCacheInfo(w http.ResponseWriter, r *http.Request) {
	send(w, r, typeCacheInfo, s.cacheInfo)
}

// getNarinfo answers GET and HEAD /<hash>.narinfo (spec 6.2). HEAD asks
// the server and downloads nothing; GET gives the stored text with this
// machine's signature added.
func (s *server) getNarinfo(w http.ResponseWriter, r *http.Request) {
	hash, ok := narinfoFile(r.PathValue("file"))
	if !ok {
		notFound(w, r)
		return
	}
	ctx, cancel := s.waiting(r)
	defer cancel()

	if r.Method == http.MethodHead {
		if ok, err := s.store.HasNarinfo(ctx, hash); err != nil || !ok {
			notFound(w, r)
			return
		}
		w.Header().Set("Content-Type", typeNarinfo)
		w.WriteHeader(http.StatusOK)
		return
	}
	// Whatever is wrong, the store has logged it, and to Nix there is
	// nothing here.
	info, err := s.store.Narinfo(ctx, hash)
	if err != nil {
		notFound(w, r)
		return
	}
	send(w, r, typeNarinfo, info.Signed(s.key.Sign(info.Fingerprint())))
}

// putNarinfo answers PUT /<hash>.narinfo (spec 5.3).
func (s *server) putNarinfo(w http.ResponseWriter, r *http.Request) {
	hash, ok := narinfoFile(r.PathValue("file"))
	if !ok {
		notAllowed(w, r)
		return
	}
	// From 2.32 Nix can be told to send narinfos compressed, and says so
	// in this header.
	if enc := r.Header.Get("Content-Encoding"); enc != "" {
		fail(w, r, http.StatusUnsupportedMediaType, fmt.Sprintf(
			"a narinfo must be sent as plain text, this one has Content-Encoding %.40q", enc))
		return
	}
	// One byte more than a narinfo may have shows the uploader that this
	// one is too long.
	body, err := io.ReadAll(io.LimitReader(r.Body, narinfo.MaxStored+1))
	if err != nil {
		fail(w, r, http.StatusBadRequest, fmt.Sprintf("the body could not be read: %v", err))
		return
	}
	if err := s.up.PutNarinfo(r.Context(), hash, body); err != nil {
		s.uploadFailed(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// getNAR answers GET and HEAD /nar/<file>. Only an uncompressed NAR under
// its own hash is ever there: a URL with an extension names a compressed
// file by that file's hash, and narrenhaus keeps no such file.
func (s *server) getNAR(w http.ResponseWriter, r *http.Request) {
	hash, ext, ok := narFile(r.PathValue("file"))
	if !ok || ext != "" {
		notFound(w, r)
		return
	}
	if r.Method == http.MethodHead {
		s.headNAR(w, r, hash)
		return
	}

	ctx, cancel := s.waiting(r)
	c, err := s.store.Content(ctx, hash)
	cancel()
	if err != nil {
		notFound(w, r)
		return
	}
	// However the handler ends, the packs are let go.
	defer c.Release()

	h := w.Header()
	h.Set("Content-Type", typeNAR)
	h.Set("Content-Length", strconv.FormatUint(c.NarSize(), 10))
	out := &sink{w: w}
	n, err := c.WriteTo(out)
	if err == nil {
		return
	}
	// The length is promised and part of the NAR is out: the one way
	// left to say that this is not the NAR is to cut the connection.
	if out.err != nil {
		s.log.Info("a NAR was not sent to its end: the client is gone",
			"hash", hash, "sent", n, "of", c.NarSize(), "error", out.err.Error())
	} else {
		s.log.Error("a NAR was not sent to its end: it could not be written from the cache",
			"hash", hash, "sent", n, "of", c.NarSize(), "error", err.Error())
	}
	panic(http.ErrAbortHandler)
}

// sink passes on what is written to it and keeps the error it failed
// with, which tells a client that left from a NAR that could not be
// written.
type sink struct {
	w   io.Writer
	err error
}

func (s *sink) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	if err != nil && s.err == nil {
		s.err = err
	}
	return n, err
}

// headNAR answers HEAD /nar/<hash>.nar, Nix's question whether it has to
// send that NAR (spec 5.1). Nothing is downloaded for it, so there is no
// length to state.
func (s *server) headNAR(w http.ResponseWriter, r *http.Request, hash string) {
	ok, err := s.up.HeadNAR(r.Context(), hash)
	switch {
	case err != nil:
		s.uploadFailed(w, r, err)
	case !ok:
		notFound(w, r)
	default:
		w.Header().Set("Content-Type", typeNAR)
		w.WriteHeader(http.StatusOK)
	}
}

// putNAR answers PUT /nar/<hash>.nar and /nar/<hash>.nar.<ext> (spec
// 5.1). The extension says nothing: the uploader knows a compression by
// the body's first bytes.
//
// When the answer is an error the body may not have been read, and for a
// server that cannot be reached none of it was. net/http then reads what
// is left if that is little and closes the connection after the answer
// if it is much, so the rest of a large NAR is not waited for.
func (s *server) putNAR(w http.ResponseWriter, r *http.Request) {
	hash, _, ok := narFile(r.PathValue("file"))
	if !ok {
		notAllowed(w, r)
		return
	}
	if err := s.up.PutNAR(r.Context(), hash, r.Body); err != nil {
		s.uploadFailed(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// uploadFailed answers an upload request that failed: with the status and
// the text the uploader gave it, and with 500 for an error the uploader
// did not foresee, whose text goes to the log and not to the client.
func (s *server) uploadFailed(w http.ResponseWriter, r *http.Request, err error) {
	var e *upload.Error
	if errors.As(err, &e) {
		fail(w, r, e.Status, e.Msg)
		return
	}
	s.log.Error("an upload request failed with an error that has no answer of its own",
		"method", r.Method, "path", r.URL.Path, "error", err.Error())
	fail(w, r, http.StatusInternalServerError, "narrenhaus failed on this request; its log says how")
}

// unknown answers a request for a path the cache does not have.
func unknown(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut {
		notAllowed(w, r)
		return
	}
	notFound(w, r)
}

func notFound(w http.ResponseWriter, r *http.Request) {
	fail(w, r, http.StatusNotFound, "not found")
}

// notAllowed answers a PUT of something narrenhaus does not take: a NAR
// listing, a build log, debug information.
func notAllowed(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "GET, HEAD")
	fail(w, r, http.StatusMethodNotAllowed, "narrenhaus takes NARs and narinfos only")
}

// send answers 200 with body. net/http leaves the body out for HEAD.
func send(w http.ResponseWriter, r *http.Request, contentType string, body []byte) {
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

// fail answers status with text as the body: one line of plain text,
// short enough for Nix to show. HEAD gets the status alone, and no length
// of a body that is not there.
func fail(w http.ResponseWriter, r *http.Request, status int, text string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	if len(text) > maxText {
		text = strings.ToValidUTF8(text[:maxText], "")
	}
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.Itoa(len(text)+1))
	w.WriteHeader(status)
	io.WriteString(w, text+"\n")
}
