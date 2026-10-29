package e2e

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/narrenhaus/testserver"
)

func TestUploadOnOneFetchOnTheOther(t *testing.T) {
	h := start(t, testserver.Options{})
	dep := pathOf(t, "glibc-2.40", tree(1, 60<<10))
	p := pathOf(t, "hello-2.12", tree(2, 100<<10), dep)
	p.references = append(p.references, p.base()) // and itself, as most paths do

	// Uploaded on a with xz, as Nix does unless told otherwise.
	h.a.copy(dep, p)
	h.a.settled()
	h.bucket.forget()

	// The narinfo on b: it is the one that was uploaded, it names the NAR
	// where narrenhaus serves it, and b has signed it.
	info := h.b.narinfoOf(p)
	if info.URL != "nar/"+p.narHash+".nar" || info.Compression != "none" {
		t.Errorf("the narinfo names the NAR as %q with compression %q", info.URL, info.Compression)
	}
	if !slices.Contains(info.Sigs, uploadedSig) {
		t.Errorf("the signature that came with the upload is gone: %q", info.Sigs)
	}
	if len(info.Sigs) != 2 || h.a.signed(info) {
		t.Errorf("the narinfo b serves has the signatures %q, want the uploaded one and b's own", info.Sigs)
	}
	// b fetched the pack of the narinfo for it, index and data, and no
	// pack of content.
	root := p.narinfoRoot(t).String()
	if gets := h.bucket.gets(""); !slices.Equal(gets, []string{root, root}) {
		t.Errorf("for the narinfo b downloaded from the packs %v, want only the narinfo's, %s", gets, root)
	}
	if h.b.cache.Size() > 1<<10 {
		t.Errorf("b caches %d bytes after one narinfo", h.b.cache.Size())
	}

	// The NAR on b.
	h.b.substitute(p)
	if gets := h.bucket.gets(".data"); !slices.Equal(gets, []string{root, p.root.String()}) {
		t.Errorf("b downloaded data of the packs %v, want the narinfo's and the content's (%s)", gets, p.root)
	}
	// And the path it refers to, which nothing has asked b for yet.
	h.b.substitute(dep)

	// a serves what it was given without a download, under its own key.
	h.bucket.forget()
	fromA := h.a.substitute(p)
	if h.b.signed(fromA) || !slices.Contains(fromA.Sigs, uploadedSig) || len(fromA.Sigs) != 2 {
		t.Errorf("the narinfo a serves has the signatures %q, want the uploaded one and a's own", fromA.Sigs)
	}
	if gets := h.bucket.gets(""); len(gets) != 0 {
		t.Errorf("a downloaded %v to serve what was uploaded to it", gets)
	}
	// Nix does not send the path again: its narinfo is there.
	h.a.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	h.b.want(http.StatusOK, http.MethodHead, "/"+p.hash+".narinfo", nil)
}

func TestNixOrderOfTwoPointThirtyFive(t *testing.T) {
	h := start(t, testserver.Options{})
	// Five paths that refer to one another; the paths others stand on are
	// the small ones.
	libc := pathOf(t, "glibc-2.40", tree(1, 20<<10))
	zlib := pathOf(t, "zlib-1.3", tree(2, 40<<10), libc)
	openssl := pathOf(t, "openssl-3.4", tree(3, 60<<10), libc, zlib)
	curl := pathOf(t, "curl-8.11", tree(4, 80<<10), libc, zlib, openssl)
	app := pathOf(t, "app-1.0", tree(5, 100<<10), libc, curl)
	leavesFirst := []*storePath{libc, zlib, openssl, curl, app}

	h.a.opened()
	for _, p := range leavesFirst {
		h.a.want(http.StatusNotFound, http.MethodGet, "/"+p.hash+".narinfo", nil)
	}

	// Every NAR, the largest first, all at once; no narinfo yet.
	largestFirst := slices.SortedFunc(slices.Values(leavesFirst), func(a, b *storePath) int {
		return cmp.Compare(len(b.sent), len(a.sent))
	})
	if largestFirst[0] != app || largestFirst[4] != libc {
		t.Fatal("the paths of the test do not sort by size as it means them to")
	}
	failed := make(chan error, len(leavesFirst))
	for _, p := range largestFirst {
		go func() { failed <- h.a.sendNAR(p) }()
	}
	for range largestFirst {
		if err := <-failed; err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Then every narinfo, the leaves first, all at once: each is answered
	// when its content is on the server, whatever was sent before it.
	for _, p := range leavesFirst {
		go func() { failed <- h.a.sendNarinfo(p) }()
	}
	for range leavesFirst {
		if err := <-failed; err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	h.a.settled()
	if names := h.a.staged(); len(names) != 0 {
		t.Errorf("a has %v staged after every push ended", names)
	}

	// b substitutes the closure: the top path, then what its narinfo
	// names, as Nix walks the references.
	paths := map[string]*storePath{}
	for _, p := range leavesFirst {
		paths[p.base()] = p
	}
	seen := map[string]bool{}
	var walk func(p *storePath)
	walk = func(p *storePath) {
		if seen[p.base()] {
			return
		}
		seen[p.base()] = true
		info := h.b.substitute(p)
		for _, ref := range info.References {
			next, ok := paths[ref]
			if !ok {
				t.Fatalf("the narinfo of %s refers to %s, which was not uploaded", p.name, ref)
			}
			walk(next)
		}
	}
	walk(app)
	if len(seen) != len(leavesFirst) {
		t.Errorf("b substituted %d paths from the top one, want all %d", len(seen), len(leavesFirst))
	}

	// A second copy of the closure finds every narinfo and sends nothing.
	for _, p := range leavesFirst {
		h.a.want(http.StatusOK, http.MethodGet, "/"+p.hash+".narinfo", nil)
	}
}

func TestPatchAcrossInstances(t *testing.T) {
	h := start(t, testserver.Options{})
	// Two versions of a tree of 1 MiB that differ in one file of eight.
	version := func(seed string, changed byte) *storePath {
		entries := map[string]node{}
		for i := range byte(8) {
			s := 10 + i
			if i == 0 {
				s = changed
			}
			entries[fmt.Sprintf("lib%d.so", i)] = file(noise(s, 128<<10))
		}
		return pathOf(t, seed, dir(entries)).uncompressed()
	}
	v1, v2 := version("tool-1.0", 100), version("tool-1.1", 101)
	h.a.copy(v1)
	h.a.settled() // a push can lean only on what is on the server when it begins
	h.a.copy(v2)
	h.a.settled()

	// The server holds the second version as a patch of the first.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, packs, err := h.srv.Client(t).Lookup(ctx, testPrefix+"nar/"+v2.narHash+".nar")
	if err != nil {
		t.Fatalf("looking up the second version: %v", err)
	}
	if root != v2.root {
		t.Fatalf("the second version has the root %s on the server, want %s", root, v2.root)
	}
	if len(packs) != 2 || packs[0].Root != v2.root || packs[1].Root != v1.root {
		t.Fatalf("the second version is held by %d packs, want its own as a patch of the first's", len(packs))
	}

	// b serves it from the two packs, and has them as what they are.
	h.bucket.forget()
	h.b.substitute(v2)
	if meta, ok := h.b.cache.Meta(v2.root); !ok || meta.Complete || meta.Parent != v1.root {
		t.Errorf("b caches the second version as %+v (%v), want a patch of %s", meta, ok, v1.root)
	}
	if meta, ok := h.b.cache.Meta(v1.root); !ok || !meta.Complete {
		t.Errorf("b caches the first version's pack as %+v (%v), want a complete pack", meta, ok)
	}
	data := h.bucket.gets(".data")
	if !slices.Contains(data, v2.root.String()) || !slices.Contains(data, v1.root.String()) {
		t.Errorf("b downloaded data of the packs %v, want the patch and its parent", data)
	}
	// The patch is the smaller download by far: one file of eight.
	if patch, whole := h.b.cache.Size(), 2*v2.size; patch > whole*3/4 {
		t.Errorf("b caches %d bytes for a version and a patch, two whole versions are %d", patch, whole)
	}
	// The first version needs no download now.
	h.bucket.forget()
	h.b.substitute(v1)
	if data := h.bucket.gets(".data"); slices.Contains(data, v1.root.String()) {
		t.Error("b downloaded the first version's pack again, which it had as the patch's parent")
	}
}

func TestConnectionKilledBetweenRequests(t *testing.T) {
	h := start(t, testserver.Options{})
	one := pathOf(t, "one-1.0", tree(1, 10<<10))
	two := pathOf(t, "two-1.0", tree(2, 10<<10))
	h.a.copy(one, two)
	h.a.settled()

	h.b.narinfoOf(one)
	// The server ends every connection, as a restart of it or a sleep of
	// the laptop does. b learns of it only when it next asks.
	h.srv.DropConnections()
	// The next request is for a path b has nothing of: only the server
	// can have answered it.
	h.b.narinfoOf(two)
	h.b.narinfoOf(one)
	h.b.substitute(two)

	// The uploading side recovers the same way.
	h.srv.DropConnections()
	three := pathOf(t, "three-1.0", tree(3, 10<<10))
	h.a.copy(three)
	h.srv.DropConnections()
	h.b.substitute(three)
}

func TestServerDownAnswers(t *testing.T) {
	h := start(t, testserver.Options{})
	p := pathOf(t, "hello-2.12", tree(1, 20<<10))
	h.a.copy(p)
	h.a.settled()
	later := pathOf(t, "later-1.0", tree(2, 20<<10))

	// b has asked for nothing yet and has no connection.
	h.srv.Stop()

	// Serving answers 404 in time, which Nix survives: it builds the path
	// or asks another cache.
	type timed struct {
		took time.Duration
		err  error
	}
	served := make(chan timed, 1)
	go func() {
		began := time.Now()
		got, err := h.b.expect(http.StatusNotFound, http.MethodGet, "/"+p.hash+".narinfo", nil)
		if err == nil && (len(got.body) >= 1024 || !strings.HasPrefix(got.header.Get("Content-Type"), "text/plain")) {
			err = fmt.Errorf("a 404 with %d bytes of %q", len(got.body), got.header.Get("Content-Type"))
		}
		served <- timed{time.Since(began), err}
	}()

	// An upload answers 502, so that a copy fails at its first NAR and
	// says why.
	began := time.Now()
	h.a.want(http.StatusBadGateway, http.MethodHead, "/nar/"+later.narHash+".nar", nil)
	// The question for a compressed file needs no server: the answer is
	// always that it is not there.
	h.a.want(http.StatusNotFound, http.MethodHead, "/"+later.url, nil)
	got := h.a.want(http.StatusBadGateway, http.MethodPut, "/"+later.url, later.sent, "Content-Type", "application/x-nix-nar")
	if len(got.body) < 2 || len(got.body) >= 1024 || !bytes.Contains(got.body, []byte("server")) {
		t.Errorf("the 502 says %q, want a line that names the server", got.body)
	}
	if took := time.Since(began); took > 22*time.Second {
		t.Errorf("the uploads were refused after %s", took)
	}
	if names := h.a.staged(); len(names) != 0 {
		t.Errorf("a has %v staged with the server down", names)
	}
	h.a.want(http.StatusBadGateway, http.MethodPut, "/"+later.hash+".narinfo", later.narinfo(), "Content-Type", "text/x-nix-narinfo")

	answer := <-served
	if answer.err != nil {
		t.Error(answer.err)
	}
	if answer.took > 11*time.Second {
		t.Errorf("b answered the narinfo after %s, want a 404 within 11s", answer.took)
	}
	// The NAR and HEAD are 404 as well, and now without the wait: b knows
	// the server is gone.
	began = time.Now()
	h.b.want(http.StatusNotFound, http.MethodHead, "/"+p.hash+".narinfo", nil)
	h.b.want(http.StatusNotFound, http.MethodGet, "/nar/"+p.narHash+".nar", nil)
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("b took %s for two more answers with the server known to be down", took)
	}

	// a has the path it uploaded and serves it without the server.
	h.a.substitute(p)
}
