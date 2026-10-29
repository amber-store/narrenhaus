# narrenhaus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Updated after the whole-branch review:** the Tech Stack line and the interface blocks of Tasks B3, B5, B6, B8, B9 and B10 state the interfaces as the fixes that followed the review left them: the xz decoder, the limit of `nartree.Ingest` with `MaxNARSize` and `ErrTooLarge`, `packcache.ErrForeign` and `ErrLimit`, `testserver.Forget`, `remote.Config.RepeatAfter`, `httpd.Config.Host`, and the fields of `nixconf.Env` for the user's configuration file. The tasks, their steps and their lists of tests are as they were written and were not revised; where they differ from the program, the spec as revised holds.

**Goal:** A local Nix binary cache daemon, `narrenhaus`, that stores NARs as chunked core objects on a jaccard-store server (two references per store path) and serves them back uncompressed from a size-bounded local cache of pack files.

**Architecture:** Part A adds two things to jaccard-store's `client` package (push from any object source; lookup and fetch packs without a packstore) and releases them as v0.8.0. Part B builds narrenhaus on that release as flat Go packages: `nar` (strict NAR codec), `nartree` (NAR ⇄ core objects), `narinfo`, `packcache` (expanded pack files with LRU eviction), `remote` (one shared iroh connection), `refs` (making references local and verified), `upload` (staging and push work), `httpd` (the binary-cache HTTP surface), `nixconf` (setup check), `cmd/narrenhaus`.

**Tech Stack:** Go 1.26.3 (as jaccard-store's `go.mod`), `github.com/amber-store/jaccard-store` v0.8.0, `github.com/amber-store/core` v0.10.0, `github.com/urfave/cli/v2`, `github.com/klauspost/compress` (zstd, gzip), `github.com/xi2/xz` (the xz decoder, which takes a limit on the dictionary; `github.com/ulikunitz/xz` only in tests, to write xz), stdlib `compress/bzip2`, `crypto/ed25519`; flake with gonixgo v0.5.0.

**Spec:** `docs/superpowers/specs/2026-10-29-narrenhaus-design.md` (in `~/amber-store/narrenhaus`). Section numbers below ("spec 5.1") refer to it. Every implementer reads the spec sections named in their task before writing code.

## Global Constraints

- Repositories: Part A in `../jaccard-store` (branch `client-objects-and-fetch`), Part B in `../narrenhaus` (branch `main`, local only, no remote).
- narrenhaus `go.mod`: `module github.com/amber-store/narrenhaus`, `go 1.26.3`; jaccard-store is required at the released tag `v0.8.0` only. No pseudo-version, no `replace`, ever.
- Flat top-level packages. No `internal/` directory.
- CLI on `github.com/urfave/cli/v2`; every option has a flag and a `NARRENHAUS_…` environment variable.
- Licence `LGPL-3.0-only`: `LICENSE` (LGPL text) and `COPYING` (GPL text), copied from jaccard-store.
- No real server endpoint ID and no machine name in the tree or in a commit. Tests use the RFC 8032 test-vector key (`d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a`); examples use the prefix `acme/`.
- Core chunking and the metadata convention of spec 4.2 are frozen: `chunkers.SplitBytes(r, nil, …)`, `chunkers.NewItemChunker(ingest.DefaultItemBits)`, modes `0o100444` / `0o100555` / `0o040555` / `0o120777`, UID 0, GID 0, Mtime `1_000_000_000` ns.
- TDD: a failing test first, then the code. Commit after each task with only that task's paths staged (`git add <paths>`, never `git add -A`). Commit messages end with the two attribution lines used in this repository's history.
- Run Go directly (`go test ./<pkg>/...`); `go` 1.26.3 is on `PATH`. Never leave a built binary behind: no `go build` output in the tree, `nix build --no-link` only.
- Tests that need the `nix` program, a network or a container skip themselves when it is missing (`t.Skip`), because gonixgo runs package tests inside the build sandbox.
- Error bodies of HTTP answers are plain text under 1024 bytes.

## Review Focus

Conditions the spec implies but does not test; each has a test in the task that owns the code.

1. Two narrenhaus processes started on one cache directory: the second must refuse with a message naming the directory, not corrupt the first (Task B5: `TestOpenRefusesADirectoryInUse`).
2. A client that disconnects in the middle of a NAR download: the packs it held are released and evictable (Task B9: `TestAbandonedNARDownloadReleasesItsPacks`).
3. Bad start-up input: a prefix that makes an invalid reference name, a malformed endpoint ID, `--cache-size` that is zero or not a size, a listen port in use — each a one-line error and a non-zero exit, no panic (Task B11: `TestServeRefusesBadOptions`).
4. A NAR much larger than memory budgets assume: ingest and serving stream; a 64 MiB file is ingested and served with allocations that do not grow with its size (Task B3: `TestIngestIsStreaming`).
5. A cache directory that cannot be written (read-only, or `tmp/` missing after a manual cleanup): `Open` recreates what it can and fails clearly otherwise (Task B5: `TestOpenRecreatesItsDirectories`).

---

# Part A: jaccard-store v0.8.0

Work in `../jaccard-store` on a new branch `client-objects-and-fetch` from `main`. Existing behaviour and tests must not change. Run `gofmt -l .`, `go vet ./...` and `go test ./client/... ./packfile/... ./e2e/...` before each commit (the e2e container tests skip without Docker).

### Task A1: Push from any object source

**Files:**
- Modify: `client/push.go` (`Push`, `keySet`, `push`, `build`)
- Create: `client/objects.go`
- Test: `client/push_test.go`, `e2e/objects_test.go`

**Interfaces:**
- Produces:
  ```go
  // ObjectSource is where a push reads objects from. *packstore.Store is
  // one. (Not "Objects": client.Objects is already the progress unit.)
  // Get must be safe for concurrent calls and return a slice the caller owns.
  type ObjectSource interface {
      Get(k key.Key) ([]byte, error)
  }

  // PushFrom is Push for any source of objects.
  func (c *Client) PushFrom(ctx context.Context, objects ObjectSource, name string, root key.Key, opts PushOptions) (PushResult, error)
  ```
  `Push(ctx, objects *packstore.Store, …)` keeps its signature and calls `PushFrom`. A source that also has `SortByLocation(keys []key.Key)` (unexported interface `sorter` in `objects.go`) has it applied to the pack's own keys before packing, exactly where `build` calls it today; other sources are packed in key-set order.

- [ ] **Step 1: Failing tests.** In `client/push_test.go`: `TestBuildOrdersByLocationWhenTheSourceCan` (a fake source recording `SortByLocation` calls: called once with the pack's own keys) and `TestBuildKeepsKeySetOrderOtherwise` (a map-backed source: objects are written in `keys` order). In `e2e/objects_test.go` (same harness as `e2e_test.go`): `TestPushFromAMapEqualsPushFromAPackstore` (push one directory tree twice under two names, once with `Push` from a packstore and once with `PushFrom` from a `map[key.Key][]byte`; both report the same `Root`, the second reports `Stored`), and `TestPushFromASingleBlobRoot` (root = one Blob of 300 bytes; `PushResult.Objects == 1`; a `Pull` of the name into a fresh packstore returns that root and `Packs == 1`), run both verifying and with `--no-verify` as `noverify_test.go` does.
- [ ] **Step 2: Run** `go test ./client/ -run 'TestBuild' ./e2e/ -run 'TestPushFrom'` — expect compile failures for `PushFrom`.
- [ ] **Step 3: Implement.** Change the `objects *packstore.Store` parameter of `keySet`, `push` and `build` to `ObjectSource`; in `build` replace `objects.SortByLocation(own)` with a type assertion on `sorter`. Add `PushFrom` (the body of today's `Push`) and make `Push` call it. Update the package comment to say a push reads from any `Objects`.
- [ ] **Step 4: Run** `gofmt -l . ; go vet ./... ; go test ./client/... ./e2e/...` — all pass, including every pre-existing test.
- [ ] **Step 5: Commit** `client: push from any source of objects`.

### Task A2: Lookup and fetch packs without a packstore

**Files:**
- Create: `client/fetch.go`
- Modify: `client/pull.go` (`Pull` and `importPack` reuse the new helpers; behaviour unchanged)
- Test: `client/fetch_test.go`, `e2e/fetch_test.go`

**Interfaces:**
- Produces:
  ```go
  // Pack is one pack of a reference as the server named it in a lookup.
  // Its URLs expire: fetch it soon after the lookup.
  type Pack struct {
      Root      key.Key
      Objects   uint64 // objects in the pack
      Bytes     uint64 // their uncompressed size
      DataSize  uint64 // size of the compressed data
      IndexSize uint64
      // unexported: indexURL, dataURL string
  }

  // Lookup returns the root name points at and the packs that hold it: the
  // reference's own pack and, for a patch pack, its parent. It fetches
  // nothing. An unknown name is ErrNotFound.
  func (c *Client) Lookup(ctx context.Context, name string) (key.Key, []Pack, error)

  // Fetch downloads p. It holds the index to what the server announced,
  // then hands each object to each, in offset order, after checking it
  // against its key. Objects handed over back to back are the pack's
  // uncompressed data at the index's offsets. Fetch returns the index.
  //
  // An index or object that is wrong fails with an error wrapping
  // ErrBadPack; a failure of the transfer or of each is returned as it is.
  func (c *Client) Fetch(ctx context.Context, p Pack, each func(o packfile.Object) error) (*packfile.Index, error)

  // ErrBadPack wraps every error of a fetch that is the pack's fault, not
  // the transfer's.
  var ErrBadPack = errors.New("client: bad pack")

  // StatusCode returns the HTTP status of the bucket's answer that err
  // reports, or 0 when err is not one.
  func StatusCode(err error) int
  ```
  `Lookup` applies the checks `Pull` applies to the answer (root parses, 1 or 2 packs, each pack's root parses). `Fetch` applies `importPack`'s checks (`Objects <= packfile.MaxEntries`, `IndexSize == packfile.IndexSize(Objects)`, the parsed index's `Len()` and `DataSize()` equal the announced ones, data limited to `DataSize`), and `verify.Object` on each object.

- [ ] **Step 1: Failing tests.** `client/fetch_test.go` with an `httptest` bucket and a fake `Opener` as `http_test.go` builds them: `TestLookupOfAnUnknownName` (`errors.Is(err, ErrNotFound)`); `TestLookupRefusesThreePacks`; `TestFetchHandsOverObjectsInOffsetOrder` (concatenating `o.Data` equals `packfile.Expand` of the same pack); `TestFetchRefusesAnObjectThatDoesNotMatchItsKey` (`errors.Is(err, ErrBadPack)`, and `each` was not called for that object); `TestFetchRefusesAnIndexOtherThanAnnounced` (`ErrBadPack`); `TestFetchReportsTheBucketsStatus` (bucket answers 403: `StatusCode(err) == 403`, not `ErrBadPack`); `TestFetchReturnsTheErrorOfEach`. `e2e/fetch_test.go`: `TestLookupAndFetchABaseAndAPatch` (push a tree, push a changed tree as a patch; `Lookup` of the second names two packs, own first; fetching both and reading by index reproduces every object of `fstree.ReachableKeys(root)`).
- [ ] **Step 2: Run** `go test ./client/ -run 'TestLookup|TestFetch'` — compile failure.
- [ ] **Step 3: Implement** `Lookup`, `Fetch`, `ErrBadPack`, `StatusCode` in `client/fetch.go`. Refactor `Pull` to call a shared unexported `lookup` and `importPack` to share the index download and checks with `Fetch`; `Pull` keeps writing through `PutVerified` and its progress steps.
- [ ] **Step 4: Run** `gofmt -l . ; go vet ./... ; go test ./...` — all pass (pre-existing tests unchanged).
- [ ] **Step 5: Commit** `client: look a reference up and fetch its packs without a packstore`.

### Task A3: Document, pull request, merge, release v0.8.0

**Files:**
- Modify: `README.md` (a short "Using the client from Go" section naming `PushFrom`, `ObjectSource`, `Lookup`, `Fetch`, `Pack`)

- [ ] **Step 1:** Add the README section; commit `README: the client's Go API for other object stores`.
- [ ] **Step 2:** `git push -u origin client-objects-and-fetch`; `gh pr create` with a body describing the two additions and that no command or server behaviour changes.
- [ ] **Step 3:** Wait for the Test workflow on the pull request (`gh pr checks --watch`). Green: `gh pr merge --rebase --delete-branch`. Red: fix on the branch, push, wait again.
- [ ] **Step 4:** `git checkout main && git pull`; wait for the Test workflow on `main` (`gh run watch`); then `git tag v0.8.0 && git push origin v0.8.0` (lightweight tag, as the earlier releases).
- [ ] **Step 5:** Verify the release: `gh run watch` for the release workflow, `gh release view v0.8.0`, and `GOFLAGS=-mod=mod go list -m github.com/amber-store/jaccard-store@v0.8.0` from a scratch directory resolves.

---

# Part B: narrenhaus

Work in `../narrenhaus`. Part B starts only after `v0.8.0` of jaccard-store resolves through the Go module proxy.

**File map**

| Path | Responsibility |
|---|---|
| `nix32/` | Nix's base-32 encoding |
| `nar/` | strict NAR parser and canonical writer, no storage |
| `nartree/` | NAR stream → core objects; wrapper root and recipe; core objects → NAR stream |
| `narinfo/` | parse Nix's narinfo, the stored form, fingerprint, signing key |
| `packcache/` | cache directory: expanded packs, staging files, records, origin, LRU eviction |
| `remote/` | the shared connection to jaccard-store: dial, retire, time limits |
| `testserver/` | an in-process jaccard-store server for tests |
| `refs/` | making a reference local and verified; narinfo and content reads |
| `decompress/` | recognise and undo an upload's compression |
| `upload/` | NAR and narinfo uploads: staging, content work, narinfo work |
| `httpd/` | routes and status codes |
| `nixconf/` | the setup check |
| `cmd/narrenhaus/` | `serve` and `check` |
| `e2e/` | two-instance and real-Nix tests |

### Task B1: Scaffold, flake and `nix32`

**Files:**
- Create: `go.mod`, `go.sum`, `flake.nix`, `.envrc`, `.gitignore`, `LICENSE`, `COPYING`, `README.md`, `cmd/narrenhaus/main.go`, `cmd/narrenhaus/main_test.go`, `nix32/nix32.go`, `nix32/nix32_test.go`

**Interfaces:**
- Produces:
  ```go
  package nix32
  func Encode(b []byte) string           // Nix's base 32, alphabet "0123456789abcdfghijklmnpqrsvwxyz", Nix's digit order
  func Decode(s string) ([]byte, error)  // inverse; rejects characters outside the alphabet and non-canonical input
  func EncodedLen(n int) int             // 32 bytes -> 52, 20 bytes -> 32
  ```
  `cmd/narrenhaus`: an `urfave/cli/v2` app named `narrenhaus` with commands `serve` and `check` and the flags of spec section 10 (names, variables, defaults verbatim). In this task `serve` and `check` validate their options and return `errors.New("not implemented")`; Task B11 fills them in. Option validation lives in `func loadOptions(c *cli.Context) (options, error)`:
  ```go
  type options struct {
      Server    irohkey.EndpointID // parsed with irohkey.ParseEndpointID (github.com/tmc/go-iroh/key)
      Prefix    string             // prefix+"0…0.narinfo" (32 zeros) and prefix+"nar/"+52 zeros+".nar" must pass reference.ValidateName
      Listen    string
      CacheDir  string
      CacheSize uint64             // parsed by parseSize: "10GiB", "500MiB", "1073741824"; zero is an error
      ConfigDir string
      Priority  int
  }
  func parseSize(s string) (uint64, error)
  ```

- [ ] **Step 1: Failing tests.** `nix32`: `TestEncodeKnownHash` (SHA-256 of the empty string encodes to `0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73`), `TestRoundTrip` (random 20- and 32-byte inputs), `TestDecodeRejectsForeignCharacters` (`e`, `o`, `u`, `t`, uppercase). `cmd/narrenhaus`: `TestParseSize` (`"10GiB"` → `10<<30`, `"500MiB"`, `"123"`, errors for `""`, `"0"`, `"ten"`, `"-1GiB"`), `TestLoadOptionsRefusesABadPrefix` (prefix `a@b/` → error mentioning the prefix), `TestLoadOptionsRefusesABadServer`.
- [ ] **Step 2: Run** `go test ./...` — fails (no packages).
- [ ] **Step 3: Implement.** `go mod init`, require `github.com/amber-store/jaccard-store v0.8.0`, `github.com/amber-store/core v0.10.0`, `github.com/urfave/cli/v2` and `github.com/tmc/go-iroh` at the versions jaccard-store v0.8.0 requires; `go mod tidy`. `flake.nix` after jaccard-store's: inputs nixpkgs `nixos-26.05`, `systems`, `gonixgo` `github:draganm/gonixgo/v0.5.0` (following nixpkgs and systems); `packages.default = goEnv.buildGoApplication { pname = "narrenhaus"; src = ./.; subPackages = [ "cmd/narrenhaus" ]; meta.license = pkgs.lib.licenses.lgpl3Only; }`; `devShells.default` with `go` and `self.packages.${system}.default`; `devShells.bare` with `go`. `.envrc` as jaccard-store's (`watch_file go.mod go.sum`, `watch_dir` for each package directory of the file map, `use flake . --option allow-unsafe-native-code-during-evaluation true`). `.gitignore`: `result`, `.direnv`. `LICENSE` and `COPYING` copied from `../jaccard-store`. `README.md`: title and one paragraph (Task B11 completes it).
- [ ] **Step 4: Run** `go test ./... && go vet ./...`; then `git add` the new files (Nix sees only tracked files) and `nix build --no-link --option allow-unsafe-native-code-during-evaluation true` and `nix develop --option allow-unsafe-native-code-during-evaluation true -c narrenhaus --help` — the help lists `serve` and `check`.
- [ ] **Step 5: Commit** `scaffold: module, flake with gonixgo, the command's options, nix32`.

### Task B2: `nar` — strict parser and canonical writer

**Files:**
- Create: `nar/nar.go`, `nar/parse.go`, `nar/write.go`, `nar/nar_test.go`

**Interfaces:**
- Produces:
  ```go
  package nar

  // ErrMalformed wraps every error of Parse that is the archive's fault.
  var ErrMalformed = errors.New("nar: malformed archive")

  // Handler receives the nodes of an archive in order. The root node has
  // the empty name. An error from a method ends Parse and is returned as is.
  type Handler interface {
      // Directory is called when a directory begins; Leave when it ends.
      Directory(name string) error
      Leave() error
      // Regular must read body to its end, size bytes, before returning.
      Regular(name string, executable bool, size uint64, body io.Reader) error
      Symlink(name, target string) error
  }

  // Parse reads one archive from r and nothing after it: bytes after the
  // root node are an error.
  func Parse(r io.Reader, h Handler) error

  // Writer writes a canonical archive. Calls mirror Handler; the caller
  // gives entries in byte order of name and exactly size bytes per file.
  type Writer struct{ /* … */ }
  func NewWriter(w io.Writer) *Writer
  func (w *Writer) Directory(name string) error
  func (w *Writer) Leave() error
  func (w *Writer) Regular(name string, executable bool, size uint64) (io.Writer, error) // body of the file; the next call checks size bytes were written
  func (w *Writer) Symlink(name, target string) error
  func (w *Writer) Close() error // fails unless exactly one root node was written and closed
  ```
  Format and limits: spec 5.1 step 3 and the grammar (`nix-archive-1`, strings as 64-bit little-endian length, bytes, zero padding to 8). Tokens longer than 32 bytes where a keyword is expected are malformed without being read into memory.

- [ ] **Step 1: Failing tests** in `nar/nar_test.go`: `TestRoundTrip` — table of archives built with `Writer` (directory root with files, an executable, a subdirectory, a symlink, an empty directory, an empty file; file root; executable file root; symlink root; names `["B", "a", "é"]` in byte order) parsed with a recording `Handler` and re-written: bytes identical. `TestGolden` — the archive of a single file root with contents `"hello\n"` equals the bytes written out in the test (120 bytes: header, `(`, `type`, `regular`, `contents`, 6-byte body padded, `)`). `TestParseRefuses` — one case per rule, each `errors.Is(err, ErrMalformed)`: wrong magic; unsorted names; duplicate name; empty name; `.`; `..`; name with `/`; name with NUL; name of 256 bytes; empty symlink target; target with NUL; target of 4096 bytes; nesting of 65 levels; `executable` with a non-empty value; non-zero padding; trailing byte after the root; truncated archive; unknown node type; keyword token of 1 MiB. `TestParseAcceptsTheLimits` (name of 255 bytes, target of 4095, nesting of 64). `TestHandlerErrorEndsParse`. `TestWriterRefusesShortFile`.
- [ ] **Step 2: Run** `go test ./nar/` — compile failure.
- [ ] **Step 3: Implement** `Parse` as a recursive descent over a buffered reader that never holds more than one token in memory (file bodies are handed to the handler as an `io.LimitedReader` and the padding is read after), and `Writer` as its mirror.
- [ ] **Step 4: Run** `go test ./nar/ && go vet ./nar/` — pass.
- [ ] **Step 5: Commit** `nar: strict parser and canonical writer`.

### Task B3: `nartree` — NAR ⇄ core objects

**Files:**
- Create: `nartree/ingest.go`, `nartree/root.go`, `nartree/read.go`, `nartree/nartree_test.go`, `nartree/frozen_test.go`

**Interfaces:**
- Consumes: `nar.Parse`, `nar.Handler`, `nar.NewWriter` (Task B2); core `fstree`, `chunkers`, `ingest.DefaultItemBits`, `key`.
- Produces:
  ```go
  package nartree

  // Getter reads an object by key; it must be safe for concurrent calls.
  type Getter func(key.Key) ([]byte, error)

  // Node is the NAR's root node as an entry named "nar".
  type Node = fstree.Entry

  // MaxNARSize is the longest archive narrenhaus takes, in bytes: 64 GiB.
  // An upload gives it to Ingest as the limit, and a reference whose
  // recipe states more is not rebuilt.
  const MaxNARSize = 64 << 30

  // ErrTooLarge is an archive of more bytes than the limit given to Ingest.
  var ErrTooLarge = errors.New("nartree: the archive is too large")

  // Ingest reads one NAR from r, emits its objects and returns its root
  // node with the SHA-256 and length of the bytes read. Errors of the
  // archive wrap nar.ErrMalformed, or ErrTooLarge at the read that takes
  // the archive beyond limit bytes. The limit is the caller's.
  func Ingest(r io.Reader, emit fstree.Emit, limit uint64) (node Node, narHash [32]byte, narSize uint64, err error)

  // File emits the content objects of a file and returns its content key,
  // as core's ingest builds one.
  func File(r io.Reader, emit fstree.Emit) (key.Key, error)

  // Recipe returns version 1 of the recipe for a NAR (spec 4.3).
  func Recipe(narHash [32]byte, narSize uint64) []byte

  // Root emits the recipe and the wrapper directory {nar, recipe} and
  // returns the content reference's root key.
  func Root(node Node, narHash [32]byte, narSize uint64, emit fstree.Emit) (key.Key, error)

  // Content is an opened content reference.
  type Content struct {
      Node    Node
      NarHash [32]byte
      NarSize uint64
  }

  var (
      ErrUnknownRecipe = errors.New("nartree: unknown recipe") // another format or version: do not serve, not broken
      ErrBroken        = errors.New("nartree: broken reference") // not the wrapper of spec 4.2, or an unreadable recipe
  )

  // Open reads the wrapper directory and recipe at root.
  func Open(root key.Key, get Getter) (Content, error)

  // WriteNAR writes the canonical NAR of node to w. It fails, wrapping
  // ErrBroken, if a file's bytes differ in number from its content key's
  // length.
  func WriteNAR(w io.Writer, node Node, get Getter) error
  ```
  Entry fields per spec 4.2. `Root`'s directory is built with `fstree.NewDirBuilder(chunkers.NewItemChunker(ingest.DefaultItemBits))`; so is every directory inside the NAR. `File` follows `core/ingest/driver.go` `buildFile` (read it first: `$GOMODCACHE/github.com/amber-store/core@v0.10.0/ingest/driver.go`), including the empty-file case.

- [ ] **Step 1: Failing tests.** `TestRoundTrip` — for each archive of Task B2's round-trip table: `Ingest` into a `map[key.Key][]byte`, `Root`, then `Open` and `WriteNAR` reproduce the input bytes; `NarHash`/`NarSize` equal `sha256`/`len` of the input; every key of the map is reachable from the root (`fstree.ReachableKeys`) and nothing else is in the map. `TestMultiChunkFile` (an 8 MiB file of seeded pseudo-random bytes round-trips and its content key's type is `key.FileNode`). `TestRecipeBytes` — `Recipe(sha256(""), 120)` equals exactly `{"format":"narrenhaus-recipe","narHash":"sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73","narSize":120,"node":"nar","version":1}`. `TestOpenUnknownRecipeVersion` (`version` 2 → `ErrUnknownRecipe`), `TestOpenNotAWrapper` (a plain directory root, a Blob root, a wrapper with a third entry → `ErrBroken`). `TestModesAsCoreReadsThem` — decode the wrapper and the tree with `fstree.CollectEntries`: modes are exactly `0o100444`, `0o100555`, `0o040555`, `0o120777`; UID and GID 0; Mtime `1_000_000_000`. `TestIngestIsStreaming` — ingest a 64 MiB file root from a generated reader with an `emit` that discards: `testing.AllocsPerRun`-style measurement of `runtime.MemStats.TotalAlloc` shows peak heap (`HeapInuse` sampled in `emit`) under 16 MiB. `TestSameNARSameRoot` (two ingests give one root key). In `frozen_test.go`: `TestFrozenKeys` — the fixed small archive (directory with `file` = `"plain\n"`, executable `run` = `"#!/bin/sh\n"`, subdirectory `sub` holding `inner` = `"x"`, symlink `link` → `file`) gives a hard-coded root key, and the 8 MiB seeded file a hard-coded content key.
- [ ] **Step 2: Run** `go test ./nartree/` — compile failure.
- [ ] **Step 3: Implement.** `Ingest`: a `nar.Handler` holding a stack of `fstree.DirBuilder`s; `r` is wrapped in a `sha256` tee and a counter. For `TestFrozenKeys`, run once, print the two keys, check by reading them back with core that the tree is what the test describes, then paste the keys into the test as constants.
- [ ] **Step 4: Run** `go test ./nartree/ && go vet ./nartree/` — pass.
- [ ] **Step 5: Commit** `nartree: a NAR as core objects and back`.

### Task B4: `narinfo` — parse, stored form, signature

**Files:**
- Create: `narinfo/narinfo.go`, `narinfo/key.go`, `narinfo/narinfo_test.go`, `narinfo/key_test.go`

**Interfaces:**
- Consumes: `nix32` (Task B1).
- Produces:
  ```go
  package narinfo

  const StoreDir = "/nix/store"
  const MaxStored = 1 << 20 // bytes of a stored narinfo

  var ErrMalformed = errors.New("narinfo: malformed")

  type Info struct {
      StorePath   string   // full path under StoreDir
      NarHash     [32]byte // SHA-256
      NarSize     uint64
      References  []string // base names, as given
      Deriver     string   // "" when absent or "unknown-deriver"
      Sigs        []string // "name:base64", as given
      CA          string
      URL         string   // as parsed; Stored ignores it
      Compression string   // as parsed; Stored ignores it
  }

  // Parse reads Nix's narinfo text. Unknown keys are ignored. NarHash is
  // accepted as sha256:<nix32>, sha256:<hex> and SRI sha256-<base64>.
  func Parse(b []byte) (*Info, error)

  func (i *Info) HashPart() string // the 32 characters before the first '-' of the base name
  func (i *Info) NarHashNix32() string // 52 characters
  func (i *Info) NarURL() string       // "nar/<NarHashNix32>.nar"

  // Stored is the text narrenhaus keeps (spec 5.2): URL nar/<N>.nar,
  // Compression none, FileHash and FileSize as the NAR's, Nix's field order.
  func (i *Info) Stored() []byte

  // Signed is Stored with one more Sig line, sig, placed so that the Sig
  // lines stay sorted.
  func (i *Info) Signed(sig string) []byte

  // Fingerprint is what a signature covers:
  // 1;<StorePath>;sha256:<nix32>;<NarSize>;<full reference paths, sorted, comma-joined>
  func (i *Info) Fingerprint() string

  type SigningKey struct{ /* name, ed25519 private key */ }
  // LoadOrCreateSigningKey reads path ("<name>:<base64 of 64 bytes>"), or
  // makes a key named "narrenhaus-<first 8 hex digits of the public key>"
  // and writes it with mode 0600 (parent directories 0700).
  func LoadOrCreateSigningKey(path string) (SigningKey, error)
  func (k SigningKey) Name() string
  func (k SigningKey) Public() string            // "<name>:<base64 of the 32-byte public key>"
  func (k SigningKey) Sign(fingerprint string) string // "<name>:<base64 of the 64-byte signature>"
  ```
  Parse rules (spec 5.3 step 1 and the recon notes): each line is `Key: value`; `StorePath`, `NarHash`, `NarSize` required; `NarSize` non-zero; a second `References` or `CA` line is an error; `StorePath` must be `StoreDir + "/" + <32 nix32 chars> + "-" + <name>`.

- [ ] **Step 1: Failing tests.** `TestParseWhatNixWrites` (a real narinfo text with xz URL, two references, deriver, one `cache.nixos.org-1` signature); `TestParseNarHashForms` (nix32, hex, SRI give the same `NarHash`; `md5:` and `sha512:` → `ErrMalformed`); `TestStored` — exact expected text for that narinfo: lines `StorePath`, `URL: nar/<52>.nar`, `Compression: none`, `FileHash: sha256:<52>`, `FileSize`, `NarHash: sha256:<52>`, `NarSize`, `References: a b`, `Deriver`, `Sig`, each ended by `\n`; `TestStoredEmptyReferences` (line is `References: ` with the trailing space); `TestStoredOmitsUnknownDeriver`; `TestStoredParsesBackEqual`; `TestFingerprint` (references given unsorted come out sorted and prefixed with `/nix/store/`); `TestSigned` (lines sorted; verifying the added signature with `ed25519.Verify` over `Fingerprint()` succeeds); `TestParseRefuses` (missing StorePath; StorePath outside `/nix/store`; hash part of 31 characters; NarSize 0; two References lines); `TestParseIgnoresUnknownKeys` (`System: x86_64-linux`). `key_test.go`: `TestCreateThenLoad` (same key; file mode 0600; name matches `^narrenhaus-[0-9a-f]{8}$` and equals the first 8 hex digits of the public key), `TestPublicFormat`, `TestLoadRefusesGarbage`.
- [ ] **Step 2: Run** `go test ./narinfo/` — compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./narinfo/ && go vet ./narinfo/` — pass. Where `nix` is on `PATH`, `TestNixAcceptsTheSignature` (skips otherwise) writes the key's public half and the signed narinfo to a temp dir served by `httptest` and runs `nix path-info --store <url> --option trusted-public-keys <public> <store path>` with `XDG_CACHE_HOME` fresh, `NIX_USER_CONF_FILES=` and `NIX_CONFIG` unset; it must succeed.
- [ ] **Step 5: Commit** `narinfo: parse, the stored form, fingerprint and signing key`.

### Task B5: `packcache` — pack files, records, eviction

**Files:**
- Create: `packcache/cache.go`, `packcache/writer.go`, `packcache/hold.go`, `packcache/evict.go`, `packcache/records.go`, `packcache/packcache_test.go`

**Interfaces:**
- Consumes: `packfile.NewIndex`, `packfile.ParseIndex`, `packfile.Entry` (jaccard-store), core `key`.
- Produces:
  ```go
  package packcache

  type Origin struct{ Server, Prefix string }

  var (
      ErrOrigin  = errors.New("packcache: directory belongs to another server or prefix")
      ErrForeign = errors.New("packcache: directory is not a cache directory") // no origin, and not empty either
      ErrInUse   = errors.New("packcache: directory is in use by another process")
      ErrAbsent  = errors.New("packcache: no such object")
      ErrLimit   = errors.New("packcache: beyond the limits of a pack") // Writer.Add: no failed write, and no repeat cures it
  )

  // Open opens or creates the cache directory of spec 7.1, takes an
  // exclusive lock on it (a flock on <dir>/lock), removes packs without
  // .meta and everything under tmp/ and staging/, and recreates missing
  // subdirectories. limit is the size limit in bytes. A directory in use
  // (ErrInUse), of another origin (ErrOrigin) or without an origin and not
  // empty (ErrForeign, spec 7.1) is refused and left as it was.
  func Open(dir string, limit uint64, origin Origin) (*Cache, error)
  func (c *Cache) Close() error
  func (c *Cache) TempDir() string // <dir>/tmp
  func (c *Cache) Size() uint64    // sum of .data and .idx sizes under packs/

  // Writer appends objects, each key once, to a flat file under staging/
  // (NewStaged) or tmp/ (NewDownload).
  func (c *Cache) NewStaged() (*Writer, error)
  func (c *Cache) NewDownload() (*Writer, error)
  func (w *Writer) Add(k key.Key, data []byte) error // a key already added is skipped; an object beyond what a pack may hold (a key that is not canonical, more than 256 MiB, one more than 2^24 objects) is refused with ErrLimit
  func (w *Writer) Discard()
  // Seal ends writing, writes the index beside the data and returns the
  // file as a readable set of objects.
  func (w *Writer) Seal() (*Sealed, error)

  // Sealed is a finished file not yet in packs/. Get is safe for
  // concurrent calls (positional reads) and returns a fresh slice.
  type Sealed struct{ /* … */ }
  func (s *Sealed) Get(k key.Key) ([]byte, error) // ErrAbsent
  func (s *Sealed) Has(k key.Key) bool
  func (s *Sealed) Bytes() uint64 // uncompressed object bytes
  func (s *Sealed) Discard()

  // Place moves s into packs/ under root: as a complete pack when parent
  // is nil, as a patch naming *parent otherwise. .meta is written last.
  // A pack already present under root is kept and s is discarded.
  // Place stamps the pack and runs eviction.
  func (c *Cache) Place(s *Sealed, root key.Key, parent *key.Key) error

  type Meta struct {
      Complete bool
      Parent   key.Key // valid when !Complete
  }
  func (c *Cache) Meta(root key.Key) (Meta, bool)
  func (c *Cache) Drop(root key.Key) // deletes the pack and the records whose Root it is; waits for no one: held packs are deleted when released

  // Acquire holds the packs of roots (own pack first, then its parent)
  // and stamps them. ok is false, and nothing is held, unless all are
  // present.
  func (c *Cache) Acquire(roots ...key.Key) (h *Hold, ok bool)
  // Reserve holds roots that may not be present yet, so that a pack placed
  // for them is not evicted before the holder has used it.
  func (c *Cache) Reserve(roots ...key.Key) *Hold
  func (h *Hold) Get(k key.Key) ([]byte, error) // own pack, then parent; ErrAbsent
  func (h *Hold) Has(k key.Key) (bool, error)
  func (h *Hold) Release() // idempotent; runs eviction

  // Record is what refs/ keeps per reference name.
  type Record struct {
      Root    key.Key
      Packs   []key.Key // own pack first, then its parent
      Checked bool      // a content reference whose NAR was rebuilt and compared
  }
  func (c *Cache) Record(name string) (Record, bool)
  func (c *Cache) PutRecord(name string, r Record) error
  func (c *Cache) DeleteRecord(name string)
  ```
  Stamps are the modification time of `packs/<root hex>.data` (`os.Chtimes`), never access time. Eviction: spec 7.3. Record file names are the SHA-256 hex of the reference name plus `.json` (names contain `/`).

- [ ] **Step 1: Failing tests.** `TestStagedObjectsReadBack` (Add three objects, one twice; `Seal`; `Get` each; `Bytes()` is the sum of distinct objects; the `.idx` parses with `packfile.ParseIndex` and tiles the data); `TestConcurrentGet` (64 goroutines, race detector); `TestPlaceAndAcquire` (Place as complete; Acquire; `Get`; a second `Open` of the directory sees the pack); `TestPatchReadsThroughItsParent` (Acquire(patch, parent): an object only in the parent is found; Acquire(patch) alone with the parent absent is `ok == false`); `TestEvictionOrder` (limit fits two of three packs; stamp order decides; the oldest unheld pack goes with its `.idx`, `.meta` and its records); `TestHeldPackIsNotEvicted` (over the limit while held; evicted on `Release`); `TestUsingAPatchStampsItsParent`; `TestReserveKeepsAPlacedPack`; `TestDropWhileHeld` (readable until `Release`, gone after); `TestRecoveryAfterAKill` (a pack without `.meta`, files under `tmp/` and `staging/`: removed by `Open`; a complete pack survives); `TestOrigin` (second `Open` with another server or prefix → `ErrOrigin`, message contains the directory and both values); `TestOpenRefusesADirectoryInUse` (`ErrInUse` while the first `Cache` is open; succeeds after `Close`); `TestOpenRecreatesItsDirectories` (remove `tmp/` and `refs/` by hand, `Open` succeeds and they exist; a directory made read-only gives an error naming it; skip as root); `TestRecords` (put, get, delete; a name with `/`).
- [ ] **Step 2: Run** `go test ./packcache/` — compile failure.
- [ ] **Step 3: Implement.** One mutex guards the pack table, hold counts and sizes; file reads happen outside it through `os.File.ReadAt`.
- [ ] **Step 4: Run** `go test -race ./packcache/ && go vet ./packcache/` — pass.
- [ ] **Step 5: Commit** `packcache: expanded packs, records, least-recently-used eviction`.

### Task B6: `testserver` and `remote` — the shared connection

**Files:**
- Create: `testserver/testserver.go`, `remote/remote.go`, `remote/remote_test.go`

**Interfaces:**
- Consumes: jaccard-store `client.New`, `client.Opener`, `client.Lookup`, `client.Fetch`, `client.PushFrom`, `client.ObjectSource`, `client.Pack`, `node.Dial`, `node.DialConfig`, `wire.ALPN`, `wire.Error`, `wire.CodeBadRequest`, `wire.CodeMalformedPack` (check the exact constant names in `wire/wire.go`); for `testserver`: jaccard-store `server`, `db`, `node`, `bucket/buckettest`, following `../jaccard-nfs-nix-store/jstest/jstest.go` (copy the pattern; do not import that module).
- Produces:
  ```go
  package testserver

  // Server is a jaccard-store server in this process: a database in a
  // temporary directory, a bucket in memory, iroh on loopback. A test that
  // calls Start cannot be parallel (the bucket sets environment variables).
  type Server struct{ /* … */ }
  type Options struct {
      NoVerify     bool
      MaxPackBytes uint64 // 0 means the server's default
  }
  func Start(t testing.TB, opts Options) *Server
  // Dial is a remote.Config.Dial for this server, with a fresh client key.
  func (s *Server) Dial() func(ctx context.Context) (remote.Conn, error)
  // Client is a plain jaccard-store client, for tests that write or delete references directly.
  func (s *Server) Client(t testing.TB) *client.Client
  // Stop ends the server; Restart brings it back on the same address and data.
  func (s *Server) Stop()
  func (s *Server) Restart()
  // DropConnections closes every client connection from the server's side.
  func (s *Server) DropConnections()
  // Forget makes the server lose every client connection without a word to
  // its client: the endpoint goes away with its connections as they are,
  // and a new one answers at the same address. On a stopped server it does
  // nothing.
  func (s *Server) Forget()
  // SlowBucket makes every bucket transfer wait d before its first byte; zero undoes it.
  func (s *Server) SlowBucket(d time.Duration)
  ```
  ```go
  package remote

  // Conn is one connection to the server.
  type Conn interface {
      client.Opener
      Close() error
  }

  type Config struct {
      Dial          func(ctx context.Context) (Conn, error)
      HTTP          *http.Client  // bucket transfers; nil means one whose transfers fail after 30 s without a byte
      LookupTimeout time.Duration // 0 means 10 s: connection plus lookup round trip
      // RepeatAfter is how long a lookup waits for its answer on a
      // connection that was up before it; then it is repeated on a fresh
      // one. It is also the least a lookup must have waited on a connection
      // for its silence to retire it. 0 means 3 s, or three tenths of
      // LookupTimeout where that is less; never more than half of
      // LookupTimeout.
      RepeatAfter   time.Duration
      DialTimeout   time.Duration // 0 means 30 s
      FailFast      time.Duration // 0 means 30 s after a failed dial
      Log           *slog.Logger
  }

  // ErrUnreachable is returned, wrapped, when the server could not be asked.
  var ErrUnreachable = errors.New("remote: server unreachable")

  type Remote struct{ /* … */ }
  func New(cfg Config) *Remote
  func (r *Remote) Close()

  // Reachable returns nil when a connection is up or can be dialled within
  // LookupTimeout, and an error wrapping ErrUnreachable otherwise.
  func (r *Remote) Reachable(ctx context.Context) error

  // Lookup is client.Lookup within LookupTimeout. client.ErrNotFound for an
  // unknown name; ErrUnreachable when the server could not be asked. On a
  // connection that was up before it, a lookup waits RepeatAfter for its
  // answer; without one, or when that connection fails it or is retired
  // meanwhile, it is repeated once on a fresh one within the same limit.
  func (r *Remote) Lookup(ctx context.Context, name string) (key.Key, []client.Pack, error)

  // Fetch is client.Fetch; it has no time limit of its own.
  func (r *Remote) Fetch(ctx context.Context, p client.Pack, each func(packfile.Object) error) (*packfile.Index, error)

  // Push is client.PushFrom; it has no time limit of its own.
  func (r *Remote) Push(ctx context.Context, objects client.ObjectSource, name string, root key.Key, opts client.PushOptions) (client.PushResult, error)

  // ForGood reports whether a push failed for a reason a repeat cannot
  // cure: the server answered bad_request or malformed_pack.
  func ForGood(err error) bool
  ```
  Connection rules: spec 9.3 verbatim (retire on stream-open or frame failure or a lookup's wait in vain, by the rule of `RepeatAfter`; never for `ErrNotFound`, a `*wire.Error`, a bucket error or `client.ErrBadPack`; only the first failing call retires; closed when its last call returns; dials serialised, run under `Remote`'s own context, fail-fast window after a failed dial). The default `HTTP` client enforces the 30-second stall rule with a transport whose response bodies fail when a read gets no byte for 30 s.

- [ ] **Step 1: Failing tests** in `remote/remote_test.go` against `testserver`: `TestLookupNotFound`; `TestLookupAfterTheConnectionDied` (`DropConnections`, then a lookup of an existing name succeeds: one redial); `TestLookupWhenTheServerIsDown` (`Stop`: error wraps `ErrUnreachable` within `LookupTimeout` set to 500 ms; a second lookup inside `FailFast` returns in under 50 ms); `TestRecoversAfterFailFast` (`Restart`, wait out a 200 ms `FailFast`, lookup succeeds); `TestRefusalDoesNotRetire` (a `Push` the server refuses with `bad_request` — a name with `@` — leaves a concurrent slow `Push` running to success; `ForGood` is true for the refusal); `TestRetiredConnectionOutlivesItsCalls` (a `Push` in flight, slowed by `SlowBucket`, while another call's lookup times out: the push still finishes); `TestDialsAreSerialised` (20 concurrent lookups on a fresh `Remote`: `Dial` was called once); `TestReachable`; `TestStalledTransferFails` (default HTTP client, a bucket that sends headers and then nothing: `Fetch` fails within the stall limit, set to 300 ms through an unexported knob).
- [ ] **Step 2: Run** `go test ./remote/` — compile failure.
- [ ] **Step 3: Implement** `testserver` first (it is test support with no tests of its own beyond use), then `remote`.
- [ ] **Step 4: Run** `go test -race ./remote/ && go vet ./remote/ ./testserver/` — pass.
- [ ] **Step 5: Commit** `remote: one shared connection to the server, with its time limits`.

### Task B7: `refs` — making a reference local

**Files:**
- Create: `refs/refs.go`, `refs/fetch.go`, `refs/narinfo.go`, `refs/content.go`, `refs/refs_test.go`

**Interfaces:**
- Consumes: `packcache` (B5), `remote` (B6), `nartree.Open`, `nartree.WriteNAR`, `nartree.ErrUnknownRecipe`, `nartree.ErrBroken` (B3), `narinfo.Parse`, `narinfo.MaxStored` (B4), `nix32`, core `fstree.CheckComplete`, `fstree.WriteContent`, `key`.
- Produces:
  ```go
  package refs

  type Options struct {
      Prefix           string
      ContentDownloads int // 0 means 8
      NarinfoDownloads int // 0 means 16
      Log              *slog.Logger
  }

  var (
      // ErrNotFound: no such reference, or one that must not be served
      // (broken, unknown recipe, transfer failure, server unreachable and
      // nothing local). The HTTP answer is 404; the log says which.
      ErrNotFound = errors.New("refs: not found")
  )

  type Store struct{ /* … */ }
  // New returns a store whose downloads and checks run under ctx, not
  // under the contexts of the calls that start them.
  func New(ctx context.Context, cache *packcache.Cache, rem *remote.Remote, opts Options) *Store

  // NarinfoName and ContentName are the reference names of spec 4.1.
  func (s *Store) NarinfoName(storeHash string) string
  func (s *Store) ContentName(narHash string) string

  // Narinfo returns the stored narinfo of the store path with hash part
  // storeHash (spec 6.2). ctx bounds the wait, not the work.
  func (s *Store) Narinfo(ctx context.Context, storeHash string) (*narinfo.Info, error)

  // HasNarinfo answers HEAD on a narinfo: a lookup and the pre-checks of
  // spec 6.2 step 3, no download.
  func (s *Store) HasNarinfo(ctx context.Context, storeHash string) (bool, error)

  // Content makes the content reference of narHash local and checked
  // (spec 6.3 steps 1 and 2) and returns it held. ctx bounds the wait.
  func (s *Store) Content(ctx context.Context, narHash string) (*Content, error)

  type Content struct{ /* … */ }
  func (c *Content) NarSize() uint64
  // WriteTo writes the NAR; it fails if the bytes written would differ
  // from NarSize.
  func (c *Content) WriteTo(w io.Writer) (int64, error)
  func (c *Content) Release()

  // PlaceUploaded records a pack an upload has just placed in the cache as
  // the local, checked copy of the reference name with root.
  func (s *Store) PlaceUploaded(name string, root key.Key) error
  ```
  Behaviour, all from the spec: pre-checks before any download (narinfo root is a Blob or FileNode key with `Length() <= narinfo.MaxStored`, announced pack bytes total at most 4 MiB; content root is a DirLeaf or DirNode key); one download per root, joined by later callers, running under the store's context; a download writes through `packcache.NewDownload`, `Fetch`, `Seal`, `Place`; a bucket answer of 403 looks the reference up again once; after the last pack `fstree.CheckComplete`; for content, the once-per-root check (recipe, rebuild, compare SHA-256 and length with the recipe and with the name), recorded as `Record.Checked`; narinfo checks of spec 6.2 step 4 (`HashPart() == storeHash`, `URL == NarURL()`, `Compression == "none"`); the cached-pack presence rule of spec 7.2; broken references remembered per name and root for the life of the process and their packs dropped; offline answers from the record and its packs. A caller whose `ctx` ends gets `ErrNotFound` wrapping the context's error while the work goes on.

- [ ] **Step 1: Failing tests** against `testserver`, writing references with a helper that builds them through `nartree`/`narinfo` and `remote.Push`: `TestNarinfoFetchesNoContent` (after `Narinfo`, the cache holds one pack and the bucket served no data object of the content reference — count bucket requests through the server's HTTP middleware or compare `Cache.Size()` with the narinfo's size); `TestNarinfoNotFound`; `TestNarinfoOffline` (fetch once, `Stop`, fetch again: served; another hash: `ErrNotFound`); `TestHasNarinfoDownloadsNothing`; `TestNarinfoNamePointingAtADirectory` (a `.narinfo` name pushed with a 5 MiB directory root: `ErrNotFound`, cache empty, and `HasNarinfo` false); `TestNarinfoForAnotherPath` (text whose StorePath hash differs from the name: `ErrNotFound`, remembered: a second call makes no download); `TestContentRoundTrip` (NAR bytes out equal NAR bytes in, `NarSize` right); `TestContentOfAPatch` (two similar trees; the second is a patch; both packs cached; reading works); `TestContentWhoseTreeDoesNotMatchItsRecipe` (a recipe with another `narHash`: `ErrNotFound`, packs dropped); `TestContentWithACorruptObject` (server run with `NoVerify`, a pack with one object's bytes flipped: `ErrNotFound`, nothing kept); `TestUnknownRecipeVersionIsNotBroken` (`ErrNotFound`, the packs stay cached); `TestConcurrentCallsShareOneDownload` (16 callers, the data object is requested once); `TestWaitEndsWorkGoesOn` (`SlowBucket(500ms)`, a caller with a 50 ms context gets `ErrNotFound`; a later call succeeds without a second download); `TestNarinfoIsNotBehindContentDownloads` (`ContentDownloads: 1`, one slowed content download running: a narinfo call returns before it ends); `TestCachedPatchWhoseParentChanged`; `TestExpiredURLIsLookedUpAgain` (bucket answers 403 once); `TestNarinfoWithAFileNodeRoot` (a narinfo of about 900 KiB of varied references whose content key is a `key.FileNode`; if the chunker leaves it whole, grow the text until it splits, staying under 1 MiB); `TestPlaceUploaded`.
- [ ] **Step 2: Run** `go test ./refs/` — compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test -race ./refs/ && go vet ./refs/` — pass.
- [ ] **Step 5: Commit** `refs: references made local and verified before they are served`.

### Task B8: `decompress` and `upload` — NAR and narinfo uploads

**Files:**
- Create: `decompress/decompress.go`, `decompress/decompress_test.go`, `upload/upload.go`, `upload/content.go`, `upload/narinfo.go`, `upload/queue.go`, `upload/upload_test.go`

**Interfaces:**
- Consumes: `nartree.Ingest`, `nartree.Root`, `nartree.File`, `nartree.Open`, `nartree.WriteNAR` (B3); `narinfo` (B4); `packcache` (B5); `remote` (B6); `refs.Store.NarinfoName`, `ContentName`, `PlaceUploaded` (B7); `nix32`.
- Produces:
  ```go
  package decompress

  var ErrUnsupported = errors.New("decompress: unsupported compression")

  // Reader recognises r's compression by its first bytes (xz, zstd, gzip,
  // bzip2, or an uncompressed NAR's header) and returns the decompressed
  // stream. zstd input may be several frames. compressed says whether r
  // was compressed at all. A body that asks its reader for more memory
  // than one of Nix does, an xz dictionary above 64 MiB or a zstd window
  // above 128 MiB, fails when out is read.
  func Reader(r io.Reader) (out io.Reader, compressed bool, err error)
  ```
  ```go
  package upload

  // Error is a failed upload request with the HTTP status to answer.
  type Error struct {
      Status int    // 400, 415, 422, 502, 507
      Msg    string // plain text, under 1024 bytes
  }
  func (e *Error) Error() string

  type Options struct {
      ContentPushes int     // 0 means 4
      NarinfoPushes int     // 0 means 8
      MinDedup      float64 // 0 means 0.5
      IdleExpiry    time.Duration // 0 means 15 minutes
      Log           *slog.Logger
  }

  type Uploader struct{ /* … */ }
  // New returns an uploader whose work runs under ctx.
  func New(ctx context.Context, cache *packcache.Cache, rem *remote.Remote, store *refs.Store, opts Options) *Uploader

  // PutNAR handles PUT /nar/<urlHash>.nar[.<ext>] (spec 5.1 steps 1-7).
  // urlHash is the hash in the URL; it is compared with the NAR's hash
  // only when the body turns out to be uncompressed.
  func (u *Uploader) PutNAR(ctx context.Context, urlHash string, body io.Reader) error

  // HeadNAR answers HEAD /nar/<narHash>.nar for an extension-less URL:
  // true when content work for the hash is queued or running or the
  // server has the reference. An *Error with Status 502 when the server
  // could not be asked.
  func (u *Uploader) HeadNAR(ctx context.Context, narHash string) (bool, error)

  // PutNarinfo handles PUT /<storeHash>.narinfo (spec 5.3).
  func (u *Uploader) PutNarinfo(ctx context.Context, storeHash string, body []byte) error

  // Idle reports whether no upload request is being handled and no push
  // is queued or running; Sweep deletes staged NARs once Idle has held
  // for IdleExpiry. The command calls Sweep on a timer.
  func (u *Uploader) Sweep(now time.Time)
  ```
  State, order, failure classes, statuses and the outcome table: spec section 5, "Work", "State", "Order", "Failure" and 5.1–5.3 verbatim. Content is pushed with `client.PushOptions{MinDedup: 0.5, TempDir: cache.TempDir()}`; a narinfo with `MinDedup: 2`. After a content push the staged file is `Place`d as a complete pack and `store.PlaceUploaded` is called; likewise for the narinfo pack. The queue is its own small type in `queue.go`: `push(n *work)`, `promote(n *work)` (mark as asked for by a narinfo; sticky), `next() *work` (asked-for first, each kind in order).

- [ ] **Step 1: Failing tests.** `decompress`: `TestRecognises` (xz, zstd, gzip, bzip2 fixtures of one small NAR made in the test with the Go libraries or checked-in bytes; and the plain NAR, `compressed == false`); `TestZstdOfSeveralFrames`; `TestUnsupported` (brotli-like bytes, an empty body → `ErrUnsupported`). `upload` against `testserver`: `TestUploadThenBothReferencesExist` (PutNAR then PutNarinfo; `Lookup` finds both names; the narinfo's text is `Stored()`); `TestNARPutAnswersBeforeThePush` (`SlowBucket`: `PutNAR` returns while the push runs); `TestAllNARsBeforeAnyNarinfo` (five NARs staged behind a slowed push with `ContentPushes: 1`; `PutNarinfo` for the last one staged: its content is pushed second, before the three nobody asked for — observe the server's reference creation order); `TestTwoStorePathsOneContent` (one content reference, two narinfo references); `TestNarinfoJoinsARunningPush` (two `PutNarinfo` calls for one hash: one push); `TestFirstWriterWins` (a narinfo reference written beforehand with another NarHash: `PutNarinfo` succeeds and the reference is unchanged); `TestNarinfoWithoutNAR` (`*Error` 400, message contains the NAR hash); `TestNarSizeMismatch` (400); `TestUncompressedURLHashMismatch` (400); `TestGzipToAnExtensionlessURL` (accepted; the content reference is named by the NAR's hash; `HeadNAR(urlHash)` is false before and after); `TestMalformedNAR` (400, staging empty); `TestUnsupportedCompression` (415); `TestServerDown` (`PutNAR` → 502 and the body was not read — a reader that records reads; `HeadNAR` → 502; staging empty); `TestPushFailsForNowThenARepeatSucceeds` (`Stop` after staging, `PutNarinfo` → 502 and the NAR is still staged; `Restart`; `PutNarinfo` → nil); `TestPushFailsForGood` (a server started with `testserver.Options{MaxPackBytes: 1 << 10}`: `PutNarinfo` → 422 with the server's message and `HeadNAR` false afterwards); `TestHeadForQueuedWork` (true while queued); `TestHeadForAReferenceOnTheServer`; `TestNarinfoOverOneMiB` (400); `TestFrozenNarinfoRoot` (a fixed narinfo text gives a hard-coded narinfo root key, found by running once and pasting it in); `TestNarinfoContentEncodingIsNotOurs` is in B9. `TestSweep` (staged NAR with failed-for-now work is deleted after `IdleExpiry` of idleness and its entry is gone: `PutNarinfo` → 400; not deleted while a push runs); `TestStagingWriteFailure` (a cache directory whose `staging/` is read-only: 507; skip as root).
- [ ] **Step 2: Run** `go test ./decompress/ ./upload/` — compile failure.
- [ ] **Step 3: Implement** `decompress` (klauspost zstd and gzip, `ulikunitz/xz`, stdlib bzip2; magic bytes: xz `FD 37 7A 58 5A 00`, zstd `28 B5 2F FD`, gzip `1F 8B`, bzip2 `42 5A 68`, NAR `0D 00 00 00 00 00 00 00` + `nix-archive-1`), then `upload`.
- [ ] **Step 4: Run** `go test -race ./decompress/ ./upload/ && go vet ./decompress/ ./upload/` — pass.
- [ ] **Step 5: Commit** `upload: NARs staged and pushed as they arrive, narinfos after their content`.

### Task B9: `httpd` — the binary-cache surface, and two-instance tests

**Files:**
- Create: `httpd/httpd.go`, `httpd/httpd_test.go`, `e2e/e2e_test.go`, `e2e/harness_test.go`

**Interfaces:**
- Consumes: `refs.Store` (B7), `upload.Uploader`, `upload.Error` (B8), `narinfo.SigningKey` (B4).
- Produces:
  ```go
  package httpd

  type Config struct {
      Store     *refs.Store
      Uploader  *upload.Uploader
      Key       narinfo.SigningKey
      Priority  int
      ServeWait time.Duration // 0 means 240 s: how long a narinfo or NAR request waits for its reference
      // Host is the host the cache listens at. When it is a name, requests
      // that call the cache by it are answered; requests that call it by an
      // IP address or localhost always are.
      Host      string
      Log       *slog.Logger
  }
  func New(cfg Config) http.Handler
  ```
  Before any route, a request that is a web page's by spec 6.4 (a `Host` that is neither an IP address, `localhost` nor `Config.Host`; an `Origin` on a `PUT`; a `Sec-Fetch-Site` of another site) is refused: a `PUT` with 403, anything else with 404.

  Routes (anything else is 404; a `PUT` to anything else is 405):

  | Method and path | Answer |
  |---|---|
  | `GET`/`HEAD /nix-cache-info` | spec 6.1, `Content-Type: text/x-nix-cache-info` |
  | `GET /<32>.narinfo` | `Store.Narinfo` within `ServeWait`; body `info.Signed(Key.Sign(info.Fingerprint()))`, `text/x-nix-narinfo`; `ErrNotFound` → 404 |
  | `HEAD /<32>.narinfo` | `Store.HasNarinfo`; 200 or 404 |
  | `PUT /<32>.narinfo` | a `Content-Encoding` header → 415; body read up to 1 MiB + 1; `Uploader.PutNarinfo` |
  | `GET /nar/<52>.nar` | `Store.Content` within `ServeWait`; `Content-Length: NarSize`, `application/x-nix-nar`; then `WriteTo`; a write error after the header aborts the connection (`panic(http.ErrAbortHandler)`) and is logged |
  | `HEAD /nar/<52>.nar` | `Uploader.HeadNAR`; 200, 404 or 502; no `Content-Length` |
  | `HEAD`/`GET /nar/<52>.nar.<ext>` | 404 |
  | `PUT /nar/<52>.nar` and `PUT /nar/<52>.nar.<ext>` | `Uploader.PutNAR(urlHash, body)` |

  An `*upload.Error` is answered with its status and message; any other error of an upload is 500 and logged. The `Content` is released when the handler returns, however it returns.

- [ ] **Step 1: Failing tests.** `httpd_test.go` (handler level, with `testserver`): `TestCacheInfo` (exact body for priority 50); `TestNarinfoIsSignedAtServeTime` (the body parses, has the stored `Sig` lines plus one by the key, which verifies); `TestNarinfoHead`; `TestNarinfoPutWithContentEncoding` (415); `TestUnknownPaths` (`/log/x`, `/abc.ls`, `/realisations/x.doi`, `/debuginfo/x` → 404; `PUT /abc.ls` → 405); `TestNARGet` (`Content-Length`, body hashes to the name); `TestNARHeadHasNoContentLength`; `TestNARGetNotFound`; `TestCompressedNARURLIsNeverServed`; `TestUploadErrorsCarryTheirStatus`; `TestAbandonedNARDownloadReleasesItsPacks` (a client that reads 1 KiB of a large NAR and closes: afterwards a cache over its limit evicts that pack — `Cache.Size()` falls); `TestServeWaitAnswers404` (`ServeWait: 100ms`, `SlowBucket(1s)`: 404 for the NAR; one second later a second request is 200). `e2e/e2e_test.go` with a harness that starts a `testserver` and two full narrenhaus instances (each: own cache dir, key, `httptest.Server` over `httpd.New`, one prefix): `TestUploadOnOneFetchOnTheOther` (PUT NAR (xz) and narinfo on A over HTTP exactly as Nix does; GET narinfo and NAR on B; NAR bytes equal; B's narinfo is signed by B's key; B fetched no content pack for the narinfo); `TestNixOrderOfTwoPointThirtyFive` (all NARs of five paths with references, then their narinfos concurrently; all 200; B substitutes all); `TestPatchAcrossInstances` (a second version of a large tree uploaded on A is a patch — check with the server's client — and B serves it); `TestConnectionKilledBetweenRequests` (`DropConnections` between two narinfo GETs on B: both 200); `TestServerDownAnswers` (`Stop`: narinfo GET of an uncached path on B is 404 within 11 s; NAR PUT on A is 502).
- [ ] **Step 2: Run** `go test ./httpd/ ./e2e/` — compile failure.
- [ ] **Step 3: Implement** `httpd` with `net/http`'s method-and-pattern mux and explicit length checks of the hash segments (32 and 52 characters of the nix32 alphabet).
- [ ] **Step 4: Run** `go test -race ./httpd/ ./e2e/ && go vet ./httpd/ ./e2e/` — pass.
- [ ] **Step 5: Commit** `httpd: the binary cache's routes; end-to-end tests with two instances`.

### Task B10: `nixconf` — the setup check

**Files:**
- Create: `nixconf/nixconf.go`, `nixconf/nixconf_test.go`, `nixconf/testdata/` (captured JSON)

**Interfaces:**
- Produces:
  ```go
  package nixconf

  // Env is what the check reads from the machine; tests replace it.
  type Env struct {
      // Run runs nix with args and extra environment (entries "K=V"; an
      // entry "K" without '=' removes K) and returns standard output.
      Run func(ctx context.Context, env []string, args ...string) ([]byte, error)
      // ReadFile and Readlink read /etc/nix files.
      ReadFile func(path string) ([]byte, error)
      Readlink func(path string) (string, error)
      Home     string // the user's home directory
      // ConfigHome is XDG_CONFIG_HOME, under which Nix looks for the user's
      // nix/nix.conf. Empty means Home/.config.
      ConfigHome string
      // UserConfFiles is NIX_USER_CONF_FILES, and UserConfFilesSet whether
      // the variable is set at all. Nix then reads the user's settings from
      // the files it names, separated by colons, the first having the last
      // word, and from none under ConfigHome.
      UserConfFiles    string
      UserConfFilesSet bool
  }
  // System is the Env of the real machine.
  func System() Env

  type Input struct {
      Listen    string // the --listen value
      PublicKey string // "<name>:<base64>"
  }

  type Report struct {
      Configured bool
      // Text is what to print: one line when configured, otherwise the
      // missing lines, where they go and why; always the upload hint.
      Text string
  }

  // URL is the substituter URL for a listen address: "http://" + address,
  // with an unspecified host replaced by 127.0.0.1.
  func URL(listen string) string

  func Check(ctx context.Context, env Env, in Input) Report
  ```
  Every rule of spec section 8 (steps 1–7 and 8.1): the three conditions; the trusted field as `true`/`false`/`1`/`0`/missing; the daemon's view via `NIX_USER_CONF_FILES=` and `NIX_CONFIG` removed for an untrusted user, while `fallback` is always read from the full view; substituter comparison after dropping a trailing slash and `?parameters`; the conflicting-key case; where the lines go, first case that applies; the restart note; the upload hint `nix copy --to '<URL>?compression=none' <path>` and the `--option stalled-download-timeout <seconds>` note; the non-loopback warning; the fallback when `nix` cannot be run (`nix show-config --json` is tried when `config show` fails). The `fallback = true` advice carries one sentence: it lets Nix carry on when narrenhaus is not running, and it makes a failed download from any cache a local build.

- [ ] **Step 1: Failing tests** with a fake `Env` fed from `testdata/` (capture `nix --extra-experimental-features nix-command config show --json` and `store info --json` on this machine once, then edit copies for each case): `TestConfigured` (one line, `Configured`); `TestConfiguredInSystemFileSaysRestart`; `TestMissingEverythingTrustedUser` (three lines; place `~/.config/nix/nix.conf`); `TestUntrustedPlainNixConf` (`/etc/nix/nix.conf`, restart note; `fallback` line may go in the user file); `TestGeneratedNixConf` (`Readlink` into `/nix/store`: prints `nix.settings.extra-substituters = [ "…" ];` and `nix.settings.extra-trusted-public-keys = [ "…" ];`); `TestDeterminate` (`include nix.custom.conf` and `!include` forms); `TestUntrustedUserFileIsNotCounted` (lines present only in the user's view of an untrusted user → not configured); `TestFallbackOff` (only that line is missing); `TestTrustedEncodings` (`true`, `1`, `false`, `0`, missing field, command failure); `TestSubstituterMatching` (trailing slash, `?priority=30`, `?trusted=1` match; another port does not); `TestConflictingKeyOfTheSameName` (not configured; the entry to remove is printed); `TestURL` (`127.0.0.1:8484`, `:8484`, `0.0.0.0:8484`, `[::]:8484`, `192.168.1.5:8484`); `TestNonLoopbackWarning`; `TestNixCannotBeRun`; `TestOldNixShowConfig`; `TestUploadHintAlwaysPrinted`.
- [ ] **Step 2: Run** `go test ./nixconf/` — compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./nixconf/ && go vet ./nixconf/` — pass.
- [ ] **Step 5: Commit** `nixconf: the setup check and its advice`.

### Task B11: The command, real-Nix tests, README

**Files:**
- Modify: `cmd/narrenhaus/main.go`, `cmd/narrenhaus/main_test.go`, `README.md`, `.envrc` (watch every package directory)
- Create: `cmd/narrenhaus/serve.go`, `cmd/narrenhaus/check.go`, `e2e/nix_test.go`

**Interfaces:**
- Consumes: everything above; jaccard-store `node.LoadOrCreateKey`, `node.Dial`, `node.DialConfig{Key, ALPN: wire.ALPN, Server}`.
- Produces: `narrenhaus serve` and `narrenhaus check` as spec sections 8 and 10 describe. Keys: `<config-dir>/client.key` (`node.LoadOrCreateKey`), `<config-dir>/signing.key` (`narinfo.LoadOrCreateSigningKey`). `serve`: load options, load keys, print the check's report to standard error, open the cache (`packcache.Origin{Server: <endpoint ID as text>, Prefix}`), build `remote`, `refs`, `upload`, `httpd`; listen; a ticker calls `Uploader.Sweep` every minute; on SIGINT or SIGTERM `http.Server.Shutdown` with a ten-second limit, then cancel the work context and close. One log line (`slog` text handler on standard error) per upload and per served NAR. `check` needs only `--listen` and `--config-dir` (the other flags are not required for it) and exits 0 whether configured or not.

- [ ] **Step 1: Failing tests.** `main_test.go`: `TestServeRefusesBadOptions` (run the app in-process with: a prefix `a@b/`; a server ID `nonsense`; `--cache-size 0`; `--cache-size lots`; a listen address already bound by the test — each returns an error whose text names the option, none panics); `TestCheckNeedsNoServer` (`check --listen 127.0.0.1:1 --config-dir <tmp>` with `PATH` emptied: exit 0, output contains `extra-substituters = http://127.0.0.1:1` and a `narrenhaus-` key); `TestServeCreatesBothKeys`. `e2e/nix_test.go` (every test skips unless `nix` is on `PATH`; each runs `nix` with `XDG_CACHE_HOME` in a fresh directory, `NIX_USER_CONF_FILES=`, `NIX_CONFIG` unset, `--extra-experimental-features 'nix-command flakes'`, and its own store paths, made with `nix store add` (`nix store add-path` on older Nix) of generated directories so that each test's paths are new): `TestNixCopyThenSubstitute` — for each of `""` (xz), `?compression=none`, `?compression=zstd` with one path over 16 MiB: `nix copy --to http://<A><params>` a path and a second path that refers to it (a file containing the first path's name, added after), then on B `nix copy --from http://<B> --to <throwaway store, "local?root=<tmp>"> --option trusted-public-keys <B's key> <path>`, and `nix store verify` (or `nix path-info`) in the throwaway store succeeds; `TestSecondCopyWithCompressionNoneSendsNoNAR` (delete the narinfo references with the server's client, copy again: the request log of A shows `HEAD /nar/` 200 and no `PUT /nar/`); `TestStoppedNarrenhausWithFallback` (a throwaway store, `substituters` set to a closed port's URL plus B, `fallback = true`: `nix copy --from` of B's path still succeeds; this is the nearest runnable form of spec 8.1's case).
- [ ] **Step 2: Run** `go test ./cmd/... ./e2e/ -run 'TestServe|TestCheck|TestNix|TestSecond|TestStopped'` — failures.
- [ ] **Step 3: Implement** `serve` and `check`; write `README.md` (what it is; setup with the three lines and where they go; `serve` and `check` with the option table of spec 10; uploading with `nix copy --to 'http://127.0.0.1:8484?compression=none'`; the known limits of spec 12 in the user's terms; development: the flake, the dev shell, the evaluation option).
- [ ] **Step 4: Run** the whole suite: `gofmt -l . ; go vet ./... ; go test -race ./...`; then `nix build --no-link --option allow-unsafe-native-code-during-evaluation true` (which also runs the package tests in the sandbox, where the `nix`-dependent ones skip) and `nix develop --option allow-unsafe-native-code-during-evaluation true -c narrenhaus check --listen 127.0.0.1:8484 --config-dir "$(mktemp -d)"` — prints the advice for this machine.
- [ ] **Step 5: Commit** `narrenhaus: serve and check; tests with real Nix; README`. Then confirm no binary is left in the tree (`git status --short` shows nothing untracked; no `result` link).
