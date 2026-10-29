# narrenhaus: design

Date: 2026-10-29. Status: draft, waiting for the owner's approval. Checked
against jaccard-store v0.7.0, Amber-Store Core v0.10.0 and Nix 2.26.1 and
2.35.4 (the owner's machine runs 2.35.4).

## 1. Purpose

Colleagues at one company build and run similar software on their laptops.
narrenhaus lets one laptop's build save the next laptop the same build: it
is a Nix binary cache that each person runs locally and that keeps what it
is given on a shared [jaccard-store](https://github.com/amber-store/jaccard-store)
server.

- Nix on the laptop talks to narrenhaus over HTTP on loopback, as to any
  binary cache: it uploads with `nix copy --to` and substitutes from it.
- narrenhaus takes each uploaded NAR apart into content-addressed chunks,
  packs them, and pushes them to jaccard-store as one reference per store
  path, under a name prefix the group shares.
- When Nix asks for a narinfo or a NAR, narrenhaus downloads the packs of
  that reference, keeps them in a local cache of bounded size, and answers
  from them. NARs go out uncompressed.

Success: on laptop A, `nix copy --to http://127.0.0.1:8484 <path>` succeeds;
on laptop B, with narrenhaus configured as a substituter and the same server
and prefix, Nix substitutes `<path>` from it and the path it imports has the
NAR hash the narinfo names. B's pack cache returns under its size limit
whenever no request holds it above. A narrenhaus that is not yet in the Nix
configuration says exactly what to add and where.

## 2. Decisions

### 2.1 Made by the owner

- narrenhaus is **started locally by its user**. At start it **checks the
  Nix configuration and suggests** adding itself as a cache if it is not
  configured as one.
- An uploaded NAR is **decompressed and taken apart**: each file is chunked
  and stored content-addressed, and a **recipe** for rebuilding the original
  NAR is stored as an object.
- The **narinfo is changed accordingly and stored in the same reference** as
  the rest of the NAR. The reference is pushed to jaccard-store.
- A request for a narinfo or a NAR **downloads the packs** and is served
  from them.
- **NARs are served uncompressed.**
- The local cache is a **cache of packs with a fixed maximum size**. When
  it is full, the pack that has **not been used for the longest time** is
  evicted. How use is recorded was left open ("atime or a local sqlite db,
  whatever is easiest").
- **Pack files are used directly; there is no core store on disk**
  (no `packstore`, no `refstore`).
- The **client defines the prefix** of the reference names, so a group of
  clients shares a prefix.
- **No verification of who built what** for now: a laptop trusts what
  colleagues pushed. More layers of security come later.
- Uploads will happen **automatically through `post-build-hook`**, but the
  **hook is left out of the first version**. Uploads are `nix copy --to` run
  by hand.
- **Go**, CLI on **`urfave/cli/v2`**, **no `internal/` packages**.
- The project is a **flake whose dev shell builds narrenhaus**, built with
  **[gonixgo](https://github.com/draganm/gonixgo)**.

### 2.2 Made in design, accepted by the owner in conversation

These were proposed as guesses and the owner answered "nothing wrong".

- **Trust inside Nix's rules.** Each laptop's narrenhaus signs the narinfos
  it serves with its own local key, and the suggested configuration adds
  that key to the trusted keys. Nix's signature checking stays on for every
  other cache.
- **Object format.** Chunks and trees are Amber-Store Core objects
  (`github.com/amber-store/core`, the version jaccard-store pins) with
  core's default chunking. "No core store" means no local core store on
  disk, not a new format.
- **Reference shape.** The root of a reference is a small wrapper directory
  holding the narinfo, the recipe and the store path's file tree
  (section 4).
- **The recipe is small**, because a NAR is fully determined by its file
  tree. It records what the tree cannot.
- **Upload check.** narrenhaus rebuilds the NAR from what it stored and
  refuses the upload if its hash differs from the narinfo's.
- **Reference name** is the prefix followed by the 32-character hash part
  of the store path, so a narinfo request is one exact lookup.
- **Where new jaccard-store code lives.** Pushing without a core store and
  fetching packs as files are added to jaccard-store's `client` package and
  released there, not re-implemented in narrenhaus (section 9).
- **After a push** the pack just built is in the local pack cache too.
- **Narinfo request**: one round trip to the server for the reference;
  unknown is a 404 at once; known means downloading its packs unless
  cached, reading the narinfo, signing and returning it. Server unreachable
  and nothing cached: a quick 404, never a hang.
- **NAR request**: the URL resolves on its own (Nix remembers narinfos for
  30 days and may ask for the NAR without asking for the narinfo again).
  All packs are local before anything is sent; then the NAR is streamed
  with its exact length.
- **Packs are cached expanded** (index plus uncompressed data). The size
  limit counts expanded bytes.
- **Bookkeeping is plain files, no database.** Each use stamps a pack's
  file with the current time; the oldest stamp is evicted first. A small
  record per store path remembers which packs it needs.
- Serving a patch pack counts as a use of its base pack. A pack that a
  request is reading is never evicted, even if the cache is over its limit
  for that moment.
- **Priority** announced to Nix is after cache.nixos.org by default; a flag
  changes it.
- **One foreground command**, run as the user; no launchd or systemd unit.
  Flags have matching `NARRENHAUS_…` environment variables.
- **Two keys** are created on first start in the user's configuration
  directory: the identity shown to jaccard-store and the narinfo signing
  key.
- **The check only suggests.** It prints the lines and the file they belong
  in, never edits a file, never asks for root. narrenhaus serves either way.
- macOS and Linux. Directory `~/amber-store/narrenhaus`, module
  `github.com/amber-store/narrenhaus`, `LGPL-3.0-only` (`LICENSE` holds the
  LGPL, `COPYING` the GPL it incorporates, as in the sibling repositories).
  A local git repository only; no GitHub repository until the owner asks.
- Left out of the first version: the hook, deleting old references on the
  server, build logs, NAR listings, content-addressed realisations.

### 2.3 Made in design, not yet seen by the owner

Each is the designer's call and may be overruled.

1. The wrapper directory's three entries, their names and the metadata
   convention of section 4.2, with values chosen to equal what a Linux
   multi-user `/nix/store` shows, so that directory objects can be shared
   with references pushed by `jaccard-store push-dir`.
2. The recipe is one fixed JSON object with a version (section 4.3).
3. The NAR URL in served narinfos is `nar/<store hash>-<nar hash>.nar`
   (nix-serve's shape). A request whose NAR hash is not the reference's
   current one gets 404, which Nix treats as "gone" and survives, where a
   wrong NAR would be a hash mismatch error (section 6.3).
4. A narinfo `PUT` answers only when the push to jaccard-store has
   finished, so a successful `nix copy` means the path is on the server.
   The push belongs to narrenhaus, not to the request: it goes on if Nix
   gives up waiting, and a repeated `PUT` joins it (section 5.3).
5. An upload of a store path whose reference already exists is not pushed
   again: first writer wins. A reference this laptop has found broken
   counts as absent and is replaced (section 5.3).
6. `HEAD` on a NAR upload URL always answers 404, so Nix always sends the
   NAR (section 5.1).
7. Upload compressions accepted: none, xz, zstd, gzip, bzip2. Others are
   refused with 415 (section 5.1).
8. `PUT` of anything but a NAR or a narinfo is refused with 405; `GET` of
   logs, listings, realisations and debug info is 404.
9. Pack downloads verify every object against its key and the completeness
   of the tree, and the first time a reference is made local its NAR is
   rebuilt once and compared with its narinfo, because the server may run
   without verification and anyone may write to it (sections 6.2, 7.2).
10. A narinfo or NAR request whose packs are not local after 240 seconds
    is answered 404 while the download goes on, so narrenhaus answers
    before Nix's own 300-second limit (section 6.4).
11. Defaults: listen `127.0.0.1:8484`, cache size 10 GiB, priority 50,
    parent rule as jaccard-store's CLI (at least half the bytes shared),
    four pushes and eight pack downloads at once.
12. The signing key's name is `narrenhaus-<first 8 hex digits of its public
    key>`. Nix keeps one key per name, so one name for every laptop would
    break as soon as two keys met in one configuration.
13. Staged uploads are deleted after 15 minutes without upload activity
    (section 5.1).
14. A cache directory belongs to one server and one prefix; started with
    another, narrenhaus refuses and says why (section 7.1).
15. A `check` command beside `serve` that prints the same advice and exits.
16. For a user the Nix daemon does not trust, the check reads the system
    configuration alone, since that is all the daemon honours (section 8).
17. The store directory is `/nix/store`; a narinfo for another is refused.

### 2.4 Open: one question for the owner

A configured narrenhaus that is not running fails Nix operations
(section 8.1). The setup advice has to deal with that one way or another;
which way is the owner's choice.

## 3. Goals, non-goals, anti-goals

Goals

- A working binary cache for `nix copy --to` and for substitution, backed
  by jaccard-store, as section 1 describes.
- Similar store paths share storage on the server through jaccard-store's
  base and patch packs, because files are chunked by content.
- Bounded local disk use for what is served.
- Clear, correct setup advice.

Non-goals (first version)

- `post-build-hook` integration and any upload queue that outlives the
  process.
- Service manager units, a NixOS or nix-darwin module.
- Editing Nix configuration.
- Access control, verification of builders, a shared signing key.
- Retention: nothing deletes references on the server.
- `log/`, `.ls`, `realisations/` / `build-trace-v2/`, `debuginfo/`.
- HTTP range requests on NARs.
- Serving references that narrenhaus did not write (the existing
  `push-subdirs` references have no narinfo), and being mountable by
  jaccard-nfs-nix-store: the wrapper directory is not a store path. Chunks
  and directory objects are still shared with such references.
- NARs that are not canonical. Everything current Nix writes is canonical;
  anything else is refused at upload.

Anti-goals: it has failed, even if it works, when

- a NAR it serves does not hash to the NarHash of the narinfo it served;
- a running narrenhaus makes Nix hang or fail a build because the server
  or the bucket is slow, down or unreachable (it answers 404 in time
  instead);
- it keeps a core store or any second copy of objects besides pack files;
- it changes a Nix configuration file or needs root;
- a real server endpoint ID or a machine's name enters the tree or a
  commit. Tests use the RFC 8032 test-vector key, examples use prefixes
  such as `acme/`.

## 4. What a reference holds

### 4.1 Name

`<prefix><hash>`: the configured prefix, verbatim, followed by the
32-character nix32 hash part of the store path (for
`/nix/store/0c6kzph7l0dcbfmjap64f0czdafn3b7x-hello-2.12`, that is
`0c6kzph7l0dcbfmjap64f0czdafn3b7x`). The prefix is an opaque string;
narrenhaus adds no separator and normalises nothing, so a prefix wants its
own trailing separator (`acme/`). The whole name must pass core's
`reference.ValidateName`; a prefix that cannot is refused at start.

### 4.2 Root and objects

All objects are core objects (Blob, FileNode, DirLeaf, DirNode) built with
core's public builders and default parameters: `chunkers.SplitBytes` with
nil options (32 KiB / 512 KiB / 1 MiB) for file bytes, and
`chunkers.NewItemChunker(ingest.DefaultItemBits)` for file indexes and
directory entries. A file's content is built the way core's
`ingest/driver.go` `buildFile` builds it: a file of one chunk is that Blob,
an empty file is the empty Blob, a file of several chunks gets FileNodes. A
file of at most 32 KiB is always one Blob and may be encoded with
`fstree.EncodeBlob` directly; the key is the same. These parameters, and
the chunking library's behaviour behind them, decide every key and are
frozen: changing them breaks sharing with everything already stored.

The root is a directory with exactly three entries, in name order:

| Name | Kind | Content |
|---|---|---|
| `nar` | what the NAR's root node is: directory, regular file or symlink | the store path itself |
| `narinfo` | regular file | the narinfo text as stored (section 5.2) |
| `recipe` | regular file | the recipe (section 4.3) |

Because `nar` is an entry, a store path that is a single file keeps its
executable flag in the entry's mode, and one that is a symlink keeps its
target in the entry, which a bare root key could not carry. The contents of
`narinfo` and `recipe` are built as files like any other; a narinfo with
several hundred references exceeds 32 KiB and may be a FileNode.

A NAR has no owners, times or permissions beyond the executable flag, and
core's entries require them, so they are fixed:

| Field | Value |
|---|---|
| Mode, regular file | `0o100444`; with the executable flag `0o100555` |
| Mode, directory | `0o040555` |
| Mode, symlink | `0o120777` |
| UID, GID | 0, 0 |
| Mtime | 1 second after the epoch (1 000 000 000 ns) |
| Rdev, xattrs | absent |

Modes are raw POSIX `st_mode` values as core's `fstree.Entry.Mode` holds
them, identical on Linux and macOS (type mask `0o170000`). They are written
as literals or from `golang.org/x/sys/unix` (`S_IFREG`, `S_IFDIR`,
`S_IFLNK`), never by converting a Go `fs.FileMode`, whose type bits differ.
`narinfo` and `recipe` are regular, not executable. When reading, a file is
executable when `mode & 0o100 != 0`.

These values are what core's ingest records for a store path on a Linux
multi-user install, so directory objects of narrenhaus references can equal
those of references pushed with `jaccard-store push-dir`, provided the
files there carry no extended attributes (an SELinux host labels every
file, and ingest records the label) and no `.amberignore` applied. File
chunks are shared regardless. The convention is frozen once a reference has
been pushed.

Every object of a reference is reachable from the root through core's
`fstree.ChildKeys`, so references pass a verifying jaccard-store server as
well as one run with `--no-verify`.

### 4.3 Recipe

The recipe says how the NAR is rebuilt from the reference. Version 1 is
this exact byte string, with no trailing newline, so every reference shares
one Blob:

```
{"format":"narrenhaus-recipe","node":"nar","version":1}
```

Its meaning: the NAR is the canonical `nix-archive-1` serialisation of the
entry named by `node` in the root directory: directories list their entries
in byte order of name, a regular file is `executable` when its mode says
so, its contents are the file's bytes, a symlink's target is the entry's
link target. Nothing else is needed, because a canonical NAR is a function
of its tree.

A reader that finds another `format` or a `version` it does not know must
not serve the reference: it answers 404 and logs it, and does not treat the
reference as broken (section 6.4), since a newer narrenhaus may have
written it. This is the room for a later recipe that reproduces something
the tree cannot.

## 5. Upload

Nix uploads a path as two requests linked only by the narinfo's `URL`
field. Up to Nix 2.34 each path is sent as its NAR, then its narinfo. From
2.35 a `nix copy` sends **all** NARs first, largest first, and then the
narinfos in reference order, as many at once as the machine has cores. So
staging holds every path of one copy at the same time.

### 5.1 `PUT /nar/<file hash>.nar[.<ext>]`

- The body is decompressed as it arrives. The compression is recognised by
  the body's first bytes (xz, zstd, gzip, bzip2, or the `nix-archive-1`
  header of an uncompressed NAR), not by the extension: Nix gives gzip
  uploads no extension. Anything else is 415. zstd input may be several
  frames back to back (Nix 2.35 writes one per 16 MiB).
- The decompressed stream is parsed as a NAR by a strict reader. It
  rejects what Nix's own parser rejects and trailing bytes besides:
  unsorted or duplicate names; names that are empty, `.`, `..`, longer than
  255 bytes or contain `/` or NUL; symlink targets that are empty, contain
  NUL or exceed 4095 bytes; nesting deeper than 64 levels; an `executable`
  marker with a non-empty value; non-zero padding; bytes after the root
  node. While parsing, narrenhaus computes the SHA-256 and length of the
  decompressed stream.
- Each node becomes core objects as in section 4.2. Objects are appended,
  each once, to a flat data file in the staging directory, with a table
  from key to offset and length. Memory in use is the chunker's buffer
  (2 MiB) and that table; when the NAR has been read the table is written
  beside the data file (it becomes the pack's index), so memory does not
  grow with the number of staged NARs.
- The result is a staged NAR: the data file, the table, the key and entry
  metadata of the `nar` node, the NAR's hash and size. It is filed under
  the request's URL path. Several staged NARs may share a URL path (two
  store paths with identical contents); each narinfo takes one.
- A malformed NAR is 400 and leaves nothing behind. Success is 200 with an
  empty body.
- `HEAD` on these URLs always answers 404. Nix asks before each upload and
  would skip the NAR if told it exists.
- Staged NARs are deleted when 15 minutes have passed during which no
  upload request was being handled and no push was running or queued, and
  everything in staging is deleted at start. Age alone never deletes one:
  with Nix 2.35 a NAR waits for its narinfo for as long as the rest of the
  copy takes.

Upload URLs and served URLs (section 6.3) cannot be confused: an upload URL
has a 52-character file hash, a served URL a 32-character store hash, a
dash and a 52-character NAR hash.

### 5.2 The narinfo as stored

`PUT /<hash>.narinfo` carries Nix's narinfo. narrenhaus parses it (unknown
keys are ignored, as Nix does) and stores this text:

- `StorePath`, `NarHash` (as `sha256:<nix32>`), `NarSize`, `References`
  (always present, `References: ` with the trailing space when empty),
  `Deriver`, `Sig` lines and `CA` as uploaded;
- `URL: nar/<store hash>-<nar hash nix32>.nar`;
- `Compression: none`;
- `FileHash` and `FileSize` equal to `NarHash` and `NarSize`;
- fields in the order Nix writes them (StorePath, URL, Compression,
  FileHash, FileSize, NarHash, NarSize, References, Deriver, Sig sorted,
  CA), every line ended by a line feed.

The laptop's own signature is not stored; it is added when serving
(section 6.2). Uploaded signatures stay valid, because a signature covers
only the store path, NarHash, NarSize and references.

### 5.3 `PUT /<hash>.narinfo`

The work a narinfo `PUT` starts belongs to narrenhaus, not to the request.
It runs under the process's context and goes on if the client disconnects.
There is one piece of work per store hash: a `PUT` for a hash whose work is
queued or running joins it and gets its result. At most four run at once;
the rest queue.

In order:

1. Parse. The hash in the URL must be the hash part of `StorePath`,
   `StorePath` must be under `/nix/store`, and the body must be plain text
   (a `Content-Encoding` header is 415); otherwise 400.
2. Ask the server whether `<prefix><hash>` exists. If it does, and this
   process has not found that reference broken (section 6.4), drop the
   staged NAR the narinfo names, if any, and answer 200: first writer wins.
   A reference found broken counts as absent, and the upload replaces it.
3. Take a staged NAR filed under the narinfo's `URL`. None: 400 with a body
   that says the NAR was not received.
4. The staged NAR's hash and size must equal `NarHash` and `NarSize`;
   otherwise 400 (Nix does not check this itself before uploading).
5. Build the contents of `narinfo` and `recipe` as files (section 4.2) and
   the root directory, appending them to the staged data file. After this
   the staged file and table do not change and are read by position, so
   concurrent reads are safe.
6. Rebuild the NAR from the staged objects exactly as section 6.3 does and
   compare its SHA-256 and length with `NarHash` and `NarSize`. A
   difference is 422 with a body that says narrenhaus cannot reproduce this
   NAR, and is logged as a defect.
7. Push the reference from the staged objects (section 9.1) with the parent
   rule of jaccard-store's CLI (`MinDedup` 0.5, set explicitly) and
   `TempDir` set to the cache directory's `tmp/`.
8. Move the staged data file into the pack cache as a complete pack for
   the root (section 7.1) and write the store path's record. Answer 200.

What happens to the staged NAR decides whether Nix's own repeats can
succeed, since Nix repeats a request that got 5xx or no answer:

| Outcome | Status | Staged NAR |
|---|---|---|
| Success, or the reference existed | 200 | removed |
| Malformed or mismatching narinfo (steps 1, 4), no staged NAR (3) | 400 | removed if taken |
| NAR cannot be reproduced (6) | 422 | removed |
| Server refuses for a reason a repeat cannot cure (pack over the server's size limit, bad name) | 422 with the server's message | removed |
| Server or bucket unreachable, connection lost (2, 7) | 502 | back in staging |

Nix gives up on a request that has been silent for
`stalled-download-timeout` (300 seconds by default) and, in 2.35, repeats
it up to four more times; the repeats join the push under way. A path whose
push and queueing take longer than about 25 minutes therefore still fails
the copy, unless `nix copy` is run with
`--option stalled-download-timeout <seconds>`; running the copy again after
the push has finished succeeds at once (step 2).

Limits inherited from jaccard-store: 16 GiB of uncompressed objects per
pack by default (the server's `--max-pack-size`, refused only after the
pack has been built), 256 MiB per object (chunks are at most 1 MiB), 2^24
objects per pack.

## 6. Serving

All answers work for `GET` and `HEAD` alike, `HEAD` without a body.

### 6.1 `GET /nix-cache-info`

```
StoreDir: /nix/store
WantMassQuery: 1
Priority: <priority>
```

### 6.2 `GET /<hash>.narinfo`

1. Ask the server for `<prefix><hash>` (one round trip, section 9.2).
2. Not found: 404, and any local record for the hash is removed.
3. Found with root R. If the local record names R and its packs are
   cached, read the narinfo from them. Otherwise make the reference's packs
   local (section 7.2), read `recipe` and `narinfo` from the root with
   `fstree.WriteContent`, rebuild the NAR once from the tree and compare
   its SHA-256 and length with the narinfo's `NarHash` and `NarSize` (a
   difference is a broken reference, section 6.4), then write the record.
   A pack placed by a local upload has passed this check in step 6 of
   section 5.3.
4. Server unreachable: answer from the local record and its packs if they
   are there, otherwise 404.

`HEAD` stops after step 1 or 2: it answers 200 or 404 without downloading.

The answer is the stored narinfo with one more `Sig` line: the laptop's
signature over Nix's fingerprint
`1;<store path>;<NarHash as sha256:nix32>;<NarSize>;<references as full
store paths, sorted, joined by commas>`, as `<key name>:<base64 of the
64-byte Ed25519 signature>`. Content type `text/x-nix-narinfo`.

Step 3 downloads a whole pack to answer a narinfo. That is the cost of the
narinfo living in the reference, and it is usually not wasted, since Nix
asks for the NAR next. It is wasted when Nix only wants to know whether a
path is present, as `nix copy --to` does for every path of a closure
(section 12).

### 6.3 `GET /nar/<store hash>-<nar hash>.nar`

1. Find the root: from the local record if its packs are all cached,
   otherwise by asking the server (not found, or unreachable without a
   usable record: 404).
2. Make the packs local, with the check of section 6.2 step 3 if the
   reference was not local before.
3. Read the stored narinfo. If its NarHash is not the one in the URL,
   answer 404: the reference now holds another build than the narinfo the
   client remembers.
4. Send `Content-Length: <NarSize>`, `Content-Type: application/x-nix-nar`,
   then the NAR, written by walking the `nar` entry: directory entries in
   stored order, a file's length from its content key (`key.Length()`), its
   bytes with `fstree.WriteContent`. No `Accept-Ranges`. If the bytes
   written would differ from a file's stated length or from `NarSize`, the
   writer stops, cuts the connection and logs a defect.

Nothing is sent before step 3 has succeeded, because after the header a
failure can only be shown by cutting the connection.

### 6.4 Time limits and errors

Nix abandons a request that is silent for 300 seconds and then counts it as
an error, which can fail a build; a 404 it survives. So before the response
header narrenhaus answers 404 in time and never 5xx:

- **Lookup.** One request waits at most 10 seconds for the connection and
  the lookup round trip together. Beyond that the server counts as
  unreachable. Pushes (`push-start`, `push-upload`, `push-commit`) have no
  such limit: a verifying server is silent for as long as it verifies.
- **Dial.** The dial runs under the process's context with a 30-second
  limit and is not abandoned when a waiting request gives up. After a dial
  has failed, requests fail fast for 30 seconds instead of dialling again.
- **Packs.** If a request's packs are not local 240 seconds after it
  arrived, it is answered 404 and the download goes on (section 7.2), so a
  later request finds the packs.
- **Transfer failure.** A bucket error, a stalled or cut transfer, an
  expired URL or a full disk is not a broken reference: the pack being
  downloaded is discarded, packs already verified are kept, the request is
  answered 404 and the cause is logged.
- **Broken reference.** A root that is not the wrapper of section 4, an
  index or object that does not match its key, a tree that is incomplete
  after all packs are local, or a NAR that does not match its narinfo. It
  is 404 to Nix and an error in the log; its packs are dropped from the
  cache, and the process remembers the name and root as broken, which lets
  an upload replace it (section 5.3 step 2).
- Every other path is 404.

## 7. Pack cache

### 7.1 Layout

Default directory: `<os.UserCacheDir()>/narrenhaus`.

```
origin                    the server's endpoint ID and the prefix
packs/<root hex>.idx      jaccard-store index (packfile.Index encoding)
packs/<root hex>.data     the pack's objects, uncompressed, at the index's offsets
packs/<root hex>.meta     JSON: complete or patch, the parent's root for a patch
refs/<store hash>.json    record: root, roots of the packs it needs
staging/                  NARs being uploaded
tmp/                      downloads and packs being built
```

- A pack is cached under its root key. It is **complete** when it holds the
  whole key set of its root (a base pack, or a pack staged by an upload),
  otherwise it is a **patch** and names its parent's root. One root's
  complete pack serves any reference with that root and any patch that
  names it as parent.
- A record holds no narinfo text: the narinfo is read from the pack, and a
  record exists only while its root's pack is cached.
- Files are written under `tmp/` and renamed into place; `.meta` goes last
  and marks the pack valid. At start, packs without `.meta` and everything
  under `tmp/` and `staging/` are removed.
- `origin` is written on first use. If it names another server or prefix,
  narrenhaus refuses to start and names the directory and both values.
- Reading an object: find the key in the pack's index, read `length` bytes
  at `offset` from `.data` by position; for a patch, try the parent next.
  The reader is safe for concurrent use; `fstree.CheckComplete` and the
  push's key-set walk call it from several goroutines.

### 7.2 Making a reference's packs local

- The server's answer to a lookup names one or two packs: the reference's
  own and, for a patch, its parent (a base pack). URLs in the answer expire,
  so they are used at once and never stored; a download that meets an
  expired URL looks the reference up again once.
- A cached pack counts as present if it is complete, or if it is the
  reference's own pack and a patch whose parent is the one the server
  names. A cached patch found where the server names another parent, or
  names that root as a base pack, is dropped and fetched anew.
- A download belongs to the cache, not to the request that started it: it
  runs under the process's context, goes on if the client disconnects, and
  later requests for the same root join it. One download per root runs at a
  time and at most eight in all. A transfer that moves no byte for 30
  seconds has failed.
- A download fetches the index, then streams the data through
  decompression, checks each object against its key (`verify.Object`) and
  writes the objects in offset order into `tmp/`; unverified bytes are
  never kept. After the last pack, `fstree.CheckComplete` from the root
  must find every object.

### 7.3 Size limit and eviction

- The size of the cache is the sum of the `.data` and `.idx` files under
  `packs/`. Staging and `tmp/` are not counted and are not bounded by the
  limit (section 12).
- Every use of a pack (a narinfo read, a NAR served, an upload placing it)
  sets the modification time of its `.data` file to now. Using a patch
  also stamps its parent. Access time is not used: it is unreliable across
  mount options.
- A request holds every pack of its reference, the parent included, from
  the moment it knows them, before any download, until it has finished.
- Whenever a pack is added or a request lets go of its packs, while the
  size exceeds the limit, the pack with the oldest stamp that no request
  holds is deleted, with its `.idx` and `.meta`. Records under `refs/`
  whose root is that pack go with it.
- Evicting a base pack leaves its patches in place; they fetch it again
  when needed.
- If everything left is held, the cache stays over the limit until a
  request ends. A limit smaller than one reference's packs is honoured only
  between requests.

## 8. Setup check

Run at the start of `serve` (which then serves regardless) and by `check`
(which then exits).

1. Read the effective configuration with
   `nix --extra-experimental-features nix-command config show --json`
   (`nix show-config --json` on older Nix), parsing standard output only.
   Ask `nix --extra-experimental-features nix-command store info --json`
   whether the daemon trusts the user: the field is `trusted`, `true` or
   `false` on current Nix and `1` or `0` on older; a missing field or a
   failing command means not trusted. For a user it does not trust, read
   the configuration again as the daemon sees it: the same command with
   `NIX_USER_CONF_FILES` set to the empty string and `NIX_CONFIG` removed
   from the environment.
2. Configured means all of:
   - `substituters` has an entry equal to narrenhaus's URL after dropping a
     trailing slash and any `?parameters`. The URL is `http://` plus the
     listen address, with an unspecified host (`:8484`, `0.0.0.0`, `[::]`)
     replaced by `127.0.0.1`;
   - `trusted-public-keys` contains `<key name>:<base64 public key>`, and
     no entry with the same name and another key comes before it (Nix keeps
     only the first key of a name; if one does, print it and say it must be
     removed);
   - what section 8.1 settles.
3. Configured: one line saying so. Where that rests on a file under
   `/etc/nix`, add that it takes effect once the Nix daemon has been
   restarted after the edit, since the check cannot see the daemon's own
   settings.
4. Not configured: print the missing lines,

   ```
   extra-substituters = http://127.0.0.1:8484
   extra-trusted-public-keys = narrenhaus-1a2b3c4d:<base64>
   ```

   and where they go, first case that applies:
   - the daemon trusts the user: `~/.config/nix/nix.conf`, which needs
     neither root nor a restart;
   - `/etc/nix/nix.conf` is a link into the Nix store or `/etc/static`
     (NixOS, nix-darwin): the file is generated; print the same as
     `nix.settings` options for the system configuration;
   - `/etc/nix/nix.conf` has an `include` or `!include` line naming
     `nix.custom.conf` (Determinate Nix): `/etc/nix/nix.custom.conf`, and
     restart the Nix daemon;
   - otherwise `/etc/nix/nix.conf`, and restart the Nix daemon.
5. Always print how to upload:
   `nix copy --to 'http://127.0.0.1:8484?compression=none' <path>`, and
   that very large paths may need
   `--option stalled-download-timeout <seconds>`.
6. If `nix` cannot be run or its output cannot be read, say so and print
   the lines of step 4 with `/etc/nix/nix.conf` as the place.
7. If the listen address is not loopback, warn that anyone who can reach
   the port can upload into the shared prefix and get narinfos signed with
   this laptop's key.

The check reads and prints; it writes nothing outside narrenhaus's own
directories.

### 8.1 When narrenhaus is not running (open, section 2.4)

Checked in Nix's source for 2.26.1 and 2.35.4: once narrenhaus is in
`substituters`, and Nix has its `nix-cache-info` remembered (7 days), a
narrenhaus that is not running makes Nix fail any operation that needs a
path no earlier cache has, unless `fallback = true`. Before 2.32 any dead
substituter is fatal; from 2.32 only the last one asked is, and priority 50
makes narrenhaus the last. The owner starts narrenhaus by hand, so "not
running" is an everyday state. The two ways out:

- **Suggest `fallback = true`** as a third line and have the check report
  when it is off. It may go in the user's own file for any user. Its cost:
  it applies to every cache, so a failed download from cache.nixos.org
  also turns into a local build instead of an error.
- **Announce a priority before cache.nixos.org** (below 40). From Nix 2.32
  a failing narrenhaus is then skipped, because another cache comes after
  it. Its costs: narrenhaus is asked for every path Nix wants, each a
  lookup on the server; paths that cache.nixos.org also has are fetched
  through narrenhaus when a colleague pushed them; while narrenhaus is
  down each query waits through a few seconds of retries; and Nix before
  2.32 still fails.

## 9. jaccard-store

narrenhaus talks to the server through `github.com/amber-store/jaccard-store`
(`node` for the key file and dialling by endpoint ID, `client`, `packfile`,
`verify`). Today `client.Push` and `client.Pull` take a core
`*packstore.Store`. Two additions to the `client` package are part of this
project. Names below are suggestions; the requirements are what bind.

### 9.1 Push from any object source

A push that reads objects from a source other than a packstore and does
everything `Push` does today: walk the key set from the root, sketch,
`push-start`, parent choice, pack building, upload, `push-commit`, with the
same `PushOptions` and `PushResult`. The source offers what `Push` uses of
a packstore: `Get(key) ([]byte, error)`, safe for concurrent calls and
returning a slice the caller owns, and optionally an ordering of keys for
sequential reading (packstore's `SortByLocation`), applied before packing
when the source has it. `Push` becomes a caller of the new function, and a
push from a packstore produces the same pack, in the same object order, as
at v0.7.0.

### 9.2 Lookup and fetch packs as files

- A lookup of a name that returns the root and the packs the server names
  (root, object count, uncompressed bytes, compressed size), or
  `ErrNotFound`, in one round trip, fetching nothing.
- Fetching one of those packs: the index, checked against the announced
  counts and sizes as `Pull` checks it, and the data, with every object
  checked against its key and then written in offset order to a writer or
  handed to the caller one by one. Unverified bytes are never written.
  Bucket transfers take the caller's `http.Client`, so the caller sets the
  stall limit. Existing `Pull` behaviour does not change.

### 9.3 Connection

One iroh connection is shared by lookups and pushes, dialled on first need.

- It is retired when opening a stream fails, when writing or reading a
  frame fails, or when a lookup gets no answer in 10 seconds; never for
  `ErrNotFound`, a refusal by the server (`*wire.Error`), a bucket error or
  a failed verification.
- Retiring takes it out of use for new calls. Of the calls that fail on
  one connection only the first retires it. It is closed when the last
  call still running on it has returned, so a push in flight is not cut by
  another request's timeout. Each dial binds its own endpoint, so the old
  and the new connection can exist together.
- A lookup that fails on a connection that was already up is repeated once
  on a freshly dialled one, within the same 10 seconds, before the request
  counts as unreachable. This matters after sleep: the first lookups must
  not answer 404, which Nix would remember for an hour.
- Dials are serialised; a caller that waited for a dial that failed gets
  that failure.

`jaccard-nfs-nix-store/refs` has a working pattern for the generation
counter and the serialised dial; its rule of dropping the connection on any
error is not taken over.

### 9.4 Order of work and pinning

- The additions are made in `~/amber-store/jaccard-store` on a branch and
  go up as a pull request. Pushing the branch, merging and releasing are
  the owner's steps; narrenhaus pins the release that carries them.
- Until that release exists, narrenhaus's `go.mod` names the
  pseudo-version of the pull request's commit, which must first be pushed
  to `github.com/amber-store/jaccard-store`. A directory `replace` is not
  an option: gonixgo refuses one that leaves the source tree, and the dev
  shell must build. The pseudo-version is temporary, and narrenhaus is not
  finished before the pin has moved to the release tag.
- Go and dependency versions follow jaccard-store's `go.mod` (core,
  go-iroh), so the types agree.

## 10. Command line

`narrenhaus serve` and `narrenhaus check`, on `urfave/cli/v2`. Every option
has a flag and an environment variable.

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--server` | `NARRENHAUS_SERVER` | required | jaccard-store server's endpoint ID |
| `--prefix` | `NARRENHAUS_PREFIX` | required | prefix of reference names, verbatim |
| `--listen` | `NARRENHAUS_LISTEN` | `127.0.0.1:8484` | HTTP address |
| `--cache-dir` | `NARRENHAUS_CACHE_DIR` | `<UserCacheDir>/narrenhaus` | pack cache |
| `--cache-size` | `NARRENHAUS_CACHE_SIZE` | `10GiB` | limit of section 7.3 |
| `--config-dir` | `NARRENHAUS_CONFIG_DIR` | `<UserConfigDir>/narrenhaus` | the two keys |
| `--priority` | `NARRENHAUS_PRIORITY` | `50` | `Priority` in `nix-cache-info` |

`check` needs only `--listen` and `--config-dir`.

Keys, created with mode 0600 when missing: `client.key`, in the format of
jaccard-store's `node.LoadOrCreateKey`; `signing.key`, a Nix secret key
`<key name>:<base64 of the 64-byte Ed25519 secret key>`, whose name
`narrenhaus-<first 8 hex digits of the public key>` is fixed when the key
is made.

Nix remembers `nix-cache-info` for 7 days, so a changed `--priority` takes
effect only after that, or at once with `?priority=N` on the substituter
URL.

`serve` logs to standard error: the check's output, the address, then one
line per upload and per served NAR, and errors. It stops on SIGINT or
SIGTERM after running requests have finished or ten seconds have passed;
pushes still running then are abandoned.

## 11. Repository and build

- `~/amber-store/narrenhaus`, module `github.com/amber-store/narrenhaus`,
  flat top-level packages, the command in `cmd/narrenhaus`.
- `flake.nix` after jaccard-store's: nixpkgs `nixos-26.05`, `systems`,
  `gonixgo` from `github:draganm/gonixgo` at its latest release (v0.5.0 on
  2026-10-29). `packages.default` is `goEnv.buildGoApplication` with
  `pname = "narrenhaus"`, `subPackages = [ "cmd/narrenhaus" ]`,
  `meta.license = lgpl3Only`.
- `devShells.default` carries Go and `self.packages.<system>.default`, so
  entering the shell builds narrenhaus and puts it on `PATH`.
  `devShells.bare` carries Go alone, for when the module does not resolve.
- `.envrc` watches `go.mod`, `go.sum` and the package directories and uses
  the flake with `--option allow-unsafe-native-code-during-evaluation true`,
  which every command that evaluates the package needs.
- gonixgo runs the packages' tests in the build. Tests that need the `nix`
  program, a network or a container skip themselves when it is missing.
- `README.md` covers what it is, setup, the options and the known limits.

## 12. Known limits

- **Answering a narinfo for a path not yet cached downloads its pack**, and
  the base pack for a patch. `nix copy --to` asks for the narinfo of every
  path of a closure to learn what is already there, so copying a closure
  that colleagues have already pushed downloads those packs just to say
  "present". The cache limit bounds the disk; the transfer is real.
- Nix asks for every narinfo of a closure before it fetches the first NAR.
  When a closure's packs exceed the cache limit, packs downloaded for the
  narinfos are evicted before their NARs are asked for and are downloaded
  twice.
- **Staging is outside the cache limit.** With Nix 2.35 and later a
  `nix copy` sends all NARs before the first narinfo, so staging needs free
  disk for the uncompressed size of everything being copied. It cannot be
  capped by refusing NARs, because Nix sends no narinfo until every NAR is
  accepted.
- A pull of a patch fetches its whole base pack, which can be as large as
  the reference again.
- A request whose packs take more than 240 seconds to arrive gets a 404.
  Nix remembers a narinfo 404 for an hour (`narinfo-cache-negative-ttl`)
  and builds the path or looks elsewhere, although the pack arrives shortly
  after. The same hour applies to a path a colleague pushes after this
  laptop asked for it, and to 404s given while the server was unreachable.
- A push that takes longer than Nix's 300-second limit shows as repeated
  requests, and beyond about 25 minutes as a failed copy (section 5.3).
- `nix copy` copies whole closures, so paths that came from
  cache.nixos.org are pushed too.
- The user account that ran `nix copy` remembers Nix's own narinfo for the
  path, whose NAR URL narrenhaus does not serve. On a multi-user install
  the daemon has its own memory and is not affected; it will be once
  uploads run from the post-build hook.
- Pushes running at the same time cannot be patches of one another, so a
  first upload of many related paths stores more than a later one would.
- Small store paths are one reference, one index object and one data
  object each.
- Anyone who knows the server's endpoint ID can read, replace and delete
  any reference; the prefix is a name, not a boundary. Last writer wins on
  the server; narrenhaus's first-writer check narrows the window and does
  not close it.
- Nothing on the server expires.
- A configured narrenhaus that is not running fails Nix operations unless
  section 8.1's remedy is in place.

## 13. Testing

- **NAR and tree**: round trips of canonical NARs through objects and back
  give identical bytes, for a directory root, a file root with and without
  the executable flag, a symlink root, empty files and directories, a file
  of several chunks, and names that sort differently by byte than by
  locale. Malformed and non-canonical NARs are refused, each rule of
  section 5.1.
- **Frozen keys**: a fixed small NAR (a directory holding a plain file, an
  executable, a subdirectory and a symlink) with fixed narinfo must give a
  hard-coded root key, and a file of several chunks made from seeded bytes
  a hard-coded content key, so a change in the chunking library, the
  encoding or the metadata convention fails the build. The same tree is
  read back with core's own decoder and its modes compared.
- **Narinfo**: parsing what Nix writes, the stored form, the signature
  (checked against a fingerprint computed independently and, where `nix` is
  present, by Nix itself).
- **Pack cache**: expanded reads, patch with parent, a cached patch whose
  parent changed, stamps, eviction order, held packs, the over-limit case,
  recovery of a directory left by a killed process.
- **End to end without network**: an in-process jaccard-store server over
  loopback iroh with an in-memory bucket, as jaccard-store's `e2e` and
  jaccard-nfs-nix-store's `jstest` set one up. Two narrenhaus instances
  with separate directories and one prefix: upload through one, fetch
  through the other, compare NAR bytes and narinfo. Also: a repeated
  narinfo `PUT` joining a slowed push; a 502 that leaves the NAR staged; a
  server-side connection killed between two narinfo requests, the second
  still answering 200; a refused push that leaves a concurrent push
  running; a reference with a narinfo that does not match its tree.
- **With real Nix**, where the `nix` program is present: `nix copy --to` a
  closure of several paths with references between them into one instance
  and substitute it from another into a throwaway store; with the default
  xz upload, with `?compression=none`, and with zstd and a NAR over 16 MiB.
  Each such test runs `nix` with `XDG_CACHE_HOME` pointing at a fresh
  directory, `NIX_USER_CONF_FILES` empty and `NIX_CONFIG` unset, and uses
  its own store paths, because Nix remembers what a cache URL answered.
- **Setup check**: against captured `nix config show --json` and
  `nix store info --json` outputs for each case of section 8, `trusted` in
  both encodings and missing, a conflicting key of the same name.

## 14. Left to the builder

- Package names and boundaries, within flat top-level packages.
- The NAR, narinfo and signing library (`zombiezen.com/go/nix` and
  `github.com/nix-community/go-nix` were looked at; either, or own code),
  as long as the reader is as strict as section 5.1 asks and the writer is
  canonical.
- The decompression libraries.
- The exact names and signatures of the jaccard-store additions.
- The format of `.meta`, the records, `origin` and the staged table.
- Log wording and the layout of the check's output.
