# narrenhaus: design

Date: 2026-10-29. Status: draft, waiting for the owner's approval.

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
NAR hash the narinfo names. B's pack cache never stays above its size limit.
A narrenhaus that is not yet in the Nix configuration says exactly what to
add and where.

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
   convention of section 4.2, including values chosen to equal what a
   Linux multi-user `/nix/store` shows, so that directory objects can be
   shared with references pushed by `jaccard-store push-dir`.
2. The recipe is one fixed JSON object with a version (section 4.3).
3. The NAR URL in served narinfos is `nar/<store hash>-<nar hash>.nar`
   (nix-serve's shape). A request whose NAR hash is not the reference's
   current one gets 404, which Nix treats as "gone" and survives, where a
   wrong NAR would be a hash mismatch error (section 6.3).
4. A narinfo `PUT` returns only after the push to jaccard-store has
   finished, so a successful `nix copy` means the path is on the server
   (section 5.3).
5. An upload of a store path whose reference already exists is not pushed
   again: first writer wins (section 5.3).
6. `HEAD` on a NAR upload URL always answers 404, so Nix always sends the
   NAR (section 5.1).
7. Upload compressions accepted: none, xz, zstd, gzip, bzip2. Others are
   refused with 415 (section 5.1).
8. `PUT` of anything but a NAR or a narinfo is refused with 405; `GET` of
   logs, listings, realisations and debug info is 404.
9. Pack downloads verify every object against its key and the completeness
   of the tree, because the server may run without verification
   (section 7.2).
10. Defaults: listen `127.0.0.1:8484`, cache size 10 GiB, priority 50,
    signing key name `narrenhaus-1`, parent rule as jaccard-store's CLI
    (at least half the bytes shared).
11. A cache directory belongs to one server and one prefix; started with
    another, narrenhaus refuses and says why (section 7.1).
12. A `check` command beside `serve` that prints the same advice and exits.
13. For a user the Nix daemon does not trust, the check reads the system
    configuration alone, since that is all the daemon honours (section 8).
14. The store directory is `/nix/store`; a narinfo for another is refused.

## 3. Goals, non-goals, anti-goals

Goals

- A working binary cache for `nix copy --to` and for substitution, backed
  by jaccard-store, as section 1 describes.
- Similar store paths share storage on the server through jaccard-store's
  base and patch packs, because files are chunked by content.
- Bounded local disk use.
- Clear, correct setup advice.

Non-goals (first version)

- `post-build-hook` integration and any upload queue.
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
- it makes Nix hang or fail a build because the server is slow, down or
  unreachable (it must answer 404 quickly instead);
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
`ingest/driver.go` `buildFile` builds it (a file of one chunk is that Blob;
an empty file is the empty Blob). These parameters decide every key and are
frozen: changing them breaks sharing with everything already stored.

The root is a directory with exactly three entries, in name order:

| Name | Kind | Content |
|---|---|---|
| `nar` | what the NAR's root node is: directory, regular file or symlink | the store path itself |
| `narinfo` | regular file | the narinfo text as stored (section 5.2) |
| `recipe` | regular file | the recipe (section 4.3) |

Because `nar` is an entry, a store path that is a single file keeps its
executable flag in the entry's mode, and one that is a symlink keeps its
target in the entry, which a bare root key could not carry.

A NAR has no owners, times or permissions beyond the executable flag, and
core's entries require them, so they are fixed:

| Field | Value |
|---|---|
| Mode, regular file | `S_IFREG \| 0444`; with the executable flag `S_IFREG \| 0555` |
| Mode, directory | `S_IFDIR \| 0555` |
| Mode, symlink | `S_IFLNK \| 0777` |
| UID, GID | 0, 0 |
| Mtime | 1 second after the epoch (1 000 000 000 ns) |
| Rdev, xattrs | absent |

`narinfo` and `recipe` are regular, not executable. These values are what
core's ingest records for a store path on a Linux multi-user install, so
directory objects of narrenhaus references can equal those of references
pushed with `jaccard-store push-dir`. They too are frozen once a reference
has been pushed. When reading, a file is executable when its mode has the
owner execute bit (`0100`).

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
not serve the reference (it answers as for a broken reference, section 6.4).
This is the room for a later recipe that reproduces something the tree
cannot.

## 5. Upload

Nix uploads a path as two requests linked only by the narinfo's `URL`
field: the NAR first, the narinfo last. A `nix copy` sends several paths at
once.

### 5.1 `PUT /nar/<file hash>.nar[.<ext>]`

- The body is decompressed as it arrives. The compression is recognised by
  the body's first bytes (xz, zstd, gzip, bzip2, or the `nix-archive-1`
  header of an uncompressed NAR), not by the extension: Nix gives gzip
  uploads no extension. Anything else is 415.
- The decompressed stream is parsed as a NAR by a strict reader: it must
  reject unsorted or duplicate names, names that are empty, `.`, `..` or
  contain `/` or NUL, non-zero padding, and bytes after the root node.
  While parsing, narrenhaus computes the SHA-256 and length of the
  decompressed stream.
- Each node becomes core objects as in section 4.2. Objects are appended,
  each once, to a flat data file in the staging directory, with an
  in-memory table from key to offset and length. Nothing is held in memory
  beyond one chunk and that table.
- The result is a staged NAR: the data file, the table, the key and entry
  metadata of the `nar` node, the NAR's hash and size. It is filed under
  the request's URL path. Several staged NARs may share a URL path (two
  store paths with identical contents uploaded at once); each narinfo takes
  one.
- A malformed NAR is 400 and leaves nothing behind. Success is 200 with an
  empty body.
- `HEAD` on these URLs always answers 404. Nix asks before each upload and
  would skip the NAR if told it exists; a staged NAR is consumed by its
  narinfo, so narrenhaus never has one to promise.
- A staged NAR that no narinfo has claimed for one hour is deleted, as is
  everything in staging at start.

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

In order:

1. Parse. The hash in the URL must be the hash part of `StorePath`, and
   `StorePath` must be under `/nix/store`; otherwise 400.
2. Ask the server whether `<prefix><hash>` exists. If it does, drop the
   staged NAR the narinfo names, if any, and answer 200: first writer wins.
3. Take a staged NAR filed under the narinfo's `URL`. None: 400 with a body
   that says the NAR was not received.
4. The staged NAR's hash and size must equal `NarHash` and `NarSize`;
   otherwise 400 (Nix does not check this itself before uploading).
5. Build the narinfo and recipe Blobs and the root directory, appending
   them to the staged data file.
6. Rebuild the NAR from the staged objects exactly as section 6.3 does and
   compare its SHA-256 and length with `NarHash` and `NarSize`. A
   difference is 500 and is logged as a defect: it means narrenhaus cannot
   reproduce this NAR.
7. Push the reference from the staged objects (section 9.1) with the parent
   rule of jaccard-store's CLI (`MinDedup` 0.5, set explicitly).
8. Move the staged data file into the pack cache as a complete pack for
   the root (section 7.1) and write the store path's record. Answer 200.

A failure in 7 is 502 if the server could not be reached or refused, and
the staged NAR is dropped; `nix copy` then reports the failure and can be
run again. At most four pushes run at once; further narinfo requests wait.

Limits inherited from jaccard-store: 16 GiB of objects per reference by
default and 256 MiB per object; chunks are at most 1 MiB.

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
3. Found with root R. If the local record names R, answer from the record.
   Otherwise make the reference's packs local (section 7.2), read `recipe`
   and `narinfo` from the root, write the record, answer.
4. Server unreachable: answer from the local record if there is one,
   otherwise 404.

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
2. Make the packs local.
3. Read the stored narinfo. If its NarHash is not the one in the URL,
   answer 404: the reference now holds another build than the narinfo the
   client remembers.
4. Send `Content-Length: <NarSize>`, `Content-Type: application/x-nix-nar`,
   then the NAR, written by walking the `nar` entry: directory entries in
   stored order, file bytes by walking the file's chunk tree. No
   `Accept-Ranges`.

Nothing is sent before step 3 has succeeded, because after the header a
failure can only be shown by cutting the connection. Nix waits up to 300
seconds of silence.

### 6.4 Errors

- A reference whose root is not the wrapper of section 4, whose recipe is
  unknown, or whose objects are missing or do not match their keys is a
  broken reference: 404 to Nix, an error in the log, and its packs are
  dropped from the cache.
- The server not answering within 10 seconds counts as unreachable. After
  a failed dial, requests fail fast for 30 seconds instead of each waiting
  for the dial again.
- Every other path is 404.

## 7. Pack cache

### 7.1 Layout

Default directory: `<os.UserCacheDir()>/narrenhaus`.

```
origin                    the server's endpoint ID and the prefix
packs/<root hex>.idx      jaccard-store index (packfile.Index encoding)
packs/<root hex>.data     the pack's objects, uncompressed, at the index's offsets
packs/<root hex>.meta     JSON: complete or patch, the parent's root for a patch
refs/<store hash>.json    record: root, stored narinfo text, roots of the packs it needs
staging/                  NARs being uploaded
tmp/                      downloads in progress
```

- A pack is cached under its root key. It is **complete** when it holds the
  whole key set of its root (a base pack, or a pack staged by an upload),
  otherwise it is a **patch** and names its parent's root. One root's
  complete pack serves any reference with that root and any patch that
  names it as parent.
- Files are written under `tmp/` and renamed into place; `.meta` goes last
  and marks the pack valid. At start, packs without `.meta` and everything
  under `tmp/` and `staging/` are removed.
- `origin` is written on first use. If it names another server or prefix,
  narrenhaus refuses to start and names the directory and both values.
- Reading an object: find the key in the pack's index, read
  `length` bytes at `offset` from `.data`; for a patch, try the parent
  next.

### 7.2 Making a reference's packs local

- The server's answer to a lookup names one or two packs: the reference's
  own and, for a patch, its parent (a base pack). URLs in the answer expire,
  so they are used at once and never stored.
- A pack already cached is not downloaded. One download per root runs at a
  time; requests for the same root wait for it.
- A download fetches the index, then streams the data through
  decompression into `tmp/`, checking each object against its key
  (`verify.Object`). After the last pack, `fstree.CheckComplete` from the
  root must find every object. Failure discards what was downloaded and is
  a broken reference (section 6.4).
- If a cached patch's parent is no longer what the server names for the
  reference, the cached patch is dropped and the server's current packs
  are fetched.

### 7.3 Size limit and eviction

- The size of the cache is the sum of the `.data` and `.idx` files under
  `packs/`. Staging and `tmp/` are not counted.
- Every use of a pack (a narinfo read, a NAR served, an upload placing it)
  sets the modification time of its `.data` file to now. Using a patch
  also stamps its parent. Access time is not used: it is unreliable across
  mount options.
- After a pack is added, while the size exceeds the limit, the pack with
  the oldest stamp that no request is using is deleted, with its `.idx`
  and `.meta`. Records under `refs/` whose root is that pack go with it.
- Evicting a base pack leaves its patches in place; they fetch it again
  when needed.
- A request holds the packs it reads until it has finished. If everything
  left is held, the cache stays over the limit until a request ends.

## 8. Setup check

Run at the start of `serve` (which then serves regardless) and by `check`
(which then exits).

1. Read the effective configuration with `nix config show --json`
   (`nix show-config --json` on older Nix), passing
   `--extra-experimental-features nix-command`. Ask
   `nix store info --json` whether the daemon trusts the user. For a user
   it does not trust, read the configuration again with user files and
   `NIX_CONFIG` left out, because the daemon ignores those for such a user.
2. Configured means both:
   - `substituters` contains `http://<listen address>` (a trailing slash
     aside, an exact match: the URL is the key of Nix's own caches);
   - `trusted-public-keys` contains `<key name>:<base64 public key>`.
3. Configured: one line saying so.
4. Not configured: print the missing lines,

   ```
   extra-substituters = http://127.0.0.1:8484
   extra-trusted-public-keys = narrenhaus-1:<base64>
   ```

   and where they go:
   - `/etc/nix/nix.conf` is a link into the Nix store or `/etc/static`
     (NixOS, nix-darwin): the file is generated; print the same two as
     `nix.settings` options for the system configuration;
   - `/etc/nix/nix.conf` includes `nix.custom.conf` (Determinate Nix):
     `/etc/nix/nix.custom.conf`;
   - the daemon trusts the user: `~/.config/nix/nix.conf`;
   - otherwise `/etc/nix/nix.conf`, with a note that the Nix daemon must be
     restarted.
5. Always print how to upload:
   `nix copy --to 'http://127.0.0.1:8484?compression=none' <path>`.
6. If `nix` cannot be run or its output cannot be read, say so and print
   the lines of step 4 with `/etc/nix/nix.conf` as the place.

The check reads and prints; it writes nothing outside narrenhaus's own
directories.

## 9. jaccard-store

narrenhaus talks to the server through `github.com/amber-store/jaccard-store`
(`node` for the key file and dialling by endpoint ID, `client`, `packfile`,
`verify`). Today `client.Push` and `client.Pull` take a core
`*packstore.Store`. Two additions to the `client` package are part of this
project; they go to jaccard-store on a branch and a pull request, and
narrenhaus pins the release that carries them. Merging and releasing are the
owner's steps. Names below are suggestions; the requirements are what bind.

### 9.1 Push from any object source

A push that reads objects through a function or small interface
(`Get(key) ([]byte, error)`) and does everything `Push` does today: walk
the key set from the root, sketch, `push-start`, parent choice, pack
building, upload, `push-commit`, with the same `PushOptions` and
`PushResult`. `Push` becomes a caller of it; its behaviour does not change.

### 9.2 Lookup and fetch packs as files

- A lookup of a name that returns the root and the packs the server names
  (root, object count, uncompressed bytes, compressed size), or
  `ErrNotFound`, in one round trip, fetching nothing.
- Fetching one of those packs' index, and fetching its data expanded into
  an `io.Writer` (or to the caller object by object), so that a caller can
  keep packs as files. Existing `Pull` behaviour does not change.

### 9.3 Connection

One iroh connection for the life of the process, dialled on first need and
dialled again after it fails (sleep, network change), following the
reconnect pattern of `jaccard-nfs-nix-store/refs`. Go and dependency
versions follow jaccard-store's `go.mod` (core, go-iroh), so the types
agree.

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
`narrenhaus-1:<base64 of the 64-byte Ed25519 secret key>`.

`serve` logs to standard error: the check's output, the address, then one
line per upload and per served NAR, and errors. It stops on SIGINT or
SIGTERM after running requests have finished or ten seconds have passed.

## 11. Repository and build

- `~/amber-store/narrenhaus`, module `github.com/amber-store/narrenhaus`,
  flat top-level packages, the command in `cmd/narrenhaus`.
- `flake.nix` after jaccard-store's: nixpkgs `nixos-26.05`, `systems`,
  `gonixgo` from `github:draganm/gonixgo` at its latest release.
  `packages.default` is `goEnv.buildGoApplication` with
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
- A pull of a patch fetches its whole base pack, which can be as large as
  the reference again.
- Nix remembers a miss for an hour (`narinfo-cache-negative-ttl`). A path a
  colleague pushes after this laptop asked for it is not seen until then.
- `nix copy` copies whole closures, so paths that came from
  cache.nixos.org are pushed too.
- The laptop that uploaded a path remembers Nix's own narinfo for it, whose
  NAR URL narrenhaus does not serve. If that laptop deletes the path and
  wants it back within 30 days, it gets a 404 for the NAR and builds or
  substitutes elsewhere.
- Pushes running at the same time cannot be patches of one another, so a
  first upload of many related paths stores more than a later one would.
- Small store paths are one reference, one index object and one data
  object each.
- Anyone who knows the server's endpoint ID can read, replace and delete
  any reference; the prefix is a name, not a boundary. Last writer wins on
  the server; narrenhaus's first-writer check narrows the window and does
  not close it.
- Nothing on the server expires.
- With Nix older than 2.32, a narrenhaus that is not running fails builds
  unless `fallback = true` is set.

## 13. Testing

- **NAR and tree**: round trips of canonical NARs through objects and back
  give identical bytes, for a directory root, a file root with and without
  the executable flag, a symlink root, empty files and directories, a file
  of several chunks, and names that sort differently by byte than by
  locale. Malformed and non-canonical NARs are refused.
- **Narinfo**: parsing what Nix writes, the stored form, the signature
  (checked against a fingerprint computed independently and, where `nix` is
  present, by Nix itself).
- **Pack cache**: expanded reads, patch with parent, stamps, eviction order,
  held packs, the over-limit case, recovery of a directory left by a
  killed process.
- **End to end without network**: an in-process jaccard-store server over
  loopback iroh with an in-memory bucket, as jaccard-store's `e2e` and
  jaccard-nfs-nix-store's `jstest` set one up. Two narrenhaus instances
  with separate directories and one prefix: upload through one, fetch
  through the other, compare NAR bytes and narinfo.
- **With real Nix**, where the `nix` program is present: `nix copy --to` a
  small path into one instance and substitute it from another into a
  throwaway store, with the default xz upload and with
  `?compression=none`.
- **Setup check**: against captured `nix config show --json` outputs for
  each case of section 8.

## 14. Left to the builder

- Package names and boundaries, within flat top-level packages.
- The NAR, narinfo and signing library (`zombiezen.com/go/nix` and
  `github.com/nix-community/go-nix` were looked at; either, or own code),
  as long as the reader is as strict as section 5.1 asks and the writer is
  canonical.
- The decompression libraries.
- The exact names and signatures of the jaccard-store additions.
- The format of `.meta`, the records and `origin`.
- Log wording and the layout of the check's output.
