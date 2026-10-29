# narrenhaus: design

Date: 2026-10-29. Status: approved by the owner ("looks good, go ahead and
do it all"), including the calls of section 2.3.
Revised after the whole-branch review of the built program, where the fixes
that followed it changed what this document states; the decisions the
revision added are entries 23 to 33 of section 2.3, calls the owner has not
yet confirmed.
Reviewed twice against jaccard-store v0.7.0, Amber-Store Core v0.10.0 and
the source of Nix 2.26.1 and 2.35.4 (the owner's machine runs 2.35.4).
When it was approved, nothing in it had been run against a live cache; what
has been run since is in sections 8.1 and 13.

## 1. Purpose

Colleagues at one company build and run similar software on their laptops.
narrenhaus lets one laptop's build save the next laptop the same build: it
is a Nix binary cache that each person runs locally and that keeps what it
is given on a shared [jaccard-store](https://github.com/amber-store/jaccard-store)
server.

- Nix on the laptop talks to narrenhaus over HTTP on loopback, as to any
  binary cache: it uploads with `nix copy --to` and substitutes from it.
- narrenhaus takes each uploaded NAR apart into content-addressed chunks,
  packs them, and pushes them to jaccard-store, under a name prefix the
  group shares: one reference for the NAR's content and one for its
  narinfo.
- When Nix asks for a narinfo or a NAR, narrenhaus downloads the packs of
  that reference, keeps them in a local cache of bounded size, and answers
  from them. NARs go out uncompressed.

Success: on laptop A, `nix copy --to http://127.0.0.1:8484 <path>` succeeds;
on laptop B, with narrenhaus configured as a substituter and the same server
and prefix, Nix substitutes `<path>` from it and the path it imports has the
NAR hash the narinfo names. Answering a narinfo fetches no NAR content. B's
pack cache returns under its size limit whenever no request holds it above.
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
- **Two references per store path: one for the NAR content and one for the
  narinfo**, so that answering a narinfo does not pull the NAR's content.
  The narinfo is changed to match what narrenhaus serves. (The owner's
  first wording put the narinfo in the same reference as the NAR; the
  owner changed it on seeing that every narinfo answer would then download
  a whole pack.)
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
- The setup advice includes **`fallback = true`**, so that a narrenhaus
  that is not running does not fail Nix operations. The owner chose this
  over announcing a priority before cache.nixos.org (section 8.1).
- **jaccard-store's additions are made, merged and released by the
  builder** (pull request, merge, release), and **narrenhaus never
  references an unreleased jaccard-store** (section 9.4).

### 2.2 Made in design, accepted by the owner in conversation

These were proposed and the owner answered "nothing wrong" or "fine".
Three guesses of the first round (one wrapper reference holding the
narinfo, its name, and a narinfo request downloading its packs) fell with
the owner's split into two references and are not listed.

- **The content reference is named by the NAR's hash**, not the store
  path's (section 4.1).
- **Trust inside Nix's rules.** Each laptop's narrenhaus signs the narinfos
  it serves with its own local key, and the suggested configuration adds
  that key to the trusted keys. Nix's signature checking stays on for every
  other cache.
- **Object format.** Chunks and trees are Amber-Store Core objects
  (`github.com/amber-store/core`, the version jaccard-store pins) with
  core's default chunking. "No core store" means no local core store on
  disk, not a new format.
- **The recipe is small**, because a NAR is fully determined by its file
  tree. It records what the tree cannot.
- **Upload check.** narrenhaus rebuilds the NAR from what it stored and
  refuses the upload if it does not come out as the NAR it was given.
- **Where new jaccard-store code lives.** Pushing without a core store and
  fetching packs as files are added to jaccard-store's `client` package and
  released there, not re-implemented in narrenhaus (section 9).
- **After a push** the pack just built is in the local pack cache too.
- **Lookups**: one round trip to the server per request; unknown is a 404
  at once. Server unreachable and nothing cached: a quick 404, never a
  hang.
- **NAR request**: the URL resolves on its own (Nix remembers narinfos for
  30 days and may ask for the NAR without asking for the narinfo again).
  All packs are local before anything is sent; then the NAR is streamed
  with its exact length.
- **Packs are cached expanded** (index plus uncompressed data). The size
  limit counts expanded bytes.
- **Bookkeeping is plain files, no database.** Each use stamps a pack's
  file with the current time; the oldest stamp is evicted first. A small
  record per reference remembers which packs it needs.
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

Each is the designer's call and may be overruled. Entries 1 to 22 stood
when the owner approved the document. Entries 23 to 33 were added by the
revision after the review of the built program, each with its reason; the
owner has not yet confirmed them.

1. **Names of the two references.** The prefix followed by the path Nix
   asks for: `<prefix><store hash>.narinfo` and
   `<prefix>nar/<NAR hash>.nar` (section 4.1).
2. **A NAR is pushed as soon as it has been received**, without waiting
   for its narinfo (section 5.1). With Nix 2.35, which sends every NAR of
   a copy before the first narinfo, staging then drains as pushes finish.
3. The content reference's root is a wrapper directory holding the store
   path's tree and the recipe; the narinfo reference's root is the narinfo
   text itself, one small object (section 4).
4. The metadata convention of section 4.2, with values chosen to equal
   what a Linux multi-user `/nix/store` shows, so that directory objects
   can be shared with references pushed by `jaccard-store push-dir`.
5. The recipe is a small JSON object with a version, the NAR's hash and
   its size (section 4.3).
6. The NAR URL in served narinfos is `nar/<NAR hash>.nar`: the same URL
   Nix itself uploads to with `?compression=none`.
7. A narinfo `PUT` answers only when both references are on the server,
   so a successful `nix copy` means the path could be substituted at that
   moment. The work belongs to narrenhaus, not to the request: it goes on
   if Nix gives up waiting, and a repeated `PUT` joins it (section 5.3).
8. **First writer wins** for both references: one that already exists is
   never pushed again. **narrenhaus does not repair** a reference it finds
   broken; it answers 404 for it and logs it, and the cure is to delete the
   reference by hand and upload again (sections 6.4, 12).
9. `HEAD` on `nar/<NAR hash>.nar` answers truthfully, so with
   `?compression=none` Nix skips NARs the server already has. `HEAD` on a
   compressed upload URL always answers 404 (section 5.1).
10. Uploads fail fast when the server cannot be reached: a NAR `PUT` is
    refused with 502 before its body is read (section 5.1).
11. Upload compressions accepted: none, xz, zstd, gzip, bzip2. Others are
    refused with 415 (section 5.1).
12. `PUT` of anything but a NAR or a narinfo is refused with 405; `GET` of
    logs, listings, realisations and debug info is 404.
13. Pack downloads verify every object against its key and the completeness
    of the tree, and the first time a content reference is made local its
    NAR is rebuilt once and compared with its recipe and its name, because
    the server may run without verification and anyone may write to it
    (sections 6.3, 7.2).
14. A request whose packs are not local after 240 seconds is answered 404
    while the download goes on, so narrenhaus answers before Nix's own
    300-second limit (section 6.4).
15. Defaults: listen `127.0.0.1:8484`, cache size 10 GiB, priority 50,
    parent rule as jaccard-store's CLI (at least half the bytes shared);
    at once, four content pushes, eight narinfo pushes, eight content
    downloads and sixteen narinfo downloads.
16. The signing key's name is `narrenhaus-<first 8 hex digits of its public
    key>`. Nix keeps one key per name, so one name for every laptop would
    break as soon as two keys met in one configuration.
17. Staged uploads are deleted after 15 minutes without upload activity
    (section 5.1).
18. A stored narinfo is at most 1 MiB.
19. A cache directory belongs to one server and one prefix; started with
    another, narrenhaus refuses and says why (section 7.1).
20. A `check` command beside `serve` that prints the same advice and exits.
21. For a user the Nix daemon does not trust, the check reads the system
    configuration alone, since that is all the daemon honours (section 8).
22. The store directory is `/nix/store`; a narinfo for another is refused.
23. **Requests of a web page are not answered.** A request is answered
    only when its `Host` is an IP address, `localhost` or the host of
    `--listen`; a `PUT` that carries an `Origin` header is refused, and so
    is any request the browser marks as coming from another site
    (section 6.4). A page in the user's browser can reach a port on
    loopback as Nix can, and could otherwise upload into the shared prefix
    and read what the cache serves.
24. **A NAR has at most 64 GiB.** A longer one is refused at upload, and a
    reference whose recipe states more is broken (sections 5.1, 6.3).
    Checking a reference costs the hashing of its NAR and anyone may write
    a reference, so without a bound one reference could cost every laptop
    that asks for it any amount of work; the upload has the same bound so
    that a NAR one laptop takes is one every other can check.
25. **Every object read from the cache is checked against its key** before
    it goes out in a NAR or, as part of a narinfo, under the laptop's
    signature (sections 6.2, 6.3). The cache syncs nothing and takes a
    pack for whole by the sizes of its files, so bytes that changed on
    disk would otherwise be served as what a colleague pushed.
26. **The packs of a reference stay held after it was made local** until a
    request takes the reference, or for 15 minutes if none comes
    (section 7.3). Section 6.4 promises a request that gave up that the
    next one finds the work done; where the reference does not fit the
    cache limit its packs would be evicted as the download ended, and the
    next request would begin the same download. Fifteen minutes is the
    time staged NARs get.
27. **A lookup waits 3 seconds for its answer on a connection that was
    already up**, and is then repeated on a fresh one (section 9.3). A
    connection the server gave up while the laptop slept still takes
    requests and answers none; a lookup that waited its whole 10 seconds
    on it had no time left for the repeat, and Nix remembers the 404 for
    an hour. The cost: a server that takes longer than 3 seconds to answer
    a lookup has its connection dialled again.
28. **An xz dictionary above 64 MiB and a zstd window above 128 MiB are
    refused** (section 5.1). Both are memory a body makes its reader hold,
    for xz up to 4 GiB; 64 MiB is the dictionary of `xz -9`, the largest
    Nix writes, and 128 MiB is what the zstd program reads without being
    told to read more.
29. **Which content addresses a narinfo may carry**: those Nix 2.35.4
    reads and can check a path with (section 5.3 step 1). Under first
    writer wins, a narinfo that Nix refuses or fails over would stay on
    the server for everyone. `git:` and BLAKE3 addresses are taken although
    a Nix without the experimental features refuses them, because refusing
    them would fail the uploads of users who have the features on.
30. **A cache directory that is not empty and not a cache is refused**
    (section 7.1). A start empties `staging/` and `tmp/` in its cache
    directory, which in a home or project directory given by mistake
    would delete the user's files.
31. **A second SIGINT or SIGTERM ends the process at once**, with exit
    status 1 (section 10). Stopping waits for requests and then for the
    work to end, and whoever signals twice does not want to wait. The
    command keeps the signals for this: a shell starts the background jobs
    of a script with SIGINT ignored, and a second signal left to the
    system would then do nothing.
32. **A content push that failed for good yields to the server.** When a
    `HEAD` or a narinfo `PUT` finds the content reference on the server
    after all, the refusal is forgotten (sections 5.1, 5.3). The refusal
    was of that push. Once someone else has pushed the content, `HEAD`
    tells Nix not to send the NAR again, so with `?compression=none`
    nothing else would replace the entry and the narinfo would be refused
    until a restart.
33. **The advice follows `NIX_USER_CONF_FILES`** (section 8 step 4). Where
    that variable is set Nix reads the files it names and not
    `~/.config/nix/nix.conf`, so advice that names the default file would
    send the user to a file Nix does not read.

## 3. Goals, non-goals, anti-goals

Goals

- A working binary cache for `nix copy --to` and for substitution, backed
  by jaccard-store, as section 1 describes.
- Similar store paths share storage on the server through jaccard-store's
  base and patch packs, because files are chunked by content.
- Narinfo answers that cost a lookup and a few hundred bytes, not a pack
  of NAR content.
- Bounded local disk use for what is served.
- Clear, correct setup advice.

Non-goals (first version)

- `post-build-hook` integration and any upload queue that outlives the
  process.
- Service manager units, a NixOS or nix-darwin module.
- Editing Nix configuration.
- Access control, verification of builders, a shared signing key.
- Retention and repair: nothing deletes or replaces references on the
  server.
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
- a running narrenhaus makes Nix hang or fail a build or a substitution
  because the server or the bucket is slow, down or unreachable (it
  answers 404 in time instead);
- it keeps a core store or any second copy of objects besides pack files;
- it changes a Nix configuration file or needs root;
- a real server endpoint ID or a machine's name enters the tree or a
  commit. Tests use the RFC 8032 test-vector key, examples use prefixes
  such as `acme/`.

## 4. What the references hold

A store path `/nix/store/<H>-<name>` whose NAR has the SHA-256 `<N>` (in
Nix's base 32, 52 characters; `<H>` is 32) has two references.

### 4.1 Names

| Reference | Name | Example with prefix `acme/` |
|---|---|---|
| narinfo | `<prefix><H>.narinfo` | `acme/0c6kzph7l0dcbfmjap64f0czdafn3b7x.narinfo` |
| content | `<prefix>nar/<N>.nar` | `acme/nar/<52 characters>.nar` |

A name is the prefix, verbatim, followed by the path Nix asks narrenhaus
for. The prefix is an opaque string; narrenhaus adds no separator and
normalises nothing, so a prefix wants its own trailing separator. Names
must pass core's `reference.ValidateName`; a prefix that cannot is refused
at start.

The narinfo reference is found by the store path and names the content
through the narinfo's `NarHash`. The content reference is named by what it
holds. Two store paths with identical contents share one content
reference, and two builds of one store path that differ have two, of which
the narinfo names one.

### 4.2 Content reference

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

The root is a directory with exactly two entries, in name order:

| Name | Kind | Content |
|---|---|---|
| `nar` | what the NAR's root node is: directory, regular file or symlink | the store path itself |
| `recipe` | regular file | the recipe (section 4.3) |

Because `nar` is an entry, a store path that is a single file keeps its
executable flag in the entry's mode, and one that is a symlink keeps its
target in the entry, which a bare root key could not carry.

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
`recipe` is regular, not executable. When reading, a file is executable
when `mode & 0o100 != 0`.

These values are what core's ingest records for a store path on a Linux
multi-user install, so directory objects of narrenhaus references can equal
those of references pushed with `jaccard-store push-dir`, provided the
files there carry no extended attributes (an SELinux host labels every
file, and ingest records the label) and no `.amberignore` applied. File
chunks are shared regardless. The convention is frozen once a reference has
been pushed.

Every object is reachable from the root through core's `fstree.ChildKeys`,
so the reference passes a verifying jaccard-store server as well as one run
with `--no-verify`. Two laptops given the same NAR build the same root.

### 4.3 Recipe

The recipe says how the NAR is rebuilt from the content reference and what
must come out. Version 1 is a JSON object written exactly so, keys in this
order, no spaces, no trailing newline, `narSize` in decimal:

```
{"format":"narrenhaus-recipe","narHash":"sha256:<N>","narSize":<decimal>,"node":"nar","version":1}
```

Its meaning: the NAR is the canonical `nix-archive-1` serialisation of the
entry named by `node` in the root directory: directories list their entries
in byte order of name, a regular file is `executable` when its mode says
so, its contents are the file's bytes, a symlink's target is the entry's
link target. Nothing else is needed, because a canonical NAR is a function
of its tree. The result is `narSize` bytes long and hashes to `narHash`,
which is also the hash in the reference's name.

A reader that finds another `format` or a `version` it does not know must
not serve the reference: it answers 404 and logs it, and does not count the
reference as broken (section 6.4), since a newer narrenhaus may have
written it. This is the room for a later recipe that reproduces something
the tree cannot.

### 4.4 Narinfo reference

The root is the content key of the stored narinfo text (section 5.2), built
as a file: one Blob, or, where the chunker cuts a text above 32 KiB, a
FileNode over Blobs. There is no directory around it. It is pushed as a
base pack of that one object or those few (`MinDedup` above 1, so no parent
is ever chosen); a reader takes whatever packs the server names.

## 5. Upload

Nix uploads a path as two requests: its NAR and its narinfo. Up to Nix 2.34
each path is sent as its NAR, then its narinfo. From 2.35 a `nix copy`
sends **all** NARs first, largest first, and then the narinfos, leaves of
the reference graph first, as many at once as the machine has cores.

narrenhaus ties the two together by the NAR's hash, which it computes from
the NAR and reads from the narinfo's `NarHash`; the narinfo's `URL` field
is not used for that.

**Work.** Upload work belongs to narrenhaus, not to the request that
started it. It runs under the process's context and goes on if the client
disconnects. There is one piece of content work per NAR hash and one piece
of narinfo work per store hash; a request for one that is queued or running
joins it and gets its result.

**State.** Per NAR hash the process keeps an entry only while its content
work is queued or running, has failed for now (with its staged NAR and the
last error), or has failed for good (with the reason, until the next NAR
`PUT` for that hash, or until a `HEAD` or a narinfo `PUT` for it finds the
content reference on the server). A finished push leaves no entry: from
then on narrenhaus goes by what the server says.

**Order.** At most four content pushes run at once; the rest queue. Queued
content work that a narinfo `PUT` has asked for, at any time in this
process, runs before work no narinfo has asked for, each kind in the order
it was first asked or arrived. Without this, Nix 2.35's order (largest NARs
first, narinfos of leaf paths first) would leave each narinfo waiting
behind the whole closure. Narinfo pushes have their own limit of eight and
never wait behind content pushes.

**Failure.** A push has failed for good when the server answers
`bad_request` or `malformed_pack` (for example a pack over the server's
size limit). Every other failure is for now: the server or bucket
unreachable, a lost connection, a bucket error, the server's `internal`, an
upload that outlived the server's upload timeout.

**Error bodies** are plain text under 1024 bytes, because Nix shows a body
only below that size.

**Web pages.** A `PUT` that is a web page's by the rule of section 6.4 is
answered 403 with the reason, before anything of it is read.

### 5.1 `PUT /nar/<hash>.nar[.<ext>]`

1. narrenhaus needs a usable connection to the server before it reads the
   body (section 9.3: the shared connection, or a dial within the 10
   seconds of section 6.4). Without one it answers 502 and reads nothing,
   so a copy fails at its first NAR and not after staging a whole closure.
2. The body is decompressed as it arrives. The compression is recognised by
   the body's first bytes (xz, zstd, gzip, bzip2, or the `nix-archive-1`
   header of an uncompressed NAR), not by the extension: Nix gives gzip
   uploads no extension. Anything else is 415. zstd input may be several
   frames back to back (Nix 2.35 writes one per 16 MiB). A compressed body
   may make its reader hold no more memory than those of Nix do: an xz
   stream with a block that states a dictionary above 64 MiB, and a zstd
   frame that states a window above 128 MiB, fail where they are read and
   are 400.
3. The decompressed stream is parsed as a NAR by a strict reader. It
   rejects what Nix's own parser rejects and trailing bytes besides:
   unsorted or duplicate names; names that are empty, `.`, `..`, longer than
   255 bytes or contain `/` or NUL; symlink targets that are empty, contain
   NUL or exceed 4095 bytes; nesting deeper than 64 levels; an `executable`
   marker with a non-empty value; non-zero padding; bytes after the root
   node. While parsing, narrenhaus computes the SHA-256 and length of the
   decompressed stream: the NAR's hash `<N>` and size. The stream may have
   at most 64 GiB, and the reading stops at the read that passes the
   limit. It is the number a recipe's `narSize` is held to when a
   reference is checked (section 6.3), so a NAR one laptop takes is one
   every other can check.
4. Each node becomes core objects as in section 4.2. Objects are appended,
   each once, to a flat data file in the staging directory, with a table
   from key to offset and length. Then the recipe and the root directory
   are appended. Memory in use is the chunker's buffer (2 MiB) and the
   table; when the NAR is complete the table is written beside the data
   file (it becomes the pack's index), so memory does not grow with the
   number of staged NARs. After this the staged file and table do not
   change and are read by position, so concurrent reads are safe.
5. If the body was an uncompressed NAR, the hash in the URL must be `<N>`
   (Nix names an uncompressed upload by its NAR hash); otherwise 400. For
   a compressed body the URL's hash is that of the compressed file and is
   not checked.
6. The NAR is rebuilt from the staged objects exactly as section 6.3 does,
   and its SHA-256 and length compared with what was received. A difference
   is 422 with a body that says narrenhaus cannot reproduce this NAR, and
   is logged as a defect.
7. Content work for `<N>` starts and the request is answered 200 with an
   empty body; it does not wait for the push. If an entry for `<N>` exists:
   queued or running, the new staged copy is dropped; failed for now, the
   new copy is dropped and the work starts again on the staged one; failed
   for good, the entry is replaced and the work starts on the new copy.

A malformed NAR is 400, a failed write to staging is 507; both leave
nothing behind. Neither does a NAR that is beyond a limit, which no repeat
cures and which is therefore 422 with a body that names the limit: a NAR of
more than 64 GiB, logged as a warning, and a NAR that a pack cannot hold
(more than 2^24 objects, an object above 256 MiB, or a key that is not
canonical), logged as an error. The cache tells such a limit from a write
that failed.

Content work for `<N>`:

- Ask the server whether `<prefix>nar/<N>.nar` exists. If it does, drop
  the staged NAR: first writer wins. If its root is not the one staged,
  log both roots as a warning: it was written by another recipe version or
  by something else, and whether it can be served shows on a pull.
- Otherwise push the reference from the staged objects (section 9.1) with
  the parent rule of jaccard-store's CLI (`MinDedup` 0.5, set explicitly)
  and `TempDir` set to the cache directory's `tmp/`. Then move the staged
  data file into the pack cache as a complete pack for the root
  (section 7.1) and write the reference's record.
- Failed for now: the NAR stays staged and the entry stays; the next
  request that needs `<N>` starts the work again. Failed for good: the
  staged NAR is dropped.

`HEAD /nar/<hash>.nar` with a 52-character hash and no extension is Nix's
question "do I need to send this NAR" and nothing else. It answers 200 when
content work for the hash is queued or running in this process or the
server has the reference, 404 when neither, and 502 when the server could
not be asked. When the server has the reference and the entry for the hash
is one that failed for good, the entry is dropped: Nix sends no NAR after a
200, so the narinfo that follows must not meet the refusal of a push that
is past. It downloads nothing and sends no `Content-Length`. With
`?compression=none` Nix asks this before each upload and skips a NAR that
is already there. `HEAD` on a URL with an extension always answers 404: its
hash is that of the compressed file, which narrenhaus does not keep. A gzip
upload has no extension either; its hash names no content reference, so it
gets 404 too.

Staged NARs are deleted when 15 minutes have passed during which no upload
request was being handled and no push was running or queued, and everything
in staging is deleted at start. Age alone never deletes one. Deleting a
staged NAR removes its entry.

### 5.2 The narinfo as stored

`PUT /<H>.narinfo` carries Nix's narinfo. narrenhaus parses it (unknown
keys are ignored, as Nix does) and stores this text:

- `StorePath`, `NarHash` (as `sha256:<N>`, whatever form it arrived in),
  `NarSize`, `References` (always present, `References: ` with the
  trailing space when empty), `Deriver`, `Sig` lines and `CA` as uploaded;
- `URL: nar/<N>.nar`;
- `Compression: none`;
- `FileHash` and `FileSize` equal to `NarHash` and `NarSize`;
- fields in the order Nix writes them (StorePath, URL, Compression,
  FileHash, FileSize, NarHash, NarSize, References, Deriver, Sig sorted,
  CA), every line ended by a line feed.

This is what Nix itself writes for an upload with `?compression=none`. The
laptop's own signature is not stored; it is added when serving
(section 6.2). Uploaded signatures stay valid, because a signature covers
only the store path, NarHash, NarSize and references.

### 5.3 `PUT /<H>.narinfo`

In order:

1. Parse. The hash in the URL must be the hash part of `StorePath`,
   `StorePath` must be under `/nix/store`, `NarHash` must be a SHA-256,
   the stored text must not exceed 1 MiB, and the body must be plain text
   (a `Content-Encoding` header is 415); otherwise 400. A `CA` line that
   is not empty must be a content address Nix 2.35.4 reads and can check a
   path with: `text:sha256:<digest>` or
   `fixed:[r:|git:]<algorithm>:<digest>`, the algorithm one of md5, sha1,
   sha256, sha512 and blake3 (after `git:` only sha1 or sha256), the
   digest in base 16, Nix's base 32 or base 64. A text address must not
   have the path itself among its references, and a fixed address other
   than `fixed:r:sha256:` must have no references. Anything else is 400:
   Nix refuses such a narinfo, or fails when it checks the path, and under
   first writer wins it would stay. `git:` and BLAKE3 addresses are taken
   although a Nix without the experimental features `git-hashing` and
   `blake3-hashes` refuses the whole narinfo, which stays so for
   colleagues without the feature.
2. Make sure the content for `NarHash` is on the server.
   - An entry that is queued or running: mark it as asked for by a narinfo
     and wait for it.
   - Failed for now: start it again and wait.
   - Failed for good: ask the server whether `<prefix>nar/<N>.nar` exists.
     If it does (someone else has pushed it since the refusal), drop the
     entry and go on. If it does not, or the server cannot be asked, 422
     with the reason.
   - No entry: ask the server whether `<prefix>nar/<N>.nar` exists. If not,
     400 with a body that says no NAR with hash `<N>` was received.

   While an entry exists, `NarSize` must equal the staged NAR's size;
   otherwise 400.
3. Ask the server whether `<prefix><H>.narinfo` exists. If it does, answer
   200: first writer wins.
4. Build the stored narinfo, push the narinfo reference from that one
   object as a base pack, place its pack in the cache and write its
   record. Answer 200.

The content reference is always on the server before the narinfo that
names it, and a 200 means both were there when it was given.

| Outcome | Status |
|---|---|
| Both references on the server | 200 |
| Malformed narinfo, or `NarSize` differs from the staged NAR | 400 |
| No content for `NarHash` here or on the server | 400 |
| Content work failed for good and the server does not have the content, or the server refuses the narinfo push with `bad_request` or `malformed_pack` | 422 with the reason |
| Server or bucket unreachable, connection lost | 502; the NAR stays staged |
| The request is a web page's (section 6.4) | 403 |

Nix repeats a request that got 5xx or no answer, and the repeat finds the
work or the staged NAR still there. Nix gives up on a request that has been
silent for `stalled-download-timeout` (300 seconds by default) and, in
2.35, repeats it up to four more times; the repeats join the work under
way. So a narinfo may wait about 25 minutes in all for its own content
push, the pushes already running and those of narinfos asked for before
it. Beyond that the copy fails, unless `nix copy` is run with
`--option stalled-download-timeout <seconds>`. Running the copy again
after the pushes have finished sends, with `?compression=none`, no NAR
again and only the narinfos; with a compression every NAR is compressed
and sent again and then dropped.

Limits inherited from jaccard-store: 16 GiB of uncompressed objects per
pack by default (the server's `--max-pack-size`, refused only after the
pack has been built), 256 MiB per object (chunks are at most 1 MiB), 2^24
objects per pack, and one hour from the start of an upload to its commit
(the server's `--upload-timeout`).

## 6. Serving

All answers work for `GET` and `HEAD` alike, `HEAD` without a body, except
`HEAD` on a NAR URL, which section 5.1 defines.

### 6.1 `GET /nix-cache-info`

```
StoreDir: /nix/store
WantMassQuery: 1
Priority: <priority>
```

### 6.2 `GET /<H>.narinfo`

1. Ask the server for `<prefix><H>.narinfo` (one round trip, section 9.2).
2. Not found: 404, and any local record for the name is removed.
3. Found with root R. Before anything is downloaded: R must be a Blob or
   FileNode key, its length field at most 1 MiB, and the packs the server
   names must announce no more than 4 MiB of objects in all and no more
   than 1024 objects each (an index is read whole before any object, and
   empty objects announce no bytes); otherwise the reference is broken
   (section 6.4). `HEAD` answers 200 here.
4. If the pack for R is not cached, download it (section 7.2): an index of
   a few dozen bytes and a data object about the size of the narinfo. Read
   the text with `fstree.WriteContent`, from at most 1024 objects, each
   checked against its key, also when the pack was cached before. The text
   must parse as an upload's must (section 5.3 step 1), its `StorePath`
   must have the hash `<H>`, its `URL` must be `nar/<its NarHash>.nar` and
   its `Compression` `none`; otherwise the reference is broken. Write the
   record. An object of a cached pack that does not match its key is a
   damaged cache and no broken reference (section 6.4): the answer is 404,
   nothing is signed, the pack is dropped and the next request fetches it.
5. Server unreachable: answer from the local record and its pack if they
   are there, otherwise 404.

The answer is the stored narinfo with one more `Sig` line: the laptop's
signature over Nix's fingerprint
`1;<store path>;<NarHash as sha256:nix32>;<NarSize>;<references as full
store paths, sorted, joined by commas>`, as `<key name>:<base64 of the
64-byte Ed25519 signature>`. Content type `text/x-nix-narinfo`.

No NAR content is fetched to answer a narinfo, and the content reference is
not looked at.

### 6.3 `GET /nar/<N>.nar`

1. Find the root: from the local record of `<prefix>nar/<N>.nar` if its
   packs are all cached, otherwise by asking the server (not found, or
   unreachable without a usable record: 404). A root that is not a DirLeaf
   or DirNode key is broken, without a download.
2. Make the reference local (section 7.2). For a content reference the
   last step of that is a check, made once per root: read `recipe` (unknown
   format or version: section 4.3; a `narHash` other than `<N>`: broken),
   rebuild the NAR from the tree and compare its SHA-256 and length with
   the recipe's `narHash` and `narSize` (a difference is broken). A recipe
   that states a `narSize` above 64 GiB is broken without a rebuild, and a
   rebuild stops at the first byte beyond `narSize`, so the check of any
   reference, however it was built, costs no more than the hashing of
   64 GiB. A tree in which a FileNode has a child of no bytes, a DirNode
   has no child, or a DirNode has a DirLeaf without an entry as a child is
   broken: core's builders make none of these, and such an index could
   otherwise name one object any number of times and keep its reader at
   work for nothing. Every object read for the check is checked against
   its key. The check runs under the process's context like the download,
   later requests join it, and its result is the "checked" mark in the
   record. A pack placed by a local upload passed it in step 6 of
   section 5.1.
3. Send `Content-Length: <narSize>`, `Content-Type: application/x-nix-nar`,
   then the NAR, written by walking the `nar` entry: directory entries in
   stored order, a file's length from its content key (`key.Length()`), its
   bytes with `fstree.WriteContent`. No `Accept-Ranges`. Each object is
   checked against its key before its bytes are sent; one that fails is a
   damaged cache (section 6.4), and what was sent until then is right and
   is not the whole NAR. If an object fails so, or if the bytes written
   would differ from a file's stated length or from `narSize`, the writer
   stops, cuts the connection, logs a defect and drops the reference's
   pack from the cache, so that the next request fetches it again.

Nothing is sent before step 2 has succeeded, because after the header a
failure can only be shown by cutting the connection.

### 6.4 Time limits and errors

Nix abandons a request that is silent for 300 seconds and then counts it as
an error, which can fail a build; a 404 it survives. So when serving
(narinfo `GET` and `HEAD`, NAR `GET`) narrenhaus answers 404 in time and
never 5xx before the response header. Upload requests are different: they
answer 502 when the server cannot be reached, so that a copy fails fast
and says why.

- **Lookup.** One request waits at most 10 seconds for the connection and
  the lookup round trip together, of which at most 3 on a connection that
  was already up, before the lookup is repeated once on a fresh one
  (section 9.3). Beyond that the server counts as unreachable. Pushes
  (`push-start`, `push-upload`, `push-commit`) have no such limit: a
  verifying server is silent for as long as it verifies.
- **Dial.** The dial runs under the process's context with a 30-second
  limit and is not abandoned when a waiting request gives up. After a dial
  has failed, requests fail fast for 30 seconds instead of dialling again.
- **Packs.** If a request's reference is not local (and, for content,
  checked) 240 seconds after the request arrived, it is answered 404 and
  the work goes on (section 7.2), so a later request finds it done; what
  the work brought is kept for that request (section 7.3).
- **Transfer failure.** A bucket error, a stalled or cut transfer, an
  expired URL or a full disk is not a broken reference: the pack being
  downloaded is discarded, packs already verified are kept, the request is
  answered 404 and the cause is logged.
- **Broken reference.** A root of the wrong kind or size (sections 6.2,
  6.3), a narinfo that fails the checks of section 6.2, an index or object
  that does not match its key when it is downloaded, a tree that is
  incomplete after all packs are local or has a form section 6.3 refuses,
  a recipe that states more than 64 GiB, or a NAR that does not match its
  recipe and name. It is 404 to Nix and an error in the log, and its own
  pack is dropped from the cache if it was fetched for it. Its parent pack
  stays: it was checked object by object when it came, and it is the base
  of other references. The process remembers the name and root, so later
  requests that find the same root there are answered 404 without
  downloading again. narrenhaus does not replace a broken reference
  (section 12).

  The verdict is passed only on packs that came from the server for this
  piece of work. If a reference looks broken and one of its packs came
  from the cache, the cached packs are dropped and fetched again, and the
  verdict is passed on what comes then, by the same work and for the same
  request. A pack that the work of another reference drops meanwhile is
  fetched again in the same way.
- **Damaged cache.** An object of a cached pack that does not match its
  key when it is read (section 7.1) is the disk's doing and no broken
  reference. The pack is dropped and nothing is remembered against the
  reference; the pack is fetched again by the work that met it, or by the
  next request when a request met it while it read a narinfo or sent a
  NAR.
- **Requests of a web page.** A port on loopback can be reached by the
  pages in the user's browser. narrenhaus answers a request only when its
  `Host` header is an IP address, `localhost` or the host of `--listen`,
  each with or without a port. It refuses a `PUT` that carries an `Origin`
  header, and any request whose `Sec-Fetch-Site` header is there and is
  neither `none` nor `same-origin`. Nix calls the cache by its address or
  by the name it was given, and sends neither header. A refused `PUT` is
  answered 403 with the reason and anything else 404, before the path is
  looked at or a body read, and the refusal is logged as a warning.
- Every other path is 404.

## 7. Pack cache

### 7.1 Layout

Default directory: `<os.UserCacheDir()>/narrenhaus`.

```
origin                    the server's endpoint ID and the prefix
packs/<root hex>.idx      jaccard-store index (packfile.Index encoding)
packs/<root hex>.data     the pack's objects, uncompressed, at the index's offsets
packs/<root hex>.meta     JSON: complete or patch, the parent's root for a patch
refs/…                    one record per reference: root, roots of the packs it needs, checked or not
staging/                  NARs being uploaded
tmp/                      downloads and packs being built
```

- A pack is cached under its root key. It is **complete** when it holds the
  whole key set of its root (a base pack, or a pack staged by an upload),
  otherwise it is a **patch** and names its parent's root. One root's
  complete pack serves any reference with that root and any patch that
  names it as parent. Narinfo packs and content packs share the one cache
  and the one eviction order; their roots are keys of different kinds and
  cannot collide.
- A record exists only while its root's pack is cached. It holds no narinfo
  text and no object bytes.
- Files are written under `tmp/` and renamed into place; `.meta` goes last
  and marks the pack valid. At start, packs without `.meta` and everything
  under `tmp/` and `staging/` are removed.
- `origin` is written on first use. If it names another server or prefix,
  narrenhaus refuses to start and names the directory and both values. A
  directory without an `origin` is taken for a new cache only if it is
  empty, or holds nothing but what a first start leaves that ended before
  it wrote the `origin`: the lock, the four directories, their probe files
  and the temporary file the `origin` is written to. Any other directory
  is refused as not being a cache directory, named in the error and left
  as it was, because a start empties `tmp/` and `staging/`. An `origin` is
  one that can be read: a link of that name that leads nowhere is none,
  and a directory that holds it is refused.
- Reading an object: find the key in the pack's index, read `length` bytes
  at `offset` from `.data` by position; for a patch, try the parent next.
  The reader is safe for concurrent use; `fstree.CheckComplete` and the
  push's key-set walk call it from several goroutines. The cache checks
  nothing it reads against a key and syncs nothing to disk; what is given
  out of it is checked object by object first (sections 6.2, 6.3).

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
  time. At most eight content downloads run at once; narinfo packs have a
  limit of their own (sixteen) and never wait behind a content download. A
  transfer that moves no byte for 30 seconds has failed.
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
  The work that makes a reference local holds them as well, and its hold
  stays after the work has ended until a request has taken the reference,
  or for 15 minutes if none comes: the request that began the work may
  have given up (section 6.4), and the one that asks again is to find the
  packs.
- A pack placed under a root that is held in wait for it belongs to the
  holders from that moment: if it is dropped before they have read it, its
  files stay until they let go.
- Whenever a pack is added or a request lets go of its packs, while the
  size exceeds the limit, the pack with the oldest stamp that no request
  holds is deleted, with its `.idx` and `.meta`. Records under `refs/`
  whose root is that pack go with it.
- Evicting a base pack leaves its patches in place; they fetch it again
  when needed.
- If everything left is held, the cache stays over the limit until a
  request ends. A limit smaller than one reference's packs is honoured only
  between requests, and not for up to 15 minutes after a download whose
  request had given up.

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
   - `fallback` is `true` (section 8.1). This one is read from the user's
     full view for every user, trusted or not, because the daemon always
     honours it from the client.
3. Configured: one line saying so. Where that rests on a file under
   `/etc/nix`, add that it takes effect once the Nix daemon has been
   restarted after the edit, since the check cannot see the daemon's own
   settings.
4. Not configured: print the missing lines,

   ```
   extra-substituters = http://127.0.0.1:8484
   extra-trusted-public-keys = narrenhaus-1a2b3c4d:<base64>
   fallback = true
   ```

   with one sentence on what `fallback = true` is for and what it changes
   (section 8.1). A missing `fallback` line may always go in the user's
   `nix.conf`. Where the other two go, first case that applies:
   - the daemon trusts the user: the user's `nix.conf`, which needs
     neither root nor a restart;
   - `/etc/nix/nix.conf` is a link into the Nix store or `/etc/static`
     (NixOS, nix-darwin): the file is generated; print the same as
     `nix.settings` options for the system configuration;
   - `/etc/nix/nix.conf` has an `include` or `!include` line naming
     `nix.custom.conf` (Determinate Nix): `/etc/nix/nix.custom.conf`, and
     restart the Nix daemon;
   - otherwise `/etc/nix/nix.conf`, and restart the Nix daemon.

   The user's `nix.conf` is the first file `NIX_USER_CONF_FILES` names
   when that variable is set: Nix then reads only the files it names, and
   the first has the last word. Otherwise it is
   `$XDG_CONFIG_HOME/nix/nix.conf`, or `~/.config/nix/nix.conf` without
   that variable. When `NIX_USER_CONF_FILES` is set and names no file, Nix
   reads no file of the user's: the advice names the file Nix would read
   without the variable and says that the lines count once the variable
   is unset or names that file.
5. Always print how to upload:
   `nix copy --to 'http://127.0.0.1:8484?compression=none' <path>`, and
   that very large paths may need
   `--option stalled-download-timeout <seconds>`.
6. If `nix` cannot be run or its output cannot be read, say so and print
   the lines of step 4 with `/etc/nix/nix.conf` as the place. A setting
   whose value is `null` counts as output that cannot be read.
7. If the listen address is not loopback, warn that anyone who can reach
   the port can upload into the shared prefix and get narinfos signed with
   this laptop's key.

The check reads and prints; it writes nothing outside narrenhaus's own
directories.

### 8.1 When narrenhaus is not running

Read in Nix's source for 2.26.1 and 2.35.4, and run on 2.35.4 in the cases
named at the end of this section: once narrenhaus is in `substituters`,
and Nix has its `nix-cache-info` remembered (7 days), a
narrenhaus that is not running makes Nix fail any operation that needs a
path no earlier cache has, unless `fallback = true`. Before 2.32 any dead
substituter is fatal; from 2.32 only the last one asked is, and priority 50
makes narrenhaus the last. The owner starts narrenhaus by hand, so "not
running" is an everyday state.

The owner's choice is to **suggest `fallback = true`** as a third line and
have the check report when it is off. With it Nix logs the failure,
leaves the dead cache alone for 60 seconds and goes on to other caches or
builds. Its cost, which the advice states: it applies to every cache, so a
failed download from cache.nixos.org also turns into a local build instead
of an error.

Not chosen: announcing a priority before cache.nixos.org. From Nix 2.32 a
failing narrenhaus would then be skipped, but narrenhaus would be asked for
every path Nix wants, each query would wait through retries while it is
down, and Nix before 2.32 would still fail.

What was run, by the real-Nix tests of section 13, on Nix 2.35.4 on macOS:

- The build case, through the machine's Nix daemon, which trusts the
  user. Nix is given a narrenhaus as its only substituter, as an option of
  the command and not in a configuration file, and builds a one-line
  derivation while that narrenhaus runs; it asks it for `nix-cache-info`
  and for the narinfo of the output. The narrenhaus is then stopped. A
  second such derivation, built with `fallback = false`, fails, and Nix
  names the stopped cache; a third, built with `fallback = true`, is
  built.
- A substitution into a throwaway store, by the `nix` process itself: with
  the stopped narrenhaus named before a second one that runs and has the
  path, and `fallback = true`, the path arrives and Nix says that it tried
  the stopped one. Without `fallback` the path arrived as well, as was
  read of Nix from 2.32 for a dead substituter that is not the last one
  asked; the test logs that and does not require it.

What is still read and not run: Nix 2.26.1 and every Nix before 2.32,
where any dead substituter is fatal; Linux; and a daemon that does not
trust the user, where the build case skips itself because the daemon does
not ask the substituter the user names.

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
at v0.7.0. A root of any core type works, a single Blob included (the
narinfo reference).

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
  frame fails, or when a lookup has waited on it for its answer in vain
  (for how long, the third rule of this list says); never for
  `ErrNotFound`, a refusal by the server (`*wire.Error`), a bucket error
  or a failed verification.
- Retiring takes it out of use for new calls. Of the calls that fail on
  one connection only the first retires it. It is closed when the last
  call still running on it has returned, so a push in flight is not cut by
  another request's timeout. Each dial binds its own endpoint, so the old
  and the new connection can exist together.
- On a connection that was already up, a lookup waits 3 seconds for its
  answer. If none comes, if the connection fails it, or if another call
  retires the connection while it waits, the lookup is repeated once on a
  freshly dialled one, in what is left of its 10 seconds, before the
  request counts as unreachable. The lookups that wait for a first answer
  on one connection are all released when any call retires it, so they do
  not each wait their own 3 seconds. A connection dialled for the lookup,
  and the one a lookup is repeated on, have the rest of the 10 seconds;
  silence until then retires such a connection only if the lookup waited
  on it for at least 3 seconds, so a lookup that spent its time waiting
  for the dial does not retire the connection the dial made. This matters
  after sleep: the server gives up an idle connection without the laptop
  hearing of it, the connection still takes streams and frames, and only
  the missing answer shows it. The first lookups must not answer 404,
  which Nix would remember for an hour.
- A server that takes longer than 3 seconds to answer a lookup has its
  connection retired and dialled again by that lookup; pushes in flight
  finish on the retired connection.
- Only lookups have the 3-second rule. The check before an upload's body
  is read opens a stream, which a connection that died without a word
  still takes, and a push has no time limit: neither finds such a
  connection out before a lookup retires it or the transport gives it up.
  The work of an upload begins with a lookup, so an upload after sleep
  meets the rule there.
- Dials are serialised; a caller that waited for a dial that failed gets
  that failure.

`jaccard-nfs-nix-store/refs` has a working pattern for the generation
counter and the serialised dial; its rule of dropping the connection on any
error is not taken over.

### 9.4 Order of work and release

By the owner's instruction:

- The additions are made first, in `~/amber-store/jaccard-store`, on a
  branch. The builder opens the pull request, waits for the Test workflow,
  merges, and once that workflow is green on `main` cuts the release: the
  next minor version, v0.8.0, as a tag `v*` on `main`'s commit, pushed to
  GitHub, which builds the image and the GitHub release as the earlier
  ones were made.
- narrenhaus's `go.mod` always names a released version of jaccard-store:
  no pseudo-version of a branch or commit, no `replace`. Work on narrenhaus
  that needs the additions starts after the release exists. If that work
  shows the additions lacking, the correction takes the same route (pull
  request, merge, a further release) and `go.mod` moves to it.
- The additions change no behaviour of the existing commands or of the
  server, so the running server needs no rollout.
- Go and dependency versions follow jaccard-store's `go.mod` (core,
  go-iroh), so the types agree.

## 10. Command line

`narrenhaus serve` and `narrenhaus check`, on `urfave/cli/v2`. Every option
has a flag and an environment variable.

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--server` | `NARRENHAUS_SERVER` | required | jaccard-store server's endpoint ID |
| `--prefix` | `NARRENHAUS_PREFIX` | required | prefix of reference names, verbatim |
| `--listen` | `NARRENHAUS_LISTEN` | `127.0.0.1:8484` | HTTP address; when its host is a name, also the one name, besides `localhost`, that requests may call the cache by (section 6.4) |
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

In order: no new request is taken, and the running ones have their ten
seconds. Those that still run then are cut, their connections closed, and
waited for up to five seconds more. Then the work is stopped, and the
uploader, the store and the cache are closed, in that order and only now:
a request stands on them. If a request that was cut still runs after the
five seconds, `serve` says so in the log, stops the work and returns
without closing the three. An upload that is being staged notices the
stopped work at its next object, drops what it staged and answers 502;
the end of the process gives up the cache directory, and what is left in
staging goes at the next start.

A second SIGINT or SIGTERM ends the process at once with exit status 1,
without waiting for requests or work, however the process was started.

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
- gonixgo runs, in the build, the tests of the packages the command
  contains. Tests that need the `nix` program, a network or a container
  skip themselves when it is missing. The package `e2e` is test files
  alone, so the build does not run it; `go test ./e2e/` does. Of its cases
  with two instances, the upload to one that the other serves is also a
  test of the command, which the build runs.
- `README.md` covers what it is, setup, the options and the known limits.

## 12. Known limits

- **Staging is outside the cache limit and nothing bounds it.** With Nix
  2.35 and later a `nix copy` sends all NARs before the first narinfo, and
  they arrive faster than they can be pushed, so a copy needs free disk for
  the uncompressed size of what it copies, on top of the cache limit and of
  up to four compressed packs being built. It drains as pushes finish. A
  full disk fails the copy with 507.
- **narrenhaus does not repair.** A reference found broken stays, and the
  path is not substitutable from narrenhaus, until someone deletes the
  reference (`jaccard-store rm`) and uploads again. The same holds for a
  content reference deleted while its narinfo remains: the narinfo is
  answered, the NAR is 404, and Nix builds the path or looks elsewhere. A
  broken pack that other names or patch packs still hold cannot be
  replaced by any client.
- A pull of a patch fetches its whole base pack, which can be as large as
  the reference again.
- A NAR request whose packs take more than 240 seconds to arrive and check
  gets a 404, and Nix builds the path or looks elsewhere. The download
  goes on, and what it brought is kept for 15 minutes, whatever the cache
  limit, for the request that asks again.
- **A NAR of more than 64 GiB can be neither uploaded nor served.** The
  upload is refused (422), and a reference whose recipe states a longer
  NAR is broken. An upload is refused as well (422) when its NAR comes
  apart into more than 2^24 objects.
- A compressed upload that makes its reader hold more memory than those
  of Nix do, an xz dictionary above 64 MiB or a zstd window above 128 MiB,
  is refused (400).
- The first request after the laptop slept can take about three seconds
  longer: the connection to the server may have died without a word, and
  a lookup waits that long on it before it dials again. A push that was
  under way on such a connection fails only when the transport gives the
  connection up.
- What is read from the cache is checked against its key, so a cached file
  damaged on disk is found out and not served: a narinfo is answered 404,
  a NAR that was being sent is cut short, the pack is dropped and the next
  request fetches it again.
- `--cache-dir` must name a directory that is empty, not there yet, or a
  narrenhaus's own; any other is refused and left as it was.
- narrenhaus answers under an IP address, `localhost` or the host of
  `--listen` and under no other name: a substituter URL that calls it
  otherwise gets 403 for an upload and 404 for everything else. The same
  goes for a client that sends an `Origin` header with a `PUT`.
- A narinfo whose content address Nix 2.35.4 refuses or fails over is
  refused at upload (400). One with `git:` hashing or BLAKE3 is taken
  although a Nix without the experimental features `git-hashing` and
  `blake3-hashes` refuses the whole narinfo; under first writer wins the
  path stays of use only to colleagues who have the feature on.
- Where `NIX_USER_CONF_FILES` is set and names no file, Nix reads no
  configuration file of the user's, and the advice can only say so beside
  the lines (section 8).
- Nix remembers a narinfo 404 for an hour
  (`narinfo-cache-negative-ttl`). That covers a path a colleague pushes
  after this laptop asked for it, and 404s given while the server was
  unreachable.
- A narinfo `PUT` that waits longer than Nix's 300-second limit shows as
  repeated requests, and beyond about 25 minutes as a failed copy
  (section 5.3). On a verifying server every commit, a narinfo's included,
  also waits for one of the server's verify jobs, shared by all clients.
- `nix copy` copies whole closures, so paths that came from
  cache.nixos.org are pushed too.
- A path uploaded with a compression (xz is Nix's default) leaves Nix's
  own narinfo, with a compressed NAR URL that narrenhaus does not serve,
  in the memory of the user account that ran `nix copy`. With
  `?compression=none`, which the advice prints, the URL Nix remembers is
  the one narrenhaus serves.
- A store path costs two references and four to six bucket objects (index
  and data per pack, and links for each base pack on a verifying server).
  A content reference whose narinfo never arrived, or lost a race, stays
  on the server.
- Small packs cost more disk than the cache counts: a cached narinfo is
  four small files, each rounded up to a filesystem block.
- Pushes running at the same time cannot be patches of one another, so a
  first upload of many related paths stores more than a later one would.
- Anyone who knows the server's endpoint ID can read, replace and delete
  any reference; the prefix is a name, not a boundary. Last writer wins on
  the server; narrenhaus's first-writer check narrows the window and does
  not close it. A narinfo is trusted to state its NAR's size truthfully.
- Nothing on the server expires.
- A configured narrenhaus that is not running fails Nix operations unless
  `fallback = true` is set, which the setup advice asks for (section 8.1).
  With it, a failed download from any cache becomes a local build.

## 13. Testing

- **NAR and tree**: round trips of canonical NARs through objects and back
  give identical bytes, for a directory root, a file root with and without
  the executable flag, a symlink root, empty files and directories, a file
  of several chunks, and names that sort differently by byte than by
  locale. Malformed and non-canonical NARs are refused, each rule of
  section 5.1. A NAR beyond the limit is refused at the read that passes
  it, with the limit lowered for the test. Trees with an index that holds
  nothing (section 6.3) are refused, and one built to be read in millions
  of steps is refused after a few reads per level.
- **Frozen keys**: a fixed small NAR (a directory holding a plain file, an
  executable, a subdirectory and a symlink) must give a hard-coded content
  root key, a fixed narinfo a hard-coded narinfo root key, and a file of
  several chunks made from seeded bytes a hard-coded content key, so a
  change in the chunking library, the encoding, the recipe or the metadata
  convention fails the build. The same tree is read back with core's own
  decoder and its modes compared.
- **Narinfo**: parsing what Nix writes (NarHash in each form Nix accepts),
  the stored form, the signature (checked against a fingerprint computed
  independently and, where `nix` is present, by Nix itself), a narinfo
  whose text the chunker splits, so that its root is a FileNode, and one
  over 1 MiB, refused. The content addresses of section 5.3: lists of
  those taken and those refused, and the cases with references. Where
  `nix` is present and is 2.31 or later (the first to take git hashing
  with SHA-256), the same lists are put to Nix itself; on an older Nix
  that test skips.
- **Pack cache**: expanded reads, patch with parent, a cached patch whose
  parent changed, stamps, eviction order, held packs, the over-limit case,
  recovery of a directory left by a killed process. A directory that is no
  cache, in several layouts and with a link named `origin` that leads
  nowhere, is refused and left as it was, and what an unfinished first
  start leaves is opened. A pack placed for a holder that waits for it
  outlives a drop until the holder lets go. An object beyond a pack's
  limits is told from a failed write.
- **References made local**: a sound reference that shares a parent pack
  with a broken one is served, also when both are made local at once; a
  reference that looks broken on cached packs has them fetched again and
  its verdict passed by the same request; a pack dropped under a
  reference is fetched again. A cache altered on disk (a byte of a file,
  a block of zeroes, a byte in a patch's parent, a digit of a narinfo) is
  neither served nor signed. A recipe that states more than 64 GiB is not
  rebuilt, and a rebuild ends where its recipe's length does. A narinfo
  whose packs announce more than 1024 objects is refused before a
  download, and one whose index names more is not read beyond them. A
  reference made local after its request gave up is kept, and let go
  after its time, at the end, or by the request that takes it.
- **Connection**: a lookup on a connection the server lost without a word
  is answered from a fresh one after the wait, with two dials in all;
  lookups that wait on such a connection are released by the first; a
  fresh connection that is silent too is not tried a third time; a lookup
  that spent its limit on the dial retires nothing.
- **Uploads**: beside the cases below, a NAR beyond the limit, plain and
  compressed and one without end (422, nothing staged, read no further
  than the limit); a body that asks for too much memory (400); the limits
  of a pack as 422 and a failed write as 507; a NAR that does not rebuild
  from what was staged (422); a refusal for good that yields once the
  server has the content, by `HEAD` first and by the narinfo alone; an
  upload cut off at its next object when the uploader is closed.
- **Defaults**: the time limits, and the numbers of things done at once,
  that the program takes when nothing is configured are compared with
  this document by tests of the packages that hold them.
- **End to end without network**: an in-process jaccard-store server over
  loopback iroh with an in-memory bucket, as jaccard-store's `e2e` and
  jaccard-nfs-nix-store's `jstest` set one up. Two narrenhaus instances
  with separate directories and one prefix: upload through one, fetch
  through the other, compare NAR bytes and narinfo, and assert that the
  narinfo answer fetched no content pack. The instances are put together
  as the command puts them, with one limit changed: they wait 300 ms, not
  3 s, on a connection that was already up before a lookup is repeated.
  Also:
  - all NARs before any narinfo, behind a slowed push: the content a
    narinfo asks for is pushed before content nobody waits for;
  - two store paths with one content;
  - a narinfo `PUT` joining a slowed content push;
  - a 502 that leaves the NAR staged, and a repeat that succeeds;
  - with the server down, a NAR `PUT` and a NAR `HEAD` answer 502 and
    staging stays empty;
  - a narinfo `PUT` after a restart (400, no NAR received);
  - `HEAD` 200 for queued work that then fails for good (422 on the
    narinfo, 404 on the next `HEAD`);
  - a narinfo `PUT` whose reference exists and names another NAR hash;
  - `HEAD` on a NAR the server has;
  - a gzip body sent to an extension-less URL named by the gzip file's
    hash: accepted, stored under the NAR's hash, `HEAD` on that URL 404
    before and after;
  - a narinfo answered while eight slowed content downloads run;
  - a server-side connection killed between two narinfo requests, the
    second still answering 200;
  - a connection the server lost without a word: a narinfo `GET` for a
    path the instance has nothing of answers 200 after the wait and well
    within the 10 seconds, and an upload through the other instance
    succeeds;
  - a refused push that leaves a concurrent push running;
  - broken references: a narinfo name pointing at a large directory root
    (404 without a download), a content reference whose tree does not
    match its recipe, an object that does not match its key.
- **Requests of a web page**, at the handler: the forms of `Host` that are
  answered and those that are not; under a foreign `Host` a `PUT` of a
  NAR and of a narinfo 403 and everything else 404, with nothing staged
  and nothing on the server, and a body of 256 MiB not read before the
  403; an `Origin` on a `PUT` and a `Sec-Fetch-Site` of another site
  refused, `none` and a `GET` with an `Origin` answered.
- **The command**, `serve` as it is started, against the in-process
  server: an upload to one `serve` that a second one fetches and serves
  under its own signature; a web page's uploads 403 and its `GET`s 404,
  with nothing on the server; the name of `--listen` answered beside the
  addresses and `localhost`, and no other name. The stop: a running
  request that finishes, requests that are cut and waited for, a request
  that still runs with nothing closed under it, and a second signal, sent
  to the command started as a shell starts a foreground and a background
  job. The wait for a reference; staged NARs nobody came back for; the
  cache kept under its size; a narinfo `PUT` after a restart; and the
  command's own dial, to a server at a given address, under the identity
  of its key file.
- **With real Nix**, where the `nix` program is present: `nix copy --to` a
  closure of several paths with references between them into one instance
  and substitute it from another into a throwaway store; with the default
  xz upload, with `?compression=none` (a second copy after the narinfo
  references were removed sends no NAR), and with zstd and a NAR over
  16 MiB; and the two stopped-narrenhaus cases of section 8.1, of which
  the build goes through the machine's store and its daemon, not a
  throwaway store, and skips where the daemon does not trust the user.
  Each such test runs `nix` with `XDG_CACHE_HOME` pointing at a fresh
  directory, `NIX_USER_CONF_FILES` empty and `NIX_CONFIG` unset, and uses
  its own store paths, because Nix remembers what a cache URL answered.
- **Setup check**: against captured `nix config show --json` and
  `nix store info --json` outputs for each case of section 8, `trusted` in
  both encodings and missing, a conflicting key of the same name, a
  setting that is `null`, and `NIX_USER_CONF_FILES` naming files, naming
  none and unset. Where `nix` is present, Nix itself is asked which of
  those files it reads.
- **jaccard-store additions**: tested in jaccard-store itself, including a
  push from a packstore giving the same pack as before and a push and
  fetch of a single-Blob root.

## 14. Left to the builder

- Package names and boundaries, within flat top-level packages.
- The NAR, narinfo and signing library (`zombiezen.com/go/nix` and
  `github.com/nix-community/go-nix` were looked at; either, or own code),
  as long as the reader is as strict as section 5.1 asks and the writer is
  canonical.
- The decompression libraries.
- The exact names and signatures of the jaccard-store additions.
- The format of `.meta`, the records and their file names, `origin` and
  the staged table.
- Log wording and the layout of the check's output.
