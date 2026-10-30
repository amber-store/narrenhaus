# narrenhaus

narrenhaus is a Nix binary cache that each person runs on their own machine
and that keeps what it is given on a shared
[jaccard-store](https://github.com/amber-store/jaccard-store) server. Nix
talks to it over HTTP on loopback, as to any binary cache: `nix copy --to`
uploads to it, and as a substituter it serves the paths back. It takes each
uploaded NAR apart into content-addressed chunks and pushes them to the
server under a name prefix a group shares, so one machine's build saves the
next machine the same build; what it downloads again it keeps in a local
cache of bounded size, and NARs go out uncompressed.

```
laptop A                                            laptop B
nix copy --to http://127.0.0.1:8484   ...   Nix substitutes from http://127.0.0.1:8484
        |                                                   ^
   narrenhaus  --- packs --->  jaccard-store  --- packs --->  narrenhaus
```

- Each store path becomes two references on the server: one for the NAR's
  content, `<prefix>nar/<NAR hash>.nar`, and one for its narinfo,
  `<prefix><store path hash>.narinfo`. Answering a narinfo downloads a few
  hundred bytes and nothing of the NAR.
- Files are cut into chunks by their content, so similar store paths share
  storage on the server through jaccard-store's base and patch packs.
- Everyone who uses the same server and the same prefix shares one cache.
  Nobody checks who built what: a machine trusts what its colleagues pushed.
  Each narrenhaus signs the narinfos it serves with a key of its own, which
  its user adds to Nix's trusted keys; Nix's signature checking stays on for
  every other cache.
- narrenhaus is a foreground command run by its user. It never edits a Nix
  configuration file and never needs root.

The design is in
[`docs/superpowers/specs/2026-10-29-narrenhaus-design.md`](docs/superpowers/specs/2026-10-29-narrenhaus-design.md).

## Running it

```sh
narrenhaus serve --server <endpoint ID of the jaccard-store server> --prefix acme/
```

The prefix is taken as it is, so it wants its own separator at the end. On
its first start narrenhaus makes two keys in its configuration directory:
`client.key`, the identity it shows to the server, and `signing.key`, a Nix
secret key named `narrenhaus-<8 hex digits>` that signs the narinfos it
serves.

`serve` writes to standard error: what the Nix configuration lacks (see
[Setup](#setup)), the address it serves at, then one line for each upload
and each NAR served, and what goes wrong. It serves whether Nix is set up
for it or not. On SIGINT or SIGTERM it stops once the requests that are
running have finished, or after ten seconds; pushes still running then are
abandoned, and a second signal ends it at once.

### Options

Every option is a flag and an environment variable; the flag wins. Options
follow the command: `narrenhaus serve --server ...`.

| Flag | Variable | Default | Meaning |
|---|---|---|---|
| `--server` | `NARRENHAUS_SERVER` | required | endpoint ID of the jaccard-store server |
| `--prefix` | `NARRENHAUS_PREFIX` | required | prefix of the reference names, verbatim |
| `--listen` | `NARRENHAUS_LISTEN` | `127.0.0.1:8484` | HTTP address Nix talks to |
| `--cache-dir` | `NARRENHAUS_CACHE_DIR` | `narrenhaus` under the user's cache directory | the pack cache |
| `--cache-size` | `NARRENHAUS_CACHE_SIZE` | `10GiB` | size the pack cache is kept under: bytes, or a number with `KiB`, `MiB`, `GiB` or `TiB` |
| `--config-dir` | `NARRENHAUS_CONFIG_DIR` | `narrenhaus` under the user's configuration directory | the two keys |
| `--priority` | `NARRENHAUS_PRIORITY` | `50` | `Priority` in `nix-cache-info`; Nix asks the cache with the lower number first, and cache.nixos.org has 40 |
| `--content-pushes` | `NARRENHAUS_CONTENT_PUSHES` | `64` | how many NARs have their content pushed to the server at once; the others wait their turn |
| `--narinfo-pushes` | `NARRENHAUS_NARINFO_PUSHES` | `64` | how many narinfos are pushed to the server at once |

The user's cache directory is `~/.cache` on Linux (or `$XDG_CACHE_HOME`)
and `~/Library/Caches` on macOS; the configuration directory is `~/.config`
(or `$XDG_CONFIG_HOME`) and `~/Library/Application Support`.

- A cache directory belongs to one server and one prefix, and to one
  running narrenhaus. Started with another server or prefix, or while
  another narrenhaus has the directory, `serve` refuses and says why.
- `--cache-dir` names a directory that is empty or not there yet, or one
  that a narrenhaus made. `serve` empties `staging/` and `tmp/` in its
  cache directory when it starts, so it refuses a directory that has files
  in it and was not made by a narrenhaus, and leaves it as it was.
- The cache holds the packs narrenhaus downloaded or uploaded, expanded.
  When it is over its size, the pack that has not been used for the longest
  time goes first. A pack a request is reading is never removed, so the
  cache can be over its size for as long as a request lasts.
- Nix remembers a cache's `nix-cache-info` for seven days, so a changed
  `--priority` takes effect after that, or at once with `?priority=N` on the
  substituter URL.
- `--content-pushes` and `--narinfo-pushes` are at least 1. They bound
  what narrenhaus does at once, and the server has a limit of its own:
  jaccard-stored's `--verify-jobs`, which also bounds the commits on a
  server that does not verify, caps the pushes that finish at once. Raising
  narrenhaus's limits helps only as far as the server lets it.
- With a `--listen` address that is not loopback, anyone who can reach the
  port can upload into the shared prefix and get narinfos signed with this
  machine's key. The advice warns about it.
- narrenhaus answers Nix and not a page in a web browser, which can reach a
  port on loopback too. A request is answered only when it calls the cache
  by an IP address, by `localhost` or by the host of `--listen`, and an
  upload only when it carries no `Origin`, which a browser always sends
  with one. A request the browser marks as coming from another site
  (`Sec-Fetch-Site`) is not answered either; the address typed into the
  browser is. So the substituter URL names the cache by its address, as the
  advice prints it, or by the name `--listen` was given; under any other
  name an upload is refused (403) and nothing is found (404).

## Setup

Nix uses narrenhaus once three lines are in its configuration:

```
extra-substituters = http://127.0.0.1:8484
extra-trusted-public-keys = narrenhaus-1a2b3c4d:<base64 of this machine's key>
fallback = true
```

`narrenhaus check` prints them with this machine's key, says which file
they belong in, and exits; `serve` prints the same when it starts. Both
only read and suggest.

```sh
narrenhaus check [--listen ADDRESS] [--config-dir DIR]
```

`check` needs no server and no prefix; it takes the other options of
`serve` and ignores them, so the same command line works for both. It exits
0 whether Nix is set up or not.

Where the lines go depends on the machine, and the check works it out:

- The Nix daemon trusts the user (`trusted-users`): `~/.config/nix/nix.conf`,
  which needs neither root nor a restart.
- `/etc/nix/nix.conf` is generated (NixOS, nix-darwin): the same as
  `nix.settings` options of the system configuration.
- Determinate Nix: `/etc/nix/nix.custom.conf`, and a restart of the Nix
  daemon.
- Otherwise `/etc/nix/nix.conf`, and a restart of the Nix daemon.

For a user the daemon does not trust, the substituter and the key count
only in the system's files, because that is all the daemon honours.
`fallback = true` may always go in `~/.config/nix/nix.conf`.

Where `NIX_USER_CONF_FILES` is set, Nix reads the files it names in place
of `~/.config/nix/nix.conf`, and the advice names the first of them. When
the variable is set and names no file, Nix reads no file of the user's;
the advice then says so beside the lines.

**Why `fallback = true`.** narrenhaus is started by hand, so it is often
not running. Nix that has a cache in its configuration which does not
answer fails operations that need a path no other cache has, unless
`fallback` is on; with it Nix says that the cache failed, leaves it alone
for a minute and goes on with the other caches or builds the path. What it
costs: it applies to every cache, so a failed download from cache.nixos.org
also becomes a local build instead of an error.

## Uploading

```sh
nix copy --to 'http://127.0.0.1:8484?compression=none' <path>
```

`nix copy` copies the path with everything it refers to. It has succeeded
for a path only when both of the path's references are on the server, so
after a successful copy a colleague can substitute it.

- `?compression=none` is what to use: narrenhaus stores NARs taken apart, so
  compressing one first is work for nothing, and only without it can Nix
  ask whether the server already has a NAR and skip sending it. Without the
  parameter Nix compresses with xz; that works too, as do `zstd`, `gzip` and
  `bzip2`. Anything else is refused (415). So is a compressed body that
  makes its reader hold more memory than those of Nix do, an xz dictionary
  above 64 MiB or a zstd window above 128 MiB (400).
- A NAR of more than 64 GiB is refused (422).
- A path already on the server is not pushed again: the first writer wins.
- When the server cannot be reached, a copy fails at once (502) and says
  so. Substitution is different: there narrenhaus answers "not here" (404)
  whatever went wrong, because Nix survives that and may fail a build on
  anything else.
- A very large path may need `--option stalled-download-timeout <seconds>`
  on the `nix copy`: Nix gives up on a request that is silent for 300
  seconds, and the answer to a narinfo waits until the path's content is on
  the server.
- Uploading automatically after each build (`post-build-hook`) is not part
  of this version.

## Known limits

Disk and time

- **Uploads need free disk beyond the cache size.** NARs wait on disk,
  uncompressed, under `staging/` in the cache directory until they are
  pushed, and that is not counted against `--cache-size`. Nix 2.35 and
  later sends every NAR of a copy before the first narinfo, faster than
  they can be pushed, so a copy needs room for the uncompressed size of
  what it copies. A full disk fails the copy (507).
- A narinfo upload that waits longer than Nix's 300 seconds shows as
  repeated requests, and beyond about 25 minutes as a failed copy. The push
  goes on in narrenhaus regardless, and a repeated copy joins it. On a
  server that verifies packs, every push also waits for the server's
  verification.
- A substitution whose packs take more than 240 seconds to arrive is
  answered "not here", and Nix builds the path or looks elsewhere. The
  download goes on, and what it brought is kept for 15 minutes, whatever
  the cache size, for the request that asks again.
- **A NAR of more than 64 GiB can be neither uploaded nor served.** The
  upload is refused, and a reference on the server that states a longer
  NAR is treated as broken. An upload is refused as well (422) when its
  NAR comes apart into more than 2^24 objects, which takes millions of
  different files.
- The first request after the machine slept can take about three seconds
  longer. The connection to the server may have died without a word, and
  narrenhaus waits that long for an answer on it before it dials again.
- Fetching a path stored as a patch fetches its whole base pack, which can
  be as large as the path again.
- Small packs cost more disk than the cache counts: a cached narinfo is
  four small files, each rounded up to a block of the file system.

What Nix remembers

- Nix remembers that a cache did not have a path for an hour
  (`narinfo-cache-negative-ttl`). That covers a path a colleague pushes
  after this machine asked for it, and every "not here" given while the
  server could not be reached.
- A path uploaded with a compression leaves Nix's own narinfo for it, which
  names a compressed NAR that narrenhaus does not serve, in the memory of
  the account that ran `nix copy`. With `?compression=none` what Nix
  remembers is what narrenhaus serves.
- `nix copy` copies whole closures, so paths that came from cache.nixos.org
  are pushed too.

The shared server

- **narrenhaus does not repair.** A reference it finds broken is answered
  "not here" and logged as an error, and stays that way until someone
  deletes it on the server (`jaccard-store rm <name>`) and uploads the path
  again. The same holds for a NAR's reference that was deleted while its
  narinfo remained: Nix gets the narinfo, then no NAR, and builds the path
  or looks elsewhere.
- What is downloaded is checked against its hashes, each NAR is rebuilt
  and compared once before it is first served, and every object read from
  the local cache is checked against its key again. A cached file damaged
  on disk is so found out and not served: a narinfo is answered "not here",
  a NAR that was being sent is cut short, the pack is dropped from the
  cache, and the next request fetches it again.
- Anyone who knows the server's endpoint ID can read, replace and delete
  any reference. The prefix is a name, not a boundary. A narinfo is trusted
  to state its NAR's size truthfully.
- A reference can be built so that checking it costs far more than its
  size suggests. The 64 GiB limit bounds the bytes a check writes, not its
  work: a tree made for it has each byte read through many levels of
  index. narrenhaus does not bound this further, since those who can write
  a reference are trusted.
- A broken reference stored as a patch costs each laptop its cached base
  pack once: before the reference is judged broken, the base pack is
  dropped and downloaded again, and the path the base pack belongs to is
  checked again at its next request.
- Nothing on the server expires, and a store path costs two references
  there. A NAR whose narinfo never arrived stays on the server.
- Paths pushed at the same time cannot be stored as patches of one another,
  so the first upload of many related paths stores more than a later one
  would.

What it does not do

- Build logs, NAR listings, debug info and content-addressed realisations:
  asking for them is answered "not here", and uploading them is refused
  (405), so `nix copy` to a URL with `write-nar-listing=1` or
  `index-debug-info=1` fails. A narinfo sent compressed
  (`narinfo-compression=...`, Nix 2.32 and later) is refused too (415).
- Store directories other than `/nix/store`, and NARs that are not in the
  canonical form every current Nix writes.
- A narinfo whose content address (`CA`) Nix 2.35 refuses, or fails over
  when it checks the path, is refused at upload (400). Content addresses
  with `git:` hashing or BLAKE3 are taken, although a Nix without the
  experimental features `git-hashing` and `blake3-hashes` refuses the
  whole narinfo: such a path is of use only to colleagues who have the
  feature on.
- A narrenhaus that is in the Nix configuration and is not running fails
  Nix operations unless `fallback = true` is set, which is why the setup
  asks for it.

## Development

The repository is a flake; `nix build` makes the command with
[gonixgo](https://github.com/draganm/gonixgo) and runs, in the build, the
tests of the packages the command is made of. It does not run the tests of
`e2e`: that package is test files alone, the command does not contain it,
and gonixgo tests what the command contains. `go test ./e2e/` in the dev
shell runs them, as `go test ./...` does; of the cases with two instances
the build runs those in `cmd/narrenhaus`, uploads to one `serve` that
another serves. Evaluating a gonixgo package runs
a program during evaluation, so every Nix command that touches the package
needs one option:

```sh
nix build --option allow-unsafe-native-code-during-evaluation true
nix develop --option allow-unsafe-native-code-during-evaluation true
```

- The default dev shell carries Go and narrenhaus itself, built from the
  working tree, so entering it builds narrenhaus and puts it on `PATH`.
  With direnv, `.envrc` passes the option and reloads the shell when a
  source file changes. Nix sees only files that git tracks: `git add` new
  ones.
- `nix develop .#bare` is Go alone and needs no option. It is the way in
  when the module does not resolve, for example after a `go get` that left
  `go.sum` behind.
- `go test ./...` runs everything. Most tests start a jaccard-store server
  in their own process, over iroh on loopback with a bucket in memory, and
  need no network beyond loopback. A test that fails prints what its
  narrenhaus instances logged; `NARRENHAUS_TEST_LOG=1 go test -v ./...`
  prints it for every test.
- The tests in `e2e/nix_test.go` run the real `nix` against two narrenhaus
  instances and skip where there is no `nix` on `PATH`. They add small
  generated paths, one of 17 MiB, to the machine's Nix store with
  `nix store add`, and one of them three derivations and what they build,
  and delete them again where the store allows it; they change no Nix
  configuration. That one shows what `fallback = true` is for: it builds a
  one-line derivation with a narrenhaus as the only substituter while that
  runs, stops it, and then requires that a second such build fails without
  fallback and a third succeeds with it. The builds go through the
  machine's Nix daemon, and the test skips where the daemon does not trust
  the user and so does not ask that substituter.
- Tests in three other packages ask the Nix on `PATH` and add nothing to
  the store: two in `narinfo` (which content addresses Nix reads, from a
  cache of files in a temporary directory, skipped on a Nix before 2.31;
  and that Nix takes a narinfo's signature as the key's), one in `nixconf`
  (which configuration it reads) and one in `nar` (`nix-store --dump`
  against the package's own writer).
- jaccard-store is required at a released version only.

Packages, all at the top level:

| Package | What it is |
|---|---|
| `cmd/narrenhaus` | the command: `serve` and `check` |
| `httpd` | the binary cache as Nix sees it, over HTTP |
| `upload` | uploads: NARs staged and pushed as they arrive, narinfos after their content |
| `refs` | references made local and verified before they are served |
| `packcache` | the cache of expanded packs, its records and its eviction |
| `remote` | the one connection to the server, with its time limits |
| `nartree` | a NAR as core objects and back |
| `nar` | a strict NAR parser and a canonical writer |
| `narinfo` | narinfos: parsing, the stored form, the signature and its key |
| `decompress` | the compressions an upload may come in |
| `nixconf` | the setup check and its advice |
| `nix32` | Nix's base 32 |
| `testserver` | a jaccard-store server in one process, for tests |
| `e2e` | tests with two whole instances, with and without the real `nix` |

## Licence

LGPL-3.0-only. `LICENSE` holds the GNU Lesser General Public License,
`COPYING` the GNU General Public License it builds on.
