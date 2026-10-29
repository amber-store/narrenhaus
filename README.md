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

It is being built: the command has its options and does nothing yet.
