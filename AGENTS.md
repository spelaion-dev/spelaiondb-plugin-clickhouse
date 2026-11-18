# SpelaionDB's ClickHouse plugin — rules for agents

ClickHouse for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's driver,
and a small program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over ClickHouse's HTTP interface:
  - `http.rs` opens its own connections with `hyper`: to `connect_addr` when it is given (the
    local end of SpelaionDB's tunnel), never through a system proxy, with TLS verified against
    `host` (`tls.rs`, libpq's modes; `prefer` falls back to plain HTTP only when the server
    does not answer a handshake). The user and password travel in ClickHouse's headers, never in
    the address.
  - `lib.rs`: each connection is one server-side session, asked for with `session_check=1`
    after its first request, so a session the server lost is an error that loses the
    connection, never a new empty session (SpelaionDB's read-only barrier on ClickHouse is the
    session's `readonly` setting: it must never vanish silently). The session's requests go one
    at a time through an actor; a cancel is `KILL QUERY` by `query_id` on a connection of its
    own, repeated until the statement's request ends. The server holds the session until it
    has ended a request, so a response is always read to its end before the next request,
    even one nobody reads any more (killed, then drained): a connection dropped halfway lets
    the next request race the server and be refused with `SESSION_IS_LOCKED`.
  - `tsv.rs` and `types.rs`: results come as `TabSeparatedWithNamesAndTypes`, the server's own
    text for each value (escaped once, except arrays, tuples and maps, which write their own
    quoted text). An exception after the result started is the `__exception__` marker and a cut
    response.
  - `introspect.rs`: the explorer, from `system` tables, names bound as `{name:String}`
    parameters. A table's primary key is a sort order, not unique: it is never reported as a
    primary key.
- `src/bin/spelaiondb-plugin-clickhouse.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/clickhouse.rs`: the type zoo (every type's cell against the server's `toString`, what
  `clickhouse-client` shows), TLS, sessions, the read-only setting, cancel and the catalog,
  against a real server. **A new type is added there first.** `tests/conformance.rs`:
  SpelaionDB's conformance set against this program, and `plugin.toml` checked against what the
  program says of itself.
- `testing/compose.yaml`, `testing/gen-keys.sh`, `testing/clickhouse` and `ci/services.sh`:
  the test server (ClickHouse 25.8, HTTP on 18123 and HTTPS on 18443).
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent, cancels and
introspects. So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's
repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`. What must be said
  goes to standard error, a line at a time.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file, the environment or a request's address.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a connection is lost: a request lost on the way is not sent
  again.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, and opens no tunnel: it
  connects to `connect_addr` when it is given one and verifies TLS against `host`.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are git
dependencies on a tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`), all on the same tag.
They are not on crates.io.

- **A protocol change is made there first** (the new version, with its tests), then here: move
  the tag, adapt, release. SpelaionDB then moves its pin.
- Arrow must stay the version SpelaionDB uses: batches cross between the two unchanged.
- `plugin.toml`'s `protocol` is what the SDK of that tag speaks; `tests/conformance.rs` fails
  if they differ.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit): the four platforms, each archive with its
SHA-256, and notes that carry the pin in the form SpelaionDB's `plugins.toml` wants. **Nothing
is signed and no secret lives here**: a release reaches users only when SpelaionDB's
repository pins its hashes in a pull request of its own, whose CI runs SpelaionDB's tests
through that exact build (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against a real server (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_CLICKHOUSE=127.0.0.1:18123 SPELAION_TEST_CLICKHOUSE_HTTPS=127.0.0.1:18443 cargo test`.
- `cargo deny check` if dependencies change.
- A change to how values are written is checked against SpelaionDB too: build this program and
  run SpelaionDB's tests through it (`SPELAION_DEV_PLUGIN_CLICKHOUSE=<this program>` with a
  development build there).
- If something could not be tested, say so.
