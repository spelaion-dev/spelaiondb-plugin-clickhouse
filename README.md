# spelaiondb-plugin-clickhouse

ClickHouse for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop database
client: the engine's driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for ClickHouse, the first engine written only as a plugin. It is
built here, in its own repository, and SpelaionDB pins by hash the exact release it was tested
with (its ADR-042, ADR-055): the app installs it from its Plugins page.

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.0. Experimental until SpelaionDB 1.0.
- **Engine:** `clickhouse`, over ClickHouse's HTTP interface (port 8123, or 8443 with TLS),
  with `hyper` and rustls.
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test server (ClickHouse 25.8 in Docker, HTTP and HTTPS, with its certificates), and

```bash
SPELAION_TEST_CLICKHOUSE=127.0.0.1:18123 SPELAION_TEST_CLICKHOUSE_HTTPS=127.0.0.1:18443 cargo test
```

runs the type zoo and the protocol's conformance set against this plugin's own program.
Without the variables the tests that need the server are skipped, which does not count as
tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_CLICKHOUSE=/path/to/target/debug/spelaiondb-plugin-clickhouse`, in
SpelaionDB's repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
