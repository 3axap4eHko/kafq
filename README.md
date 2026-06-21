# kafq

Apache Kafka CLI. Produce, consume, and administer topics from the shell. JSONL on stdout, JSONL on stdin, exit code reflects success.

Built on top of librdkafka via the [`rdkafka`](https://crates.io/crates/rdkafka) crate.

## Build

System requirements: a C toolchain, CMake, OpenSSL headers, zlib headers. SASL and curl are vendored.

```bash
cargo build --release
```

Install to `~/.cargo/bin`:

```bash
cargo install --path .
```

## Global options

| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `-b, --brokers <list>` | `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap brokers |
| `-t, --timeout <ms>` | `KAFKA_TIMEOUT` | `0` (= 30s admin default, infinite for consume) | Operation timeout |
| `--ssl` | | off | Enable TLS |
| `--insecure` | | off | Skip TLS certificate verification (requires `--ssl`) |
| `--mechanism <m>` | `KAFKA_MECHANISM` | | SASL mechanism: `plain`, `scram-sha-256`, `scram-sha-512`, `oauthbearer` |
| `--username <u>` | `KAFKA_USERNAME` | | SASL username (plain/scram) |
| `--password <p>` | `KAFKA_PASSWORD` | | SASL password (plain/scram) |
| `--oauth-bearer <tok>` | `KAFKA_OAUTH_BEARER` | | Static OAUTHBEARER token |
| `--oidc-token-url <url>` | `KAFKA_OIDC_TOKEN_URL` | | OIDC token endpoint (enables OIDC flow) |
| `--oidc-client-id <id>` | `KAFKA_OIDC_CLIENT_ID` | | OIDC client id |
| `--oidc-client-secret <s>` | `KAFKA_OIDC_CLIENT_SECRET` | | OIDC client secret |
| `--oidc-scope <s>` | `KAFKA_OIDC_SCOPE` | | OIDC scope |
| `--oidc-extensions <kv>` | `KAFKA_OIDC_EXTENSIONS` | | SASL OAUTHBEARER extensions (`k=v,k=v`) |

Global flags work before or after the subcommand (`kafq -t 5000 list` and `kafq list -t 5000` are equivalent).

## Commands

### `kafq list` (alias `ls`)
List topic names, one JSON-encoded string per line. Internal topics (`__*`) are hidden unless `-a/--all` is given.

### `kafq metadata`
Single JSONL line with `throttleTime`, `brokers`, `clusterId`, `controllerId`, `topicMetadata`.

### `kafq config -r <topic|broker> -n <name>`
Describe a resource's configuration. Output is one JSONL object per resource with a `configs` array. `source` uses the Kafka admin protocol names (`DEFAULT_CONFIG`, `DYNAMIC_TOPIC_CONFIG`, ...).

### `kafq topic:create <topic> [-p N] [-r N]`
Creates the topic and emits one JSONL line: `{"name": "...", "partitions": N, "replicas": N}`. Exit code 0 = created, non-zero = error on stderr.

### `kafq topic:delete <topic>`
Deletes the topic. Silent on success.

### `kafq topic:offsets <topic> [timestamp] [-g group]`
- No args: per-partition `{partition, offset, high, low}`.
- Timestamp (ms or ISO 8601): per-partition `{partition, offset}` at the timestamp (`-1` if no message at or after).
- `-g <group>`: single line `{topic, partitions: [{partition, offset}]}` with committed offsets.

### `kafq consume <topic> [opts]`
Streams messages as JSONL batch lines, one per `(topic, partition)`: `{topic, partition, messages: [{key, value, headers, offset, timestamp, ahead}]}`. `ahead` = `high_watermark - offset` captured at start.

Messages are coalesced reactively: as many as are already buffered (up to `--batch-limit`) share one line; a quiet topic yields one message per line.

- `-g, --group <g>` – consumer group (ephemeral if omitted)
- `-f, --from <0|ms|ISO>` – starting position (default: latest)
- `-c, --count <n>` – stop after N messages
- `-s, --skip <n>` – skip first N messages
- `--batch-limit <n>` – maximum messages per output line (default 100)
- `--snapshot` – consume to the high watermark and exit (implies `--from 0` when unset)
- `-o, --output <file>` – write JSONL to file
- `-d, --data-format <json|raw|path.wasm>` – value decoder (default `json`)
- Global `-t <ms>` – timeout; prints `TIMEOUT` to stderr and exits 1

`SIGINT` / `SIGTERM` cause a clean exit (code 0).

### `kafq produce [opts]`
Reads JSONL batch lines from stdin (or `-i, --input <file>`). Each line is one batch: `{ "topic": string, "partition"?: int, "messages": [{ "key"?: string, "value": any, "headers"?: { k:v } }] }`. The destination topic (and optional partition) live in the data; there is no topic argument. A line's messages are sent pipelined; with `partition` set they pin to it, otherwise they route by key hash. The `offset`/`timestamp`/`ahead` fields emitted by `consume` are ignored on input, so `kafq consume … | jq … | kafq produce` round-trips.

- `-d, --data-format <json|raw|path.wasm>` – value encoder
- `-H, --header k:v` – static header applied to every message (repeatable)
- `-w, --wait <ms>` – delay between batch lines
- `-C, --compression <none|gzip|snappy|lz4|zstd>`

### `kafq topic:copy <source> <dest>`
Snapshot-copies messages from `source` to `dest`. Keys, values, and headers are preserved. Sends are batched (`--batch-size`, default 500) and ack-pipelined.

- `-f, --from <0|ms|ISO>` – starting position on source (default `0`)
- `-c, --count <n>` – stop after N messages
- `--batch-size <n>` – producer batch size
- `-C, --compression <...>` – producer compression
- `-g, --group <g>` – consumer group on the source side

### `kafq topic:dump <topic> -o <file> [opts]`
Snapshot-dumps a topic to a JSONL file. Each line is `{partition, offset, timestamp, headers, key, value}` with the raw value string (no JSON decoding).

## Examples

```bash
# Bootstrap
kafq topic:create events -p 3

# Produce one message
echo '{"topic":"events","messages":[{"key":"u-1","value":{"event":"login"}}]}' | kafq produce

# Produce a batch to a specific partition, with a header and compression
echo '{"topic":"events","partition":0,"messages":[{"value":"a"},{"value":"b"}]}' | kafq produce -H source:cli -C zstd

# Snapshot consume to a file
kafq consume events --snapshot -o /tmp/events.jsonl

# Consume from an ISO timestamp
kafq consume events -f 2026-05-01T00:00:00Z -c 100

# Offsets at a point in time
kafq topic:offsets events "2026-05-01T00:00:00Z"

# Mirror one topic to another
kafq topic:copy events events-mirror --batch-size 1000 -C lz4
```

## Tests

Unit/integration tests are off by default. E2E tests live under `tests/e2e.rs` and are gated behind the `e2e` feature. They require a reachable Kafka broker (defaults to `localhost:9092`).

```bash
# default build, no broker required
cargo build

# e2e tests against KAFKA_BROKERS (default localhost:9092)
cargo test --features e2e -- --test-threads=1
```

The suite is one orchestrating test (`cli_e2e_full_lifecycle`) because the scenarios share state. It is forced single-threaded.

## License

MIT.
