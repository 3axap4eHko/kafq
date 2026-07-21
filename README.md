# kafq

`kafq` is an Apache Kafka command-line client for producing, consuming, inspecting, copying, dumping, and administering topics. Data-oriented commands use newline-delimited JSON (JSONL), diagnostics go to stderr, and exit codes distinguish success, timeout, interruption, and errors.

It is a single Rust binary built on librdkafka through [`rdkafka` 0.39.0](https://crates.io/crates/rdkafka/0.39.0).

## Features

- Produce JSONL batches from stdin or a file, with per-message keys, values, and headers.
- Consume live streams or a point-in-time snapshot into partition-grouped JSONL batches.
- Use built-in JSON and raw-value codecs or optional WebAssembly Component Model formatters.
- List topics and inspect cluster metadata, resource configuration, watermarks, timestamp offsets, and committed offsets.
- Create and delete topics.
- Snapshot-copy records between topics while preserving keys, payloads, tombstones, and headers.
- Snapshot-dump raw records to a JSONL file.
- Connect with plaintext, TLS, SASL PLAIN, SASL SCRAM, static OAUTHBEARER, or OIDC OAUTHBEARER.
- Pipeline producer acknowledgements with idempotence enabled for retry ordering.

## Build and install

System requirements are a Rust toolchain, a C toolchain, CMake, OpenSSL headers, and zlib headers. SASL/GSSAPI and curl are built from vendored sources.

Build the default binary:

```bash
cargo build --release --locked
```

Build with WebAssembly formatter support:

```bash
cargo build --release --locked --features wasm
```

Install the default binary to Cargo's binary directory:

```bash
cargo install --path . --locked
```

Include WebAssembly formatter support when installing:

```bash
cargo install --path . --locked --features wasm
```

The package uses Rust edition 2024. The optional `wasm` feature adds Wasmtime Component Model and WASI Preview 2 support. The `e2e` feature only enables broker-backed tests.

## Global options

Global options work before or after the subcommand. For example, `kafq -t 5000 list` and `kafq list -t 5000` are equivalent.

Clap also provides `-h, --help`, `-V, --version`, and `kafq help <command>`.

| Flag | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `-b, --brokers <list>` | `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap broker addresses |
| `-t, --timeout <ms>` | `KAFKA_TIMEOUT` | `0` | Request, stream, and producer timeout policy described below |
| `--ssl` | | off | Enable TLS |
| `--insecure` | | off | Disable TLS certificate verification when TLS is enabled |
| `--mechanism <name>` | `KAFKA_MECHANISM` | | `plain`, `scram-sha-256`, `scram-sha-512`, or `oauthbearer` |
| `--username <value>` | `KAFKA_USERNAME` | | SASL username for PLAIN or SCRAM |
| `--password <value>` | `KAFKA_PASSWORD` | | SASL password for PLAIN or SCRAM |
| `--oauth-bearer <token>` | `KAFKA_OAUTH_BEARER` | | Static OAUTHBEARER token |
| `--oauth-principal <name>` | `KAFKA_OAUTH_PRINCIPAL` | | Principal associated with a static token |
| `--oauth-expiry-ms <ms>` | `KAFKA_OAUTH_EXPIRY_MS` | | Static token expiration as future Unix milliseconds |
| `--oidc-token-url <url>` | `KAFKA_OIDC_TOKEN_URL` | | OIDC token endpoint |
| `--oidc-client-id <id>` | `KAFKA_OIDC_CLIENT_ID` | | OIDC client ID |
| `--oidc-client-secret <secret>` | `KAFKA_OIDC_CLIENT_SECRET` | | OIDC client secret |
| `--oidc-scope <scope>` | `KAFKA_OIDC_SCOPE` | | Optional OIDC scope |
| `--oidc-extensions <pairs>` | `KAFKA_OIDC_EXTENSIONS` | | Optional OAUTHBEARER extensions as `key=value,key=value` |

### Timeout behavior

For a nonzero timeout, request-style Kafka operations use that duration, the streaming phase of `consume`, `topic:copy`, and `topic:dump` stops after that duration, and producer delivery uses the same deadline.

With the default timeout of `0`:

- Admin, metadata, watermark, and offset requests use a 30-second fallback.
- `consume`, `topic:copy`, and `topic:dump` have no streaming deadline.
- Producer queue admission waits for up to 30 seconds.
- The librdkafka producer delivery deadline is disabled.

### Authentication

The transport is selected from the supplied options:

| Mode | Required options |
| --- | --- |
| Plaintext | No transport or SASL flags |
| TLS | `--ssl` |
| SASL PLAIN or SCRAM | `--mechanism <name>`; `--username` and `--password` are passed when supplied; add `--ssl` for SASL over TLS |
| Static OAUTHBEARER | `--mechanism oauthbearer`, `--oauth-bearer`, `--oauth-principal`, and a future `--oauth-expiry-ms` |
| OIDC OAUTHBEARER | `--mechanism oauthbearer`, `--oidc-token-url`, `--oidc-client-id`, and `--oidc-client-secret`; scope and extensions are optional |

Static OAUTHBEARER and OIDC options are mutually exclusive. OAuth options are rejected unless the mechanism is `oauthbearer`. Static tokens are installed directly on every admin, consumer, and producer client created by the command.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Command completed successfully; `consume` also uses 0 after a handled `SIGINT` or `SIGTERM` |
| `1` | Runtime error or streaming timeout; timed-out stream commands print `TIMEOUT` to stderr |
| `2` | Command-line parsing or value-validation error from Clap |
| `130` | `topic:copy` or `topic:dump` flushed pending data after `SIGINT`, but the snapshot is incomplete |
| `143` | `topic:copy` or `topic:dump` flushed pending data after `SIGTERM`, but the snapshot is incomplete |

## JSONL and data formats

### Produce input

`produce` reads one batch object per non-empty input line:

```json
{"topic":"events","partition":0,"messages":[{"key":"u-1","value":{"event":"login"},"headers":{"source":"cli"}}]}
```

The batch contract is:

```text
{
  "topic": string,
  "partition"?: signed 32-bit integer | null,
  "messages": [
    {
      "key"?: any,
      "value": any,
      "headers"?: object,
      ...format-specific fields
    }
  ]
}
```

`topic` and `messages` are required. A batch-level `partition` pins every message in that batch and overrides a partition requested by a WebAssembly formatter. Without it, a nonnegative partition requested by a WebAssembly formatter is honored; otherwise the producer uses the encoded key or its default partitioner. Out-of-range partition integers are rejected instead of being truncated.

Input files are streamed line by line, blank lines are ignored, and batches are processed in input order. Messages inside one batch are sent and awaited concurrently. Per-message headers take precedence over a static `--header` with the same name.

The built-in encoders ignore extra message fields, including `offset`, `timestamp`, and `ahead`, so output from `consume` can be transformed and sent back to `produce`. A WebAssembly encoder receives the complete message object.

### Consume output

`consume` writes one JSONL batch per partition bucket:

```json
{"topic":"events","partition":0,"messages":[{"key":"u-1","value":{"event":"login"},"headers":{"source":"cli"},"offset":"42","timestamp":"1780000000000","ahead":1}]}
```

`offset` and `timestamp` are decimal strings. `ahead` is the startup high watermark minus the message offset, never below zero. Built-in keys and header values are decoded with lossy UTF-8. Duplicate Kafka header names collapse to the last value because JSON headers are represented as an object.

### Built-in `json` format

On consume, the payload must be valid JSON and becomes `value`; a null Kafka payload becomes JSON `null`. On produce, the required message `value` is serialized as JSON bytes. String keys and header values become their UTF-8 bytes, null keys are omitted, null header values become empty bytes, and other key or header values use their JSON text.

### Built-in `raw` format

On consume, payload bytes become a lossy UTF-8 string and a null Kafka payload remains JSON `null`. On produce, string values use their UTF-8 bytes, while null and non-string values use their JSON text. Key and header handling matches the built-in JSON format.

### WebAssembly format

A path ending in `.wasm` selects a WebAssembly component formatter. The binary must be built with `--features wasm`; otherwise the command rejects the formatter path. See [WebAssembly component formatters](#webassembly-component-formatters) for the contract and examples.

## Commands

### `kafq list` / `kafq ls`

Lists topic names in sorted order, one JSON string per line.

| Option | Description |
| --- | --- |
| `-a, --all` | Include internal topics such as `__consumer_offsets`; they are hidden by default |

### `kafq metadata`

Writes one JSON object containing cluster and topic metadata:

```text
{
  "throttleTime": 0,
  "brokers": [{"nodeId": number, "host": string, "port": number, "rack": null}],
  "clusterId": string,
  "controllerId": number,
  "topicMetadata": [{
    "topicErrorCode": number,
    "topic": string,
    "isInternal": boolean,
    "partitionMetadata": [{
      "partitionErrorCode": number,
      "partitionId": number,
      "leader": number,
      "replicas": [number],
      "isr": [number]
    }]
  }]
}
```

Partitions are sorted by partition ID. `isInternal` is derived from the `__` topic prefix.

### `kafq config -r <topic|broker> -n <name>`

Describes one topic or broker configuration resource. Broker names must be numeric IDs. The long form of `-n` is `--resourceName`.

| Option | Description |
| --- | --- |
| `-r, --resource <topic|broker>` | Resource type |
| `-n, --resourceName <name>` | Topic name or numeric broker ID |

Each result is a JSON object:

```text
{
  "resourceType": "TOPIC" | "BROKER",
  "resourceName": string,
  "configs": [{
    "name": string,
    "value": string | null,
    "readOnly": boolean,
    "isDefault": boolean,
    "sensitive": boolean,
    "source": string,
    "synonyms": []
  }]
}
```

Sensitive or absent configuration values are represented as `null`. `source` uses Kafka admin protocol labels such as `DEFAULT_CONFIG`, `DYNAMIC_TOPIC_CONFIG`, and `STATIC_BROKER_CONFIG`.

### `kafq topic:create <topic>`

Creates a topic and writes `{"name":"...","partitions":N,"replicas":N}`.

| Option | Default | Description |
| --- | --- | --- |
| `-p, --partitions <n>` | `1` | Number of partitions |
| `-r, --replicas <n>` | `1` | Replication factor |

### `kafq topic:delete <topic>`

Deletes a topic. The command is silent on success.

### `kafq topic:offsets <topic> [timestamp]`

Supports three lookup modes:

| Invocation | Output |
| --- | --- |
| `kafq topic:offsets <topic>` | One line per partition: `{"partition":N,"offset":"HIGH","high":"HIGH","low":"LOW"}` |
| `kafq topic:offsets <topic> <timestamp>` | One line per partition: `{"partition":N,"offset":"OFFSET"}`; `-1` means no record exists at or after the timestamp |
| `kafq topic:offsets <topic> -g <group>` | One line: `{"topic":"...","partitions":[{"partition":N,"offset":"OFFSET"}]}`; an uncommitted partition is `-1` |

Timestamps may be unsigned Unix milliseconds or RFC 3339 strings. Offset values are strings to preserve their full integer range. If both a timestamp and `--group` are supplied, timestamp lookup takes precedence.

### `kafq consume <topic> [options]`

Consumes from every topic partition using explicit assignments and writes partition-grouped JSONL batches.

| Option | Default | Description |
| --- | --- | --- |
| `-g, --group <name>` | Generated `kafq-consumer-*` name | Consumer group used for the client and auto commits |
| `-d, --data-format <format>` | `json` | `json`, `raw`, or a `.wasm` component path |
| `-o, --output <path>` | stdout | Create or truncate a file and write JSONL there |
| `-f, --from <position>` | Latest | `0` for the retained beginning, Unix milliseconds, or RFC 3339 |
| `-c, --count <n>` | Unlimited | Stop after outputting positive `n` messages |
| `-s, --skip <n>` | `0` | Skip the first `n` consumed messages before applying `--count` |
| `--batch-limit <n>` | `100` | Maximum messages in each partition batch line; values below 1 behave as 1 |
| `--snapshot` | off | Capture startup high watermarks, consume only records below them, and exit |

`--snapshot` defaults `--from` to `0` when no explicit start is given. Messages appended after the startup watermarks are excluded. Reactive batching collects only records already ready in the local stream, so a quiet topic can produce one message per line.

A nonzero global timeout prints `TIMEOUT`, flushes buffered JSONL, and exits 1. `SIGINT` and `SIGTERM` flush buffered JSONL and exit 0.

### `kafq produce [options]`

Reads JSONL batches from stdin or a file. There is no topic positional argument because each batch contains its destination topic.

| Option | Default | Description |
| --- | --- | --- |
| `-d, --data-format <format>` | `json` | `json`, `raw`, or a `.wasm` component path |
| `-i, --input <path>` | stdin | Stream JSONL from a file |
| `-w, --wait <ms>` | `0` | Delay after each non-empty batch line |
| `-H, --header <key:value>` | | Add a static header; repeatable |
| `-C, --compression <name>` | Broker/client default | `none`, `gzip`, `snappy`, `lz4`, or `zstd` |

Every batch waits for all delivery reports before the next input line is processed. Producer idempotence is enabled, preserving per-partition order across retries. Delivery failures return exit 1.

### `kafq topic:copy <source> <dest> [options]`

Captures source high watermarks at startup and copies only records below them. Keys, payloads, null payloads, and headers are preserved. Source offsets, timestamps, and partition numbers are not copied; destination partitioning uses the producer and message key.

| Option | Default | Description |
| --- | --- | --- |
| `-g, --group <name>` | Generated `kafq-copy-*` name | Source consumer group |
| `-f, --from <position>` | `0` | Retained beginning, Unix milliseconds, or RFC 3339 |
| `-c, --count <n>` | Unlimited | Stop after copying positive `n` messages |
| `--batch-size <n>` | `500` | Messages sent and acknowledged together |
| `-C, --compression <name>` | Broker/client default | `none`, `gzip`, `snappy`, `lz4`, or `zstd` |

The command writes the copied message count to stderr. Producer idempotence is enabled. Timeout and signal handling flush pending producer deliveries before returning the exit code described above.

### `kafq topic:dump <topic> -o <file> [options]`

Captures topic high watermarks at startup and writes only records below them to a newly created or truncated JSONL file.

Each output line has this shape:

```text
{
  "partition": number,
  "offset": string,
  "timestamp": string,
  "headers": object,
  "key": string | null,
  "value": string | null
}
```

Keys, payloads, and header values use lossy UTF-8; null payloads remain `null`. Duplicate header names collapse to the last value.

| Option | Default | Description |
| --- | --- | --- |
| `-o, --output <path>` | Required | Destination JSONL file |
| `-g, --group <name>` | Generated `kafq-dump-*` name | Source consumer group |
| `-f, --from <position>` | `0` | Retained beginning, Unix milliseconds, or RFC 3339 |
| `-c, --count <n>` | Unlimited | Stop after dumping positive `n` messages |

The command writes the dumped message count to stderr. Timeout and signal handling flush the file before returning the exit code described above.

### `kafq contract`

Prints the exact WIT contract embedded in the binary. This command does not contact Kafka and is available in builds with or without the `wasm` feature.

## WebAssembly component formatters

WebAssembly formatters are components implementing package `kafq:formatter@0.1.0`, world `formatter`, from [`wit/formatter.wit`](wit/formatter.wit).

The exported `codec` interface contains:

| Export | Purpose |
| --- | --- |
| `plugin-name()` | Returns a free-form name logged by the host when the component loads |
| `decode(kafka-record)` | Converts topic, partition, optional key/value bytes, headers, and broker timestamp into serialized JSON or an error |
| `encode(json, topic)` | Converts one message object into optional key/value bytes, headers, and an optional partition request |

For decode, object fields returned by the plugin are merged into the output message. A non-object result becomes the `value` field. The host always supplies the final `offset`, `timestamp`, and `ahead` fields. For encode, a nonnegative component partition is used only when the enclosing input batch does not specify `partition`.

Print the contract from the binary:

```bash
kafq contract
```

Install the WebAssembly Component Model target and build either bundled example:

```bash
rustup target add wasm32-wasip2

cargo build \
  --manifest-path examples/json/Cargo.toml \
  --target wasm32-wasip2 \
  --release \
  --locked

cargo build \
  --manifest-path examples/passthrough/Cargo.toml \
  --target wasm32-wasip2 \
  --release \
  --locked
```

The components are written to:

```text
examples/json/target/wasm32-wasip2/release/json_formatter.wasm
examples/passthrough/target/wasm32-wasip2/release/passthrough.wasm
```

Use a component for both directions:

```bash
echo '{"topic":"events","messages":[{"value":"hello"}]}' |
  target/release/kafq produce \
    --data-format examples/passthrough/target/wasm32-wasip2/release/passthrough.wasm

target/release/kafq consume events \
  --snapshot \
  --data-format examples/passthrough/target/wasm32-wasip2/release/passthrough.wasm
```

Plugin traps, explicit plugin errors, and invalid JSON returned by a decoder stop the command with exit 1.

## Examples

```bash
# Create a three-partition topic.
kafq topic:create events --partitions 3

# Produce one JSON message.
echo '{"topic":"events","messages":[{"key":"u-1","value":{"event":"login"}}]}' |
  kafq produce

# Produce a pinned batch with a static header and compression.
echo '{"topic":"events","partition":0,"messages":[{"value":"a"},{"value":"b"}]}' |
  kafq produce --header source:cli --compression zstd

# Consume only records present when the command starts.
kafq consume events --snapshot --output /tmp/events.jsonl

# Consume from an RFC 3339 timestamp.
kafq consume events --from 2026-05-01T00:00:00Z --count 100

# Look up offsets at a timestamp.
kafq topic:offsets events 2026-05-01T00:00:00Z

# Show committed offsets.
kafq topic:offsets events --group reporting-consumer

# Mirror the retained source snapshot.
kafq topic:copy events events-mirror --batch-size 1000 --compression lz4

# Dump raw records to a file.
kafq topic:dump events --output /tmp/events-dump.jsonl
```

## Tests

Unit tests do not require Kafka:

```bash
cargo test --locked
cargo test --locked --features wasm
```

Broker-backed tests use `KAFKA_BROKERS`, defaulting to `localhost:9092`. The lifecycle scenarios share broker state and must run single-threaded:

```bash
cargo test --locked --features e2e -- --test-threads=1
```

The WebAssembly round-trip suite additionally requires `wasm32-wasip2`:

```bash
cargo test --locked --features e2e,wasm -- --test-threads=1
```

## License

MIT.
