use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Args as ClapArgs;
use futures::{FutureExt, StreamExt};
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers};
use serde_json::{Map, Value};
use tokio::signal::unix::{SignalKind, signal};

use crate::client::{GlobalOptions, build_client_config};
use crate::commands::{now_millis, start_offset};
use crate::formatter::{Formatter, RecordView};
use crate::output::write_jsonl;
use crate::timestamp::parse_timestamp_ms;

#[derive(ClapArgs)]
pub struct Args {
    /// Topic name
    pub topic: String,
    /// consumer group name
    #[arg(short = 'g', long)]
    pub group: Option<String>,
    /// message value format: json, raw, or path to a .wasm component
    #[arg(short = 'd', long = "data-format", default_value = "json")]
    pub data_format: String,
    /// write output to a file instead of stdout
    #[arg(short = 'o', long)]
    pub output: Option<String>,
    /// start consuming from a timestamp (ms), ISO 8601, or 0 for the beginning
    #[arg(short = 'f', long)]
    pub from: Option<String>,
    /// maximum number of messages to consume
    #[arg(short = 'c', long)]
    pub count: Option<u64>,
    /// number of messages to skip before outputting
    #[arg(short = 's', long, default_value_t = 0)]
    pub skip: u64,
    /// maximum messages per output batch line
    #[arg(long = "batch-limit", default_value_t = 100)]
    pub batch_limit: usize,
    /// consume all existing messages and exit (records high watermark on start)
    #[arg(long)]
    pub snapshot: bool,
}

fn list_partitions(consumer: &BaseConsumer, topic: &str, timeout: Duration) -> Result<Vec<i32>> {
    let meta = consumer.fetch_metadata(Some(topic), timeout)?;
    let topic_meta = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("Topic {topic} not found"))?;
    let mut ids: Vec<i32> = topic_meta.partitions().iter().map(|p| p.id()).collect();
    ids.sort_unstable();
    Ok(ids)
}

fn fetch_watermarks(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
) -> Result<BTreeMap<i32, (i64, i64)>> {
    let mut map = BTreeMap::new();
    for p in partitions {
        let (low, high) = consumer.fetch_watermarks(topic, *p, timeout)?;
        map.insert(*p, (low, high));
    }
    Ok(map)
}

struct Ctx<'a> {
    topic: &'a str,
    snapshot: bool,
    skip: u64,
    limit: u64,
    batch_limit: usize,
    watermarks: &'a BTreeMap<i32, (i64, i64)>,
    format: &'a Formatter,
}

enum Flow {
    Continue,
    Stop,
}

fn decode_message(ctx: &Ctx<'_>, message: &BorrowedMessage<'_>) -> Result<Value> {
    let partition = message.partition();
    let offset = message.offset();
    let high = ctx
        .watermarks
        .get(&partition)
        .map(|(_, h)| *h)
        .unwrap_or(offset);
    let timestamp_ms = message.timestamp().to_millis().unwrap_or(0);

    let mut owned_headers: Vec<(String, Vec<u8>)> = Vec::new();
    if let Some(headers) = message.headers() {
        for h in headers.iter() {
            owned_headers.push((
                h.key.to_string(),
                h.value.map(|v| v.to_vec()).unwrap_or_default(),
            ));
        }
    }
    let view = RecordView {
        topic: ctx.topic,
        partition,
        key: message.key(),
        value: message.payload(),
        headers: &owned_headers,
        timestamp: timestamp_ms,
    };
    let decoded = ctx.format.decode(view)?;
    let ahead = (high - offset).max(0);

    let mut msg = Map::new();
    for (k, v) in decoded.fields {
        msg.insert(k, v);
    }
    msg.insert("offset".to_string(), Value::String(offset.to_string()));
    msg.insert(
        "timestamp".to_string(),
        Value::String(timestamp_ms.to_string()),
    );
    msg.insert("ahead".to_string(), Value::from(ahead));
    Ok(Value::Object(msg))
}

fn flush_bucket(
    output: &mut dyn Write,
    topic: &str,
    partition: i32,
    messages: &mut Vec<Value>,
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let mut line = Map::new();
    line.insert("topic".to_string(), Value::String(topic.to_string()));
    line.insert("partition".to_string(), Value::from(partition));
    line.insert(
        "messages".to_string(),
        Value::Array(std::mem::take(messages)),
    );
    write_jsonl(output, &Value::Object(line))
}

fn flush_all(
    output: &mut dyn Write,
    topic: &str,
    buckets: &mut BTreeMap<i32, Vec<Value>>,
) -> Result<()> {
    for (partition, messages) in buckets.iter_mut() {
        flush_bucket(output, topic, *partition, messages)?;
    }
    Ok(())
}

fn handle_message(
    ctx: &Ctx<'_>,
    message: &BorrowedMessage<'_>,
    buckets: &mut BTreeMap<i32, Vec<Value>>,
    snapshot_remaining: &mut HashSet<i32>,
    index: &mut u64,
    output: &mut dyn Write,
) -> Result<Flow> {
    let partition = message.partition();
    let offset = message.offset();

    if *index >= ctx.skip {
        let msg = decode_message(ctx, message)?;
        let bucket = buckets.entry(partition).or_default();
        bucket.push(msg);
        if bucket.len() >= ctx.batch_limit {
            flush_bucket(output, ctx.topic, partition, bucket)?;
        }
    }

    if ctx.snapshot
        && let Some((_, h)) = ctx.watermarks.get(&partition)
        && offset >= *h - 1
    {
        snapshot_remaining.remove(&partition);
        if snapshot_remaining.is_empty() {
            return Ok(Flow::Stop);
        }
    }

    *index += 1;
    if *index >= ctx.limit {
        return Ok(Flow::Stop);
    }
    Ok(Flow::Continue)
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let format = Formatter::open(&args.data_format)?;
    let group_id = args
        .group
        .clone()
        .unwrap_or_else(|| format!("kafq-consumer-{}", now_millis()));

    let mut config = build_client_config(&globals)?;
    config
        .set("group.id", &group_id)
        .set("enable.auto.commit", "true")
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "1000")
        .set("auto.offset.reset", "earliest");

    // A partition is "done" for a snapshot when it is drained, signalled by
    // partition EOF. The watermark-based check alone is not enough: under the
    // default read_committed isolation the high watermark can sit above the last
    // delivered record (control/aborted-transaction batches consume offsets but
    // are never delivered), so offset >= high-1 would never fire.
    if args.snapshot {
        config.set("enable.partition.eof", "true");
    }

    let timeout = globals.operation_timeout();

    let probe: BaseConsumer = config.create()?;
    let partitions = list_partitions(&probe, &args.topic, timeout)?;
    let watermarks = fetch_watermarks(&probe, &args.topic, &partitions, timeout)?;
    drop(probe);

    let from_arg: Option<String> = if args.snapshot && args.from.is_none() {
        Some("0".to_string())
    } else {
        args.from.clone()
    };

    let mut assignments: Vec<(i32, Offset)> = Vec::with_capacity(partitions.len());
    match from_arg.as_deref() {
        None => {
            for p in &partitions {
                assignments.push((*p, Offset::End));
            }
        }
        Some("0") => {
            for p in &partitions {
                assignments.push((*p, Offset::Beginning));
            }
        }
        Some(other) => {
            let ts = parse_timestamp_ms(other)?;
            let mut probe_tpl = TopicPartitionList::new();
            for p in &partitions {
                probe_tpl.add_partition_offset(&args.topic, *p, Offset::Offset(ts))?;
            }
            let resolver: BaseConsumer = config.create()?;
            let resolved = resolver.offsets_for_times(probe_tpl, timeout)?;
            for p in &partitions {
                let elem = resolved
                    .find_partition(&args.topic, *p)
                    .ok_or_else(|| anyhow!("Missing offset for partition {p}"))?;
                let offset = match elem.offset() {
                    Offset::Offset(o) => Offset::Offset(o),
                    _ => Offset::End,
                };
                assignments.push((*p, offset));
            }
        }
    }

    let mut tpl = TopicPartitionList::new();
    for (p, offset) in &assignments {
        tpl.add_partition_offset(&args.topic, *p, *offset)?;
    }

    let mut snapshot_remaining: HashSet<i32> = HashSet::new();
    if args.snapshot {
        for (p, offset) in &assignments {
            let (low, high) = watermarks.get(p).copied().unwrap_or((0, 0));
            if start_offset(*offset, low, high) < high {
                snapshot_remaining.insert(*p);
            }
        }
        if snapshot_remaining.is_empty() {
            return Ok(0);
        }
    }

    let consumer: StreamConsumer = config.create()?;
    consumer.assign(&tpl)?;

    let mut output: Box<dyn Write> = match &args.output {
        Some(path) => Box::new(std::io::BufWriter::new(std::fs::File::create(path)?)),
        None => Box::new(std::io::BufWriter::new(std::io::stdout())),
    };

    let ctx = Ctx {
        topic: &args.topic,
        snapshot: args.snapshot,
        skip: args.skip,
        limit: args
            .count
            .map(|c| c.saturating_add(args.skip))
            .unwrap_or(u64::MAX),
        batch_limit: args.batch_limit.max(1),
        watermarks: &watermarks,
        format: &format,
    };

    let mut buckets: BTreeMap<i32, Vec<Value>> = BTreeMap::new();
    let mut index: u64 = 0;
    let mut timed_out = false;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut stream = consumer.stream();
    let mut timeout_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        if globals.timeout_ms > 0 {
            Box::pin(tokio::time::sleep(Duration::from_millis(globals.timeout_ms)))
        } else {
            Box::pin(std::future::pending())
        };

    'outer: loop {
        tokio::select! {
            _ = &mut timeout_fut => {
                timed_out = true;
                break;
            }
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
            maybe = stream.next() => {
                let mut stop = match maybe {
                    Some(Ok(m)) => matches!(
                        handle_message(&ctx, &m, &mut buckets, &mut snapshot_remaining, &mut index, output.as_mut())?,
                        Flow::Stop
                    ),
                    Some(Err(KafkaError::PartitionEOF(p))) => {
                        args.snapshot && {
                            snapshot_remaining.remove(&p);
                            snapshot_remaining.is_empty()
                        }
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => true,
                };

                // Reactive drain: pull whatever else is already buffered (up to
                // a full bucket each), without blocking. `None` means the local
                // fetch queue is empty right now, so the current pool is done.
                while !stop {
                    match stream.next().now_or_never() {
                        Some(Some(Ok(m))) => {
                            stop = matches!(
                                handle_message(&ctx, &m, &mut buckets, &mut snapshot_remaining, &mut index, output.as_mut())?,
                                Flow::Stop
                            );
                        }
                        Some(Some(Err(KafkaError::PartitionEOF(p)))) => {
                            if args.snapshot {
                                snapshot_remaining.remove(&p);
                                if snapshot_remaining.is_empty() {
                                    stop = true;
                                }
                            }
                        }
                        Some(Some(Err(err))) => return Err(err.into()),
                        Some(None) => { stop = true; }
                        None => break,
                    }
                }

                flush_all(output.as_mut(), &args.topic, &mut buckets)?;
                if stop {
                    break 'outer;
                }
            }
        }
    }

    flush_all(output.as_mut(), &args.topic, &mut buckets)?;
    output.flush()?;

    if timed_out {
        eprintln!("TIMEOUT");
        return Ok(1);
    }
    Ok(0)
}
