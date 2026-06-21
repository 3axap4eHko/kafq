use std::collections::{BTreeMap, HashSet};
use std::io::{BufWriter, Write};
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Args as ClapArgs;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Headers;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::client::{GlobalOptions, build_client_config};
use crate::commands::{now_millis, start_offset};
use crate::output::write_jsonl;
use crate::timestamp::parse_timestamp_ms;

#[derive(ClapArgs)]
pub struct Args {
    pub topic: String,
    /// output file path
    #[arg(short = 'o', long)]
    pub output: String,
    /// consumer group name
    #[arg(short = 'g', long)]
    pub group: Option<String>,
    /// start from timestamp (ms), ISO 8601, or 0 for the beginning
    #[arg(short = 'f', long, default_value = "0")]
    pub from: String,
    /// maximum number of messages to dump
    #[arg(short = 'c', long)]
    pub count: Option<u64>,
}

#[derive(Serialize)]
struct DumpedMessage {
    partition: i32,
    offset: String,
    timestamp: String,
    headers: Map<String, Value>,
    key: Option<String>,
    value: Option<String>,
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

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let group_id = args
        .group
        .clone()
        .unwrap_or_else(|| format!("kafq-dump-{}", now_millis()));

    let mut config = build_client_config(&globals)?;
    config
        .set("group.id", &group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        // Partition EOF marks a partition drained even when its high watermark
        // sits above the last delivered record (read_committed control/aborted
        // batches), which offset >= high-1 would otherwise never reach.
        .set("enable.partition.eof", "true");

    let timeout = globals.operation_timeout();

    let probe: BaseConsumer = config.create()?;
    let partitions = list_partitions(&probe, &args.topic, timeout)?;
    let watermarks = fetch_watermarks(&probe, &args.topic, &partitions, timeout)?;
    drop(probe);

    let mut assignments: Vec<(i32, Offset)> = Vec::with_capacity(partitions.len());
    match args.from.as_str() {
        "0" => {
            for p in &partitions {
                assignments.push((*p, Offset::Beginning));
            }
        }
        other => {
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

    let mut output = BufWriter::new(std::fs::File::create(&args.output)?);

    let mut remaining: HashSet<i32> = assignments
        .iter()
        .filter_map(|(p, offset)| {
            let (low, high) = watermarks.get(p).copied().unwrap_or((0, 0));
            (start_offset(*offset, low, high) < high).then_some(*p)
        })
        .collect();

    if remaining.is_empty() {
        eprintln!("0 messages dumped to {}", args.output);
        return Ok(0);
    }

    let consumer: StreamConsumer = config.create()?;
    consumer.assign(&tpl)?;

    let limit = args.count.unwrap_or(u64::MAX);
    let mut total: u64 = 0;
    let mut stream = consumer.stream();

    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let mut timed_out = false;
    let mut timeout_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        if globals.timeout_ms > 0 {
            Box::pin(tokio::time::sleep(Duration::from_millis(globals.timeout_ms)))
        } else {
            Box::pin(std::future::pending())
        };

    loop {
        tokio::select! {
            _ = &mut timeout_fut => { timed_out = true; break; }
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
            maybe = stream.next() => {
                let message = match maybe {
                    Some(Ok(m)) => m,
                    Some(Err(KafkaError::PartitionEOF(p))) => {
                        remaining.remove(&p);
                        if remaining.is_empty() {
                            break;
                        }
                        continue;
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => break,
                };

                let partition = message.partition();
                let offset = message.offset();
                let mut headers_map = Map::new();
                if let Some(headers) = message.headers() {
                    for h in headers.iter() {
                        let value = h
                            .value
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                            .unwrap_or_default();
                        headers_map.insert(h.key.to_string(), Value::String(value));
                    }
                }
                let key = message.key().map(|k| String::from_utf8_lossy(k).into_owned());
                let value = message.payload().map(|p| String::from_utf8_lossy(p).into_owned());

                write_jsonl(
                    &mut output,
                    &DumpedMessage {
                        partition,
                        offset: offset.to_string(),
                        timestamp: message
                            .timestamp()
                            .to_millis()
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "0".to_string()),
                        headers: headers_map,
                        key,
                        value,
                    },
                )?;
                total += 1;

                if let Some((_, h)) = watermarks.get(&partition)
                    && offset >= *h - 1
                {
                    remaining.remove(&partition);
                }
                if remaining.is_empty() || total >= limit { break; }
            }
        }
    }

    output.flush()?;
    eprintln!("{total} messages dumped to {}", args.output);
    if timed_out {
        eprintln!("TIMEOUT");
        return Ok(1);
    }
    Ok(0)
}
