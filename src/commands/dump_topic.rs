use std::collections::{BTreeMap, HashSet};
use std::io::{BufWriter, Write};
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Args as ClapArgs;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Headers;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::client::{
    GlobalOptions, build_client_config, create_base_consumer, create_stream_consumer,
};
use crate::commands::{SnapshotStop, now_millis, partition_offset, start_offset};
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
    /// maximum number of messages to dump (must be at least 1)
    #[arg(short = 'c', long, value_parser = clap::value_parser!(u64).range(1..))]
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
    if let Some(error) = topic_meta.error() {
        return Err(anyhow!("Topic {topic} metadata error: {error:?}"));
    }
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

    let probe = create_base_consumer(&config, &globals)?;
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
            let resolver = create_base_consumer(&config, &globals)?;
            let resolved = resolver.offsets_for_times(probe_tpl, timeout)?;
            for p in &partitions {
                let offset = match partition_offset(
                    &resolved,
                    &args.topic,
                    *p,
                    "Timestamp offset lookup",
                )? {
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

    let consumer = create_stream_consumer(&config, &globals)?;
    consumer.assign(&tpl)?;

    let limit = args.count.unwrap_or(u64::MAX);
    let mut total: u64 = 0;
    let mut stream = consumer.stream();

    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let mut timeout_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        if globals.timeout_ms > 0 {
            Box::pin(tokio::time::sleep(Duration::from_millis(globals.timeout_ms)))
        } else {
            Box::pin(std::future::pending())
        };

    let stop = loop {
        tokio::select! {
            _ = &mut timeout_fut => break SnapshotStop::Timeout,
            _ = sigint.recv() => break SnapshotStop::Sigint,
            _ = sigterm.recv() => break SnapshotStop::Sigterm,
            maybe = stream.next() => {
                let message = match maybe {
                    Some(Ok(m)) => m,
                    Some(Err(KafkaError::PartitionEOF(p))) => {
                        remaining.remove(&p);
                        if remaining.is_empty() {
                            break SnapshotStop::Complete;
                        }
                        continue;
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => break SnapshotStop::Complete,
                };

                let partition = message.partition();
                let offset = message.offset();
                if let Some((_, high)) = watermarks.get(&partition)
                    && offset >= *high
                {
                    remaining.remove(&partition);
                    if remaining.is_empty() { break SnapshotStop::Complete; }
                    continue;
                }
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
                if remaining.is_empty() || total >= limit {
                    break SnapshotStop::Complete;
                }
            }
        }
    };

    output.flush()?;
    eprintln!("{total} messages dumped to {}", args.output);
    if stop == SnapshotStop::Timeout {
        eprintln!("TIMEOUT");
    }
    Ok(stop.exit_code())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rdkafka::ClientConfig;
    use rdkafka::consumer::BaseConsumer;
    use rdkafka::mocking::MockCluster;
    use rdkafka::types::RDKafkaRespErr;

    use super::list_partitions;

    #[test]
    fn list_partitions_propagates_topic_metadata_errors() {
        const TOPIC: &str = "dump-forbidden";
        let mock_cluster = MockCluster::new(1).expect("mock cluster creation failed");
        mock_cluster
            .topic_error(
                TOPIC,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
            )
            .expect("topic error injection failed");
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "kafq-dump-metadata-test")
            .create()
            .expect("consumer creation failed");

        let error = list_partitions(&consumer, TOPIC, Duration::from_secs(2))
            .expect_err("topic metadata error was ignored");

        assert!(error.to_string().contains("metadata error"));
    }
}
