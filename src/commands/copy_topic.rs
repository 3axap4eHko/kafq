use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Args as ClapArgs;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::client::{
    GlobalOptions, build_client_config, create_base_consumer, create_producer,
    create_stream_consumer,
};
use crate::commands::{
    SnapshotStop, now_millis, partition_offset, start_offset, wait_for_deliveries,
};
use crate::timestamp::parse_timestamp_ms;

#[derive(ClapArgs)]
pub struct Args {
    pub source: String,
    pub dest: String,
    /// consumer group name
    #[arg(short = 'g', long)]
    pub group: Option<String>,
    /// start from timestamp (ms), ISO 8601, or 0 for the beginning
    #[arg(short = 'f', long, default_value = "0")]
    pub from: String,
    /// maximum number of messages to copy (must be at least 1)
    #[arg(short = 'c', long, value_parser = clap::value_parser!(u64).range(1..))]
    pub count: Option<u64>,
    /// messages per producer send call
    #[arg(long = "batch-size", default_value_t = 500)]
    pub batch_size: usize,
    /// producer compression
    #[arg(short = 'C', long, value_parser = ["none", "gzip", "snappy", "lz4", "zstd"])]
    pub compression: Option<String>,
}

struct PendingMessage {
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    headers: Option<OwnedHeaders>,
}

fn future_record<'a>(
    dest: &'a str,
    payload: Option<&'a [u8]>,
    key: Option<&'a [u8]>,
    headers: Option<&OwnedHeaders>,
) -> FutureRecord<'a, [u8], [u8]> {
    let mut record = FutureRecord::to(dest);
    if let Some(payload) = payload {
        record = record.payload(payload);
    }
    if let Some(key) = key {
        record = record.key(key);
    }
    if let Some(headers) = headers {
        record = record.headers(headers.clone());
    }
    record
}

async fn send_batch(
    producer: &FutureProducer,
    dest: &str,
    batch: &mut Vec<PendingMessage>,
    timeout: Duration,
) -> Result<()> {
    let drained = std::mem::take(batch);
    let mut futures = Vec::with_capacity(drained.len());
    for msg in &drained {
        let record = future_record(
            dest,
            msg.payload.as_deref(),
            msg.key.as_deref(),
            msg.headers.as_ref(),
        );
        futures.push(producer.send(record, timeout));
    }
    wait_for_deliveries(futures).await
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
        .unwrap_or_else(|| format!("kafq-copy-{}", now_millis()));

    let mut consumer_config = build_client_config(&globals)?;
    consumer_config
        .set("group.id", &group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        // Partition EOF marks a partition drained even when its high watermark
        // sits above the last delivered record (read_committed control/aborted
        // batches), which offset >= high-1 would otherwise never reach.
        .set("enable.partition.eof", "true");

    let mut producer_config = build_client_config(&globals)?;
    if let Some(ref c) = args.compression {
        producer_config.set("compression.type", c);
    }
    let producer = create_producer(&producer_config, &globals)?;
    let timeout = globals.operation_timeout();

    let probe = create_base_consumer(&consumer_config, &globals)?;
    let partitions = list_partitions(&probe, &args.source, timeout)?;
    let watermarks = fetch_watermarks(&probe, &args.source, &partitions, timeout)?;
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
                probe_tpl.add_partition_offset(&args.source, *p, Offset::Offset(ts))?;
            }
            let resolver = create_base_consumer(&consumer_config, &globals)?;
            let resolved = resolver.offsets_for_times(probe_tpl, timeout)?;
            for p in &partitions {
                let offset =
                    match partition_offset(&resolved, &args.source, *p, "Timestamp offset lookup")?
                    {
                        Offset::Offset(o) => Offset::Offset(o),
                        _ => Offset::End,
                    };
                assignments.push((*p, offset));
            }
        }
    }

    let mut tpl = TopicPartitionList::new();
    for (p, offset) in &assignments {
        tpl.add_partition_offset(&args.source, *p, *offset)?;
    }

    let mut remaining: HashSet<i32> = assignments
        .iter()
        .filter_map(|(p, offset)| {
            let (low, high) = watermarks.get(p).copied().unwrap_or((0, 0));
            (start_offset(*offset, low, high) < high).then_some(*p)
        })
        .collect();

    if remaining.is_empty() {
        eprintln!("0 messages copied");
        return Ok(0);
    }

    let consumer = create_stream_consumer(&consumer_config, &globals)?;
    consumer.assign(&tpl)?;

    let limit = args.count.unwrap_or(u64::MAX);
    let mut total: u64 = 0;
    let mut batch: Vec<PendingMessage> = Vec::with_capacity(args.batch_size);
    let mut stream = consumer.stream();
    let send_timeout = if globals.timeout_ms == 0 {
        Duration::from_secs(30)
    } else {
        Duration::from_millis(globals.timeout_ms)
    };

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
                let payload = message.payload().map(|p| p.to_vec());
                let key = message.key().map(|k| k.to_vec());
                let headers = message.headers().map(|hs| {
                    let mut owned = OwnedHeaders::new();
                    for h in hs.iter() {
                        owned = owned.insert(rdkafka::message::Header {
                            key: h.key,
                            value: h.value,
                        });
                    }
                    owned
                });
                batch.push(PendingMessage { payload, key, headers });
                total += 1;

                if let Some((_, h)) = watermarks.get(&partition)
                    && offset >= *h - 1
                {
                    remaining.remove(&partition);
                }

                if batch.len() >= args.batch_size || remaining.is_empty() || total >= limit {
                    send_batch(&producer, &args.dest, &mut batch, send_timeout).await?;
                    if remaining.is_empty() || total >= limit {
                        break SnapshotStop::Complete;
                    }
                }
            }
        }
    };

    if !batch.is_empty() {
        send_batch(&producer, &args.dest, &mut batch, send_timeout).await?;
    }

    producer.flush(send_timeout)?;
    eprintln!("{total} messages copied");
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

    use super::future_record;
    use super::list_partitions;

    #[test]
    fn future_record_distinguishes_null_and_empty_payloads() {
        let null_record = future_record("dest", None, None, None);
        let empty = [];
        let empty_record = future_record("dest", Some(&empty), None, None);

        assert!(null_record.payload.is_none());
        assert_eq!(empty_record.payload, Some(empty.as_slice()));
    }

    #[test]
    fn list_partitions_propagates_topic_metadata_errors() {
        const TOPIC: &str = "copy-forbidden";
        let mock_cluster = MockCluster::new(1).expect("mock cluster creation failed");
        mock_cluster
            .topic_error(
                TOPIC,
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
            )
            .expect("topic error injection failed");
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "kafq-copy-metadata-test")
            .create()
            .expect("consumer creation failed");

        let error = list_partitions(&consumer, TOPIC, Duration::from_secs(2))
            .expect_err("topic metadata error was ignored");

        assert!(error.to_string().contains("metadata error"));
    }
}
