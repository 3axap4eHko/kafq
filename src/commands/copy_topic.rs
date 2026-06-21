use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Args as ClapArgs;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::client::{GlobalOptions, build_client_config};
use crate::commands::{now_millis, start_offset};
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
    /// maximum number of messages to copy
    #[arg(short = 'c', long)]
    pub count: Option<u64>,
    /// messages per producer send call
    #[arg(long = "batch-size", default_value_t = 500)]
    pub batch_size: usize,
    /// producer compression
    #[arg(short = 'C', long, value_parser = ["none", "gzip", "snappy", "lz4", "zstd"])]
    pub compression: Option<String>,
}

struct PendingMessage {
    payload: Vec<u8>,
    key: Option<Vec<u8>>,
    headers: Option<OwnedHeaders>,
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
        let mut record: FutureRecord<'_, Vec<u8>, Vec<u8>> = FutureRecord::to(dest).payload(&msg.payload);
        if let Some(ref k) = msg.key {
            record = record.key(k);
        }
        if let Some(ref h) = msg.headers {
            record = record.headers(h.clone());
        }
        futures.push(producer.send(record, timeout));
    }
    for fut in futures {
        fut.await
            .map_err(|(e, _)| anyhow!("Failed to send message: {e}"))?;
    }
    Ok(())
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
    let producer: FutureProducer = producer_config.create()?;
    let timeout = globals.operation_timeout();

    let probe: BaseConsumer = consumer_config.create()?;
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
            let resolver: BaseConsumer = consumer_config.create()?;
            let resolved = resolver.offsets_for_times(probe_tpl, timeout)?;
            for p in &partitions {
                let elem = resolved
                    .find_partition(&args.source, *p)
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

    let consumer: StreamConsumer = consumer_config.create()?;
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
                let payload = message.payload().map(|p| p.to_vec()).unwrap_or_default();
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
                    if remaining.is_empty() || total >= limit { break; }
                }
            }
        }
    }

    if !batch.is_empty() {
        send_batch(&producer, &args.dest, &mut batch, send_timeout).await?;
    }

    producer.flush(send_timeout)?;
    eprintln!("{total} messages copied");
    if timed_out {
        eprintln!("TIMEOUT");
        return Ok(1);
    }
    Ok(0)
}
