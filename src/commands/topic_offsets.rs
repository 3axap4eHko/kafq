use std::io::stdout;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Args as ClapArgs;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde::Serialize;

use crate::client::{GlobalOptions, build_client_config, create_base_consumer};
use crate::commands::{now_millis, partition_offset};
use crate::output::write_jsonl;
use crate::timestamp::parse_timestamp_ms;

#[derive(ClapArgs)]
pub struct Args {
    /// Topic name
    pub topic: String,
    /// Optional timestamp (ms or ISO 8601) to look up offsets at that point
    pub timestamp: Option<String>,
    /// Show committed offsets for this consumer group
    #[arg(short = 'g', long)]
    pub group: Option<String>,
}

#[derive(Serialize)]
struct Watermark {
    partition: i32,
    offset: String,
    high: String,
    low: String,
}

#[derive(Serialize)]
struct PartitionOffset {
    partition: i32,
    offset: String,
}

#[derive(Serialize)]
struct GroupTopicOffsets {
    topic: String,
    partitions: Vec<PartitionOffset>,
}

fn partition_ids(consumer: &BaseConsumer, topic: &str, timeout: Duration) -> Result<Vec<i32>> {
    let meta = consumer.fetch_metadata(Some(topic), timeout)?;
    let topic_meta = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow::anyhow!("Topic {topic} not found"))?;
    if let Some(err) = topic_meta.error() {
        bail!("Topic {topic} metadata error: {err:?}");
    }
    let mut ids: Vec<i32> = topic_meta.partitions().iter().map(|p| p.id()).collect();
    ids.sort_unstable();
    Ok(ids)
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let mut config = build_client_config(&globals)?;
    let timeout = globals.operation_timeout();

    if let Some(ref ts_raw) = args.timestamp {
        let ts = parse_timestamp_ms(ts_raw)?;
        config.set("group.id", format!("kafq-offsets-{}", now_millis()));
        let consumer = create_base_consumer(&config, &globals)?;
        let partitions = partition_ids(&consumer, &args.topic, timeout)?;
        let mut tpl = TopicPartitionList::new();
        for p in &partitions {
            tpl.add_partition_offset(&args.topic, *p, Offset::Offset(ts))?;
        }
        let resolved = consumer.offsets_for_times(tpl, timeout)?;

        let mut out = stdout().lock();
        for p in &partitions {
            let offset =
                match partition_offset(&resolved, &args.topic, *p, "Timestamp offset lookup")? {
                    Offset::Offset(o) => o.to_string(),
                    _ => "-1".to_string(),
                };
            write_jsonl(
                &mut out,
                &PartitionOffset {
                    partition: *p,
                    offset,
                },
            )?;
        }
        return Ok(0);
    }

    if let Some(ref group) = args.group {
        config.set("group.id", group);
        let consumer = create_base_consumer(&config, &globals)?;
        let partitions = partition_ids(&consumer, &args.topic, timeout)?;
        let mut tpl = TopicPartitionList::new();
        for p in &partitions {
            tpl.add_partition(&args.topic, *p);
        }
        let committed = consumer.committed_offsets(tpl, timeout)?;
        let mut offsets = Vec::new();
        for p in &partitions {
            let offset_str =
                match partition_offset(&committed, &args.topic, *p, "Committed offset lookup")? {
                    Offset::Offset(o) => o.to_string(),
                    Offset::Invalid => "-1".to_string(),
                    other => format!("{other:?}"),
                };
            offsets.push(PartitionOffset {
                partition: *p,
                offset: offset_str,
            });
        }
        let mut out = stdout().lock();
        write_jsonl(
            &mut out,
            &GroupTopicOffsets {
                topic: args.topic.clone(),
                partitions: offsets,
            },
        )?;
        return Ok(0);
    }

    config.set("group.id", format!("kafq-offsets-{}", now_millis()));
    let consumer = create_base_consumer(&config, &globals)?;
    let partitions = partition_ids(&consumer, &args.topic, timeout)?;

    let mut out = stdout().lock();
    for p in &partitions {
        let (low, high) = consumer.fetch_watermarks(&args.topic, *p, timeout)?;
        write_jsonl(
            &mut out,
            &Watermark {
                partition: *p,
                offset: high.to_string(),
                high: high.to_string(),
                low: low.to_string(),
            },
        )?;
    }
    Ok(0)
}
