use std::io::stdout;

use anyhow::Result;
use rdkafka::bindings as rdsys;
use rdkafka::client::Client;
use rdkafka::consumer::Consumer;
use serde::Serialize;
use serde_json::Value;

use crate::client::{GlobalOptions, build_client_config, create_base_consumer};
use crate::output::write_jsonl;

fn fetch_controller_id<C>(client: &Client<C>, timeout_ms: i32) -> i32
where
    C: rdkafka::ClientContext,
{
    unsafe { rdsys::rd_kafka_controllerid(client.native_ptr(), timeout_ms) }
}

#[derive(Serialize)]
struct BrokerInfo {
    #[serde(rename = "nodeId")]
    node_id: i32,
    host: String,
    port: i32,
    rack: Value,
}

#[derive(Serialize)]
struct PartitionInfo {
    #[serde(rename = "partitionErrorCode")]
    partition_error_code: i32,
    #[serde(rename = "partitionId")]
    partition_id: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}

#[derive(Serialize)]
struct TopicInfo {
    #[serde(rename = "topicErrorCode")]
    topic_error_code: i32,
    topic: String,
    #[serde(rename = "isInternal")]
    is_internal: bool,
    #[serde(rename = "partitionMetadata")]
    partition_metadata: Vec<PartitionInfo>,
}

#[derive(Serialize)]
struct MetadataOutput {
    #[serde(rename = "throttleTime")]
    throttle_time: i32,
    brokers: Vec<BrokerInfo>,
    #[serde(rename = "clusterId")]
    cluster_id: String,
    #[serde(rename = "controllerId")]
    controller_id: i32,
    #[serde(rename = "topicMetadata")]
    topic_metadata: Vec<TopicInfo>,
}

pub async fn run(globals: GlobalOptions) -> Result<i32> {
    let config = build_client_config(&globals)?;
    let consumer = create_base_consumer(&config, &globals)?;
    let timeout = globals.operation_timeout();
    let meta = consumer.fetch_metadata(None, timeout)?;
    let cluster_id = consumer.client().fetch_cluster_id(timeout).unwrap_or_default();
    let controller_id = fetch_controller_id(consumer.client(), timeout.as_millis() as i32);

    let brokers = meta
        .brokers()
        .iter()
        .map(|b| BrokerInfo {
            node_id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
            rack: Value::Null,
        })
        .collect();

    let mut topic_metadata = Vec::new();
    for topic in meta.topics() {
        let name = topic.name().to_string();
        let mut partitions: Vec<PartitionInfo> = topic
            .partitions()
            .iter()
            .map(|p| PartitionInfo {
                partition_error_code: p.error().map(|e| e as i32).unwrap_or(0),
                partition_id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
            })
            .collect();
        partitions.sort_by_key(|p| p.partition_id);
        topic_metadata.push(TopicInfo {
            topic_error_code: topic.error().map(|e| e as i32).unwrap_or(0),
            is_internal: name.starts_with("__"),
            topic: name,
            partition_metadata: partitions,
        });
    }

    let output = MetadataOutput {
        throttle_time: 0,
        brokers,
        cluster_id,
        controller_id,
        topic_metadata,
    };

    let mut out = stdout().lock();
    write_jsonl(&mut out, &output)?;
    Ok(0)
}
