use std::time::Duration;

use anyhow::{Result, anyhow};
use clap::Args as ClapArgs;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::client::{GlobalOptions, build_client_config, create_producer};
use crate::commands::wait_for_deliveries;
use crate::formatter::{EncodedRecord, Formatter};

#[derive(ClapArgs)]
pub struct Args {
    /// message value format: json, raw, or path to a .wasm component
    #[arg(short = 'd', long = "data-format", default_value = "json")]
    pub data_format: String,
    /// read batch lines from a JSONL file instead of stdin
    #[arg(short = 'i', long)]
    pub input: Option<String>,
    /// delay in milliseconds between sending each batch line
    #[arg(short = 'w', long, default_value_t = 0)]
    pub wait: u64,
    /// static header added to every message (format: key:value), repeatable
    #[arg(short = 'H', long = "header")]
    pub header: Vec<String>,
    /// compression algorithm
    #[arg(short = 'C', long, value_parser = ["none", "gzip", "snappy", "lz4", "zstd"])]
    pub compression: Option<String>,
}

struct Batch {
    topic: String,
    partition: Option<i32>,
    messages: Vec<EncodedRecord>,
}

fn parse_static_headers(headers: &[String]) -> Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    for raw in headers {
        match raw.split_once(':') {
            Some((k, v)) if !k.trim().is_empty() => {
                out.push((k.trim().to_string(), v.trim().as_bytes().to_vec()));
            }
            _ => return Err(anyhow!("Invalid header \"{raw}\"; expected key:value")),
        }
    }
    Ok(out)
}

fn parse_batch(line: &Value, format: &Formatter) -> Result<Batch> {
    let obj = line
        .as_object()
        .ok_or_else(|| anyhow!("batch line must be a JSON object"))?;
    let topic = obj
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("batch line is missing the required string field `topic`"))?
        .to_string();
    let partition = match obj.get("partition") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let partition = value
                .as_i64()
                .ok_or_else(|| anyhow!("`partition` must be an integer"))?;
            Some(i32::try_from(partition).map_err(|_| {
                anyhow!("`partition` must be between {} and {}", i32::MIN, i32::MAX)
            })?)
        }
    };
    let raw_messages = obj
        .get("messages")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("batch line is missing the required array field `messages`"))?;

    let mut messages = Vec::with_capacity(raw_messages.len());
    for (i, msg) in raw_messages.iter().enumerate() {
        messages.push(
            format
                .encode(msg, &topic)
                .map_err(|e| anyhow!("message {i}: {e}"))?,
        );
    }
    Ok(Batch {
        topic,
        partition,
        messages,
    })
}

async fn produce_line(
    line: &str,
    line_number: usize,
    format: &Formatter,
    producer: &FutureProducer,
    static_headers: &[(String, Vec<u8>)],
    send_timeout: Duration,
    wait: u64,
) -> Result<()> {
    if line.trim().is_empty() {
        return Ok(());
    }
    let value: Value = serde_json::from_str(line)
        .map_err(|e| anyhow!("Invalid JSON on line {line_number}: {e}"))?;
    let batch = parse_batch(&value, format).map_err(|e| anyhow!("line {line_number}: {e}"))?;
    send_batch(producer, &batch, static_headers, send_timeout).await?;
    if wait > 0 {
        tokio::time::sleep(Duration::from_millis(wait)).await;
    }
    Ok(())
}

fn future_record<'a>(topic: &'a str, value: Option<&'a [u8]>) -> FutureRecord<'a, [u8], [u8]> {
    match value {
        Some(value) => FutureRecord::to(topic).payload(value),
        None => FutureRecord::to(topic),
    }
}

async fn send_batch(
    producer: &FutureProducer,
    batch: &Batch,
    static_headers: &[(String, Vec<u8>)],
    send_timeout: Duration,
) -> Result<()> {
    let mut futures = Vec::with_capacity(batch.messages.len());
    for encoded in &batch.messages {
        let mut record = future_record(&batch.topic, encoded.value.as_deref());
        if let Some(ref k) = encoded.key {
            record = record.key(k.as_slice());
        }
        let partition = batch.partition.or(if encoded.partition >= 0 {
            Some(encoded.partition)
        } else {
            None
        });
        if let Some(p) = partition {
            record = record.partition(p);
        }

        let mut header_pairs: Vec<(String, Vec<u8>)> = encoded.headers.clone();
        for (k, v) in static_headers {
            if !header_pairs.iter().any(|(name, _)| name == k) {
                header_pairs.push((k.clone(), v.clone()));
            }
        }
        if !header_pairs.is_empty() {
            let mut headers = OwnedHeaders::new();
            for (k, v) in &header_pairs {
                headers = headers.insert(rdkafka::message::Header {
                    key: k,
                    value: Some(v.as_slice()),
                });
            }
            record = record.headers(headers);
        }

        futures.push(producer.send(record, send_timeout));
    }

    wait_for_deliveries(futures).await
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let format = Formatter::open(&args.data_format)?;
    let mut config = build_client_config(&globals)?;
    if let Some(ref c) = args.compression {
        config.set("compression.type", c);
    }

    let producer = create_producer(&config, &globals)?;
    let static_headers = parse_static_headers(&args.header)?;
    let send_timeout = if globals.timeout_ms == 0 {
        Duration::from_secs(30)
    } else {
        Duration::from_millis(globals.timeout_ms)
    };

    let mut line_number = 0usize;
    match &args.input {
        Some(path) => {
            let file = tokio::fs::File::open(path).await?;
            let mut reader = BufReader::new(file).lines();
            while let Some(line) = reader.next_line().await? {
                line_number += 1;
                produce_line(
                    &line,
                    line_number,
                    &format,
                    &producer,
                    &static_headers,
                    send_timeout,
                    args.wait,
                )
                .await?;
            }
        }
        None => {
            let mut reader = BufReader::new(tokio::io::stdin()).lines();
            while let Some(line) = reader.next_line().await? {
                line_number += 1;
                produce_line(
                    &line,
                    line_number,
                    &format,
                    &producer,
                    &static_headers,
                    send_timeout,
                    args.wait,
                )
                .await?;
            }
        }
    }

    producer.flush(send_timeout)?;
    Ok(0)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{future_record, parse_batch};
    use crate::formatter::Formatter;

    #[test]
    fn future_record_distinguishes_null_and_empty_payloads() {
        let null_record = future_record("topic", None);
        let empty = [];
        let empty_record = future_record("topic", Some(&empty));

        assert!(null_record.payload.is_none());
        assert_eq!(empty_record.payload, Some(empty.as_slice()));
    }

    #[test]
    fn parse_batch_accepts_i32_partition_boundaries() {
        let format = Formatter::open("json").unwrap();

        for partition in [i32::MIN, i32::MAX] {
            let line = json!({
                "topic": "topic",
                "partition": partition,
                "messages": [],
            });
            let batch = parse_batch(&line, &format).unwrap();

            assert_eq!(batch.partition, Some(partition));
        }
    }

    #[test]
    fn parse_batch_rejects_partitions_outside_i32_range() {
        let format = Formatter::open("json").unwrap();

        for partition in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
            let line = json!({
                "topic": "topic",
                "partition": partition,
                "messages": [],
            });

            assert!(
                parse_batch(&line, &format).is_err(),
                "partition {partition} should be rejected"
            );
        }
    }
}
