pub mod config;
pub mod consume;
pub mod contract;
pub mod copy_topic;
pub mod create_topic;
pub mod delete_topic;
pub mod dump_topic;
pub mod list;
pub mod metadata;
pub mod produce;
pub mod topic_offsets;

use std::future::Future;

use anyhow::{Context, Result, anyhow};
use futures::future::join_all;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::producer::future_producer::OwnedDeliveryResult;

async fn wait_for_deliveries<F>(futures: Vec<F>) -> Result<()>
where
    F: Future<Output = OwnedDeliveryResult>,
{
    for delivery in join_all(futures).await {
        delivery.map_err(|(error, _)| anyhow!("Failed to send message: {error}"))?;
    }
    Ok(())
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Numeric start offset a consumer will actually read from, given the resolved
/// assignment offset and the partition's watermarks. A start at or past `high`
/// means there is nothing to read (retention-emptied partition where
/// `low == high`, or a `--from` timestamp resolved past the end to `Offset::End`).
pub fn start_offset(resolved: Offset, low: i64, high: i64) -> i64 {
    match resolved {
        Offset::Beginning => low,
        Offset::Offset(o) => o,
        _ => high,
    }
}

fn partition_offset(
    offsets: &TopicPartitionList,
    topic: &str,
    partition: i32,
    operation: &str,
) -> Result<Offset> {
    let elem = offsets.find_partition(topic, partition).ok_or_else(|| {
        anyhow!("{operation} returned no result for topic {topic} partition {partition}")
    })?;
    elem.error()
        .with_context(|| format!("{operation} failed for topic {topic} partition {partition}"))?;
    Ok(elem.offset())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotStop {
    Complete,
    Timeout,
    Sigint,
    Sigterm,
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<SnapshotStop> {
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    tokio::select! {
        _ = sigint.recv() => Ok(SnapshotStop::Sigint),
        _ = sigterm.recv() => Ok(SnapshotStop::Sigterm),
    }
}

#[cfg(windows)]
async fn shutdown_signal() -> Result<SnapshotStop> {
    tokio::signal::ctrl_c().await?;
    Ok(SnapshotStop::Sigint)
}

impl SnapshotStop {
    fn exit_code(self) -> i32 {
        match self {
            Self::Complete => 0,
            Self::Timeout => 1,
            Self::Sigint => 130,
            Self::Sigterm => 143,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use futures::task::AtomicWaker;
    use rdkafka::Offset;
    use rdkafka::TopicPartitionList;
    use rdkafka::message::Timestamp;
    use rdkafka::producer::future_producer::{Delivery, OwnedDeliveryResult};
    use rdkafka::types::RDKafkaRespErr;

    use super::SnapshotStop;
    use super::partition_offset;
    use super::wait_for_deliveries;

    fn rendezvous(
        started: Arc<AtomicUsize>,
        waker: Arc<AtomicWaker>,
    ) -> impl Future<Output = OwnedDeliveryResult> {
        let mut announced = false;
        poll_fn(move |context| {
            waker.register(context.waker());
            if !announced {
                announced = true;
                started.fetch_add(1, Ordering::SeqCst);
                waker.wake();
            }
            if started.load(Ordering::SeqCst) == 2 {
                Poll::Ready(Ok(Delivery {
                    partition: 0,
                    offset: 0,
                    timestamp: Timestamp::NotAvailable,
                }))
            } else {
                Poll::Pending
            }
        })
    }

    #[tokio::test]
    async fn delivery_futures_are_polled_concurrently() {
        let started = Arc::new(AtomicUsize::new(0));
        let waker = Arc::new(AtomicWaker::new());
        let futures = vec![
            rendezvous(started.clone(), waker.clone()),
            rendezvous(started, waker),
        ];

        tokio::time::timeout(Duration::from_millis(100), wait_for_deliveries(futures))
            .await
            .expect("delivery futures were polled serially")
            .expect("delivery failed");
    }

    #[test]
    fn partition_offset_propagates_element_error() {
        const TOPIC: &str = "events";
        const PARTITION: i32 = 3;
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition(TOPIC, PARTITION);
        unsafe {
            // The public Rust API exposes result errors read-only, so the fixture
            // must populate the underlying librdkafka result element directly.
            (*(*offsets.ptr()).elems).err = RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;
        }

        let error = partition_offset(&offsets, TOPIC, PARTITION, "Timestamp offset lookup")
            .expect_err("partition error was ignored");

        assert!(error.to_string().contains("Timestamp offset lookup"));
        assert!(error.to_string().contains("events partition 3"));
    }

    #[test]
    fn partition_offset_preserves_error_free_invalid_offset() {
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition("events", 3);

        let offset = partition_offset(&offsets, "events", 3, "Timestamp offset lookup")
            .expect("error-free invalid offset was rejected");

        assert_eq!(offset, Offset::Invalid);
    }

    #[test]
    fn snapshot_stop_exit_codes_distinguish_signals_from_completion() {
        assert_eq!(SnapshotStop::Complete.exit_code(), 0);
        assert_eq!(SnapshotStop::Timeout.exit_code(), 1);
        assert_eq!(SnapshotStop::Sigint.exit_code(), 130);
        assert_eq!(SnapshotStop::Sigterm.exit_code(), 143);
    }
}
