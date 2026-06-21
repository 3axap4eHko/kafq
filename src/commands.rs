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

use rdkafka::Offset;

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
