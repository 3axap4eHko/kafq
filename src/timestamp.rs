use anyhow::{Result, anyhow};
use chrono::DateTime;

pub fn parse_timestamp_ms(value: &str) -> Result<i64> {
    if value.chars().all(|c| c.is_ascii_digit()) {
        value
            .parse::<i64>()
            .map_err(|e| anyhow!("Invalid timestamp \"{value}\": {e}"))
    } else {
        let parsed = DateTime::parse_from_rfc3339(value)
            .map_err(|_| anyhow!("Invalid timestamp \"{value}\""))?;
        Ok(parsed.timestamp_millis())
    }
}
