use std::io::Write;

use anyhow::Result;
use serde::Serialize;

pub fn write_jsonl<W: Write + ?Sized, T: Serialize>(out: &mut W, value: &T) -> Result<()> {
    let line = serde_json::to_string(value)?;
    out.write_all(line.as_bytes())?;
    out.write_all(b"\n")?;
    Ok(())
}

