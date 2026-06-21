wit_bindgen::generate!({
    path: "../../wit",
    world: "formatter",
});

use exports::kafq::formatter::codec::{Decoded, Guest, KafkaRecord};

struct Plugin;

impl Guest for Plugin {
    fn decode(rec: KafkaRecord) -> Decoded {
        let bytes = rec.value.unwrap_or_default();
        match String::from_utf8(bytes) {
            Ok(s) => Decoded::Json(serde_json_string(&s)),
            Err(e) => Decoded::Error(format!("value is not UTF-8: {e}")),
        }
    }

    fn encode(json: String, topic: String) -> Result<KafkaRecord, String> {
        let value = extract_string_field(&json, "value")
            .ok_or_else(|| format!("expected envelope with string `value`, got: {json}"))?;
        Ok(KafkaRecord {
            topic,
            partition: -1,
            key: None,
            value: Some(value.into_bytes()),
            headers: Vec::new(),
            timestamp: -1,
        })
    }

    fn plugin_name() -> String {
        "kafq-example:passthrough@0.1.0".to_string()
    }
}

export!(Plugin);

// Naive JSON encoder for a single string: returns `"..."` with the four mandatory escapes.
fn serde_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// Pulls out the value at `"value":` from a JSON object. Hand-rolled to avoid
// pulling serde into the wasm binary. Handles strings only — sufficient for
// the passthrough demo.
fn extract_string_field(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\"");
    let i = json.find(&needle)? + needle.len();
    let rest = &json[i..].trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut chars = rest[1..].chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '/' => out.push('/'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                _ => return None,
            },
            c => out.push(c),
        }
    }
    None
}
