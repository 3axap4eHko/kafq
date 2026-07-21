use anyhow::{Result, anyhow};
use serde_json::{Map, Value};

#[cfg(feature = "wasm")]
use crate::wasm_formatter::WasmFormatter;

#[allow(dead_code)]
pub struct RecordView<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [(String, Vec<u8>)],
    pub timestamp: i64,
}

/// Plugin output for a single decoded record. The host merges the fields the
/// plugin chose to emit into the JSONL line; host-owned fields (partition,
/// offset, timestamp, ahead) are always written by the host and override
/// plugin-supplied keys with the same name.
pub struct DecodedRecord {
    /// JSON object whose fields are merged into the JSONL output line.
    pub fields: Map<String, Value>,
}

impl DecodedRecord {
    pub fn from_json(value: Value) -> Self {
        match value {
            Value::Object(obj) => DecodedRecord { fields: obj },
            other => {
                let mut fields = Map::new();
                fields.insert("value".to_string(), other);
                DecodedRecord { fields }
            }
        }
    }
}

/// Plugin output for a single produced record.
pub struct EncodedRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    /// -1 = let the producer choose.
    pub partition: i32,
}

pub enum Formatter {
    Json,
    Raw,
    #[cfg(feature = "wasm")]
    Wasm(WasmFormatter),
}

impl Formatter {
    pub fn open(spec: &str) -> Result<Self> {
        match spec {
            "json" => Ok(Formatter::Json),
            "raw" => Ok(Formatter::Raw),
            path if path.ends_with(".wasm") => Self::open_wasm(path),
            other => Err(anyhow!(
                "Unsupported data format \"{other}\" (use json, raw, or a path ending in .wasm)"
            )),
        }
    }

    #[cfg(feature = "wasm")]
    fn open_wasm(path: &str) -> Result<Self> {
        Ok(Formatter::Wasm(WasmFormatter::load(path)?))
    }

    #[cfg(not(feature = "wasm"))]
    fn open_wasm(_path: &str) -> Result<Self> {
        Err(anyhow!(
            "WASM formatter requested but this kafq build was compiled without the `wasm` feature"
        ))
    }

    pub fn decode(&self, view: RecordView<'_>) -> Result<DecodedRecord> {
        match self {
            Formatter::Json => Ok(DecodedRecord::from_json(builtin_decode_object(
                &view,
                |bytes| Ok(serde_json::from_slice(bytes)?),
            )?)),
            Formatter::Raw => Ok(DecodedRecord::from_json(builtin_decode_object(
                &view,
                |bytes| Ok(Value::String(String::from_utf8_lossy(bytes).into_owned())),
            )?)),
            #[cfg(feature = "wasm")]
            Formatter::Wasm(f) => f.decode(view),
        }
    }

    pub fn encode(&self, line: &Value, _topic: &str) -> Result<EncodedRecord> {
        match self {
            Formatter::Json => builtin_encode(line, |v| Ok(serde_json::to_vec(v)?)),
            Formatter::Raw => builtin_encode(line, |v| {
                Ok(match v {
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Null => b"null".to_vec(),
                    other => other.to_string().into_bytes(),
                })
            }),
            #[cfg(feature = "wasm")]
            Formatter::Wasm(f) => f.encode(line, _topic),
        }
    }
}

fn builtin_decode_object(
    view: &RecordView<'_>,
    decode_value: impl Fn(&[u8]) -> Result<Value>,
) -> Result<Value> {
    let mut headers_obj = Map::new();
    for (k, v) in view.headers {
        headers_obj.insert(
            k.clone(),
            Value::String(String::from_utf8_lossy(v).into_owned()),
        );
    }

    let key_value = match view.key {
        Some(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
        None => Value::Null,
    };

    let value_value = match view.value {
        Some(b) => decode_value(b)?,
        None => Value::Null,
    };

    let mut obj = Map::new();
    obj.insert("headers".to_string(), Value::Object(headers_obj));
    obj.insert("key".to_string(), key_value);
    obj.insert("value".to_string(), value_value);
    Ok(Value::Object(obj))
}

fn builtin_encode(
    line: &Value,
    encode_value: impl Fn(&Value) -> Result<Vec<u8>>,
) -> Result<EncodedRecord> {
    let envelope = match line {
        Value::Object(obj) => obj,
        _ => {
            return Err(anyhow!(
                "produce input line must be a JSON object with a `value` field"
            ));
        }
    };

    let value_field = envelope
        .get("value")
        .ok_or_else(|| anyhow!("produce input line is missing the required `value` field"))?;
    let value_bytes = encode_value(value_field)?;

    let key_bytes = envelope.get("key").and_then(value_to_bytes);

    let headers = envelope
        .get("headers")
        .and_then(|v| v.as_object())
        .map(|map| {
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_bytes(v).unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();

    Ok(EncodedRecord {
        key: key_bytes,
        value: Some(value_bytes),
        headers,
        partition: -1,
    })
}

fn value_to_bytes(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.as_bytes().to_vec()),
        other => Some(other.to_string().into_bytes()),
    }
}
