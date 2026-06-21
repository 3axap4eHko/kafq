wit_bindgen::generate!({
    path: "../../wit",
    world: "formatter",
});

use exports::kafq::formatter::codec::{Decoded, Guest, KafkaRecord};
use serde_json::Value;

struct Plugin;

impl Guest for Plugin {
    fn decode(rec: KafkaRecord) -> Decoded {
        let value: Value = match rec.value.as_deref() {
            None => Value::Null,
            Some(b) => match serde_json::from_slice(b) {
                Ok(v) => v,
                Err(e) => return Decoded::Error(format!("invalid JSON in value: {e}")),
            },
        };

        let key: Value = match rec.key.as_deref() {
            None => Value::Null,
            Some(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
        };

        let mut headers = serde_json::Map::new();
        for (k, v) in &rec.headers {
            headers.insert(
                k.clone(),
                Value::String(String::from_utf8_lossy(v).into_owned()),
            );
        }

        let mut out = serde_json::Map::new();
        out.insert("key".to_string(), key);
        out.insert("headers".to_string(), Value::Object(headers));
        out.insert("value".to_string(), value);

        match serde_json::to_string(&Value::Object(out)) {
            Ok(s) => Decoded::Json(s),
            Err(e) => Decoded::Error(format!("could not serialize decoded record: {e}")),
        }
    }

    fn encode(json: String, topic: String) -> Result<KafkaRecord, String> {
        let envelope: Value = serde_json::from_str(&json)
            .map_err(|e| format!("invalid envelope JSON: {e}"))?;
        let value = envelope
            .get("value")
            .ok_or_else(|| "envelope is missing required field `value`".to_string())?;
        let value_bytes =
            serde_json::to_vec(value).map_err(|e| format!("could not serialize value: {e}"))?;

        let key_bytes = envelope.get("key").and_then(|k| match k {
            Value::String(s) => Some(s.as_bytes().to_vec()),
            Value::Null => None,
            other => Some(other.to_string().into_bytes()),
        });

        let headers = envelope
            .get("headers")
            .and_then(|h| h.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let bytes = match v {
                            Value::String(s) => s.as_bytes().to_vec(),
                            Value::Null => Vec::new(),
                            other => other.to_string().into_bytes(),
                        };
                        (k.clone(), bytes)
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(KafkaRecord {
            topic,
            partition: -1,
            key: key_bytes,
            value: Some(value_bytes),
            headers,
            timestamp: -1,
        })
    }

    fn plugin_name() -> String {
        "kafq-example:json@0.1.0".to_string()
    }
}

export!(Plugin);
