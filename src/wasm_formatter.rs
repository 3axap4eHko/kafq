use std::path::Path;
use std::sync::Mutex;

use anyhow::{Result, anyhow};
use serde_json::Value;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::formatter::{DecodedRecord, EncodedRecord, RecordView};

wasmtime::component::bindgen!({
    path: "wit/formatter.wit",
    world: "formatter",
});

use exports::kafq::formatter::codec::{Decoded, KafkaRecord};

struct State {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

pub struct WasmFormatter {
    inner: Mutex<Inner>,
    name: String,
}

struct Inner {
    store: Store<State>,
    bindings: Formatter,
}

impl WasmFormatter {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let engine = Engine::default();
        let component = Component::from_file(&engine, path)
            .map_err(|e| anyhow!("Failed to load WASM component {}: {e}", path.display()))?;
        let mut linker: Linker<State> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| anyhow!("Failed to add WASI to linker: {e}"))?;
        let ctx = WasiCtxBuilder::new().build();
        let mut store = Store::new(
            &engine,
            State {
                ctx,
                table: ResourceTable::new(),
            },
        );
        let bindings = Formatter::instantiate(&mut store, &component, &linker)
            .map_err(|e| anyhow!("Failed to instantiate WASM component: {e}"))?;
        let name = bindings
            .kafq_formatter_codec()
            .call_plugin_name(&mut store)
            .unwrap_or_else(|_| "wasm:unknown".to_string());
        eprintln!("loaded WASM formatter: {name}");
        Ok(Self {
            inner: Mutex::new(Inner { store, bindings }),
            name,
        })
    }

    #[allow(dead_code)]
    pub fn plugin_name(&self) -> &str {
        &self.name
    }

    pub fn decode(&self, view: RecordView<'_>) -> Result<DecodedRecord> {
        let record = KafkaRecord {
            topic: view.topic.to_string(),
            partition: view.partition,
            key: view.key.map(|b| b.to_vec()),
            value: view.value.map(|b| b.to_vec()),
            headers: view
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            timestamp: view.timestamp,
        };
        let mut inner = self.inner.lock().unwrap();
        let Inner { bindings, store } = &mut *inner;
        let codec = bindings.kafq_formatter_codec();
        let decoded = codec
            .call_decode(store, &record)
            .map_err(|e| anyhow!("Plugin decode trap: {e}"))?;
        match decoded {
            Decoded::Json(s) => {
                let v: Value = serde_json::from_str(&s)
                    .map_err(|e| anyhow!("Plugin returned invalid JSON: {e}"))?;
                Ok(DecodedRecord::from_json(v))
            }
            Decoded::Error(msg) => Err(anyhow!("Plugin decode error: {msg}")),
        }
    }

    pub fn encode(&self, line: &Value, topic: &str) -> Result<EncodedRecord> {
        let json_str = serde_json::to_string(line)?;
        let mut inner = self.inner.lock().unwrap();
        let Inner { bindings, store } = &mut *inner;
        let codec = bindings.kafq_formatter_codec();
        let record = codec
            .call_encode(store, &json_str, topic)
            .map_err(|e| anyhow!("Plugin encode trap: {e}"))?
            .map_err(|e| anyhow!("Plugin encode error: {e}"))?;
        Ok(EncodedRecord {
            key: record.key,
            value: record.value,
            headers: record.headers,
            partition: record.partition,
        })
    }
}
