#![cfg(all(feature = "e2e", feature = "wasm"))]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

const KAFQ: &str = env!("CARGO_BIN_EXE_kafq");

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn cli(args: &[&str]) -> String {
    let output = Command::new(KAFQ)
        .args(args)
        .args(["-b", &brokers()])
        .output()
        .expect("spawn kafq");
    if !output.status.success() {
        panic!(
            "kafq {args:?} failed: status={:?}\nstdout={}\nstderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn cli_with_stdin(args: &[&str], input: &str) {
    let mut child = Command::new(KAFQ)
        .args(args)
        .args(["-b", &brokers()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kafq");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    drop(child.stdin.take());
    let output = child.wait_with_output().expect("wait kafq");
    if !output.status.success() {
        panic!(
            "kafq {args:?} stdin run failed: status={:?}\nstderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

fn parse_jsonl(s: &str) -> Vec<Value> {
    if s.is_empty() {
        return Vec::new();
    }
    s.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn flatten(batches: &[Value]) -> Vec<Value> {
    batches
        .iter()
        .flat_map(|b| {
            b.get("messages")
                .and_then(|m| m.as_array())
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}

fn project_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
}

fn build_passthrough() -> PathBuf {
    let root = project_root();
    let plugin_dir = root.join("examples").join("passthrough");
    let status = Command::new("cargo")
        .args(["build", "--release", "--target", "wasm32-wasip2"])
        .current_dir(&plugin_dir)
        .status()
        .expect("cargo build for passthrough plugin");
    assert!(status.success(), "failed to build passthrough plugin");
    let wasm = plugin_dir
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join("passthrough.wasm");
    assert!(wasm.exists(), "expected wasm at {}", wasm.display());
    wasm
}

struct ScopedTopic {
    name: String,
}

impl Drop for ScopedTopic {
    fn drop(&mut self) {
        let _ = Command::new(KAFQ)
            .args(["topic:delete", &self.name, "-b", &brokers()])
            .output();
    }
}

#[test]
fn wasm_formatter_roundtrip() {
    let wasm = build_passthrough();
    let wasm = wasm.to_str().expect("wasm path is utf-8");

    let topic = format!("e2e-wasm-{}", now_millis());
    let _guard = ScopedTopic {
        name: topic.clone(),
    };
    cli(&["topic:create", &topic]);

    cli_with_stdin(
        &["produce", "-d", wasm],
        &format!(r#"{{"topic":"{topic}","messages":[{{"value":"hello via wasm"}}]}}"#),
    );

    let messages = flatten(&parse_jsonl(&cli(&[
        "-t",
        "10000",
        "consume",
        &topic,
        "--snapshot",
        "-d",
        wasm,
    ])));
    assert_eq!(messages.len(), 1);
    let m = messages[0].as_object().unwrap();
    assert_eq!(m["value"].as_str(), Some("hello via wasm"));
}

#[test]
fn contract_round_trips_to_disk() {
    let printed = cli(&["contract"]);
    let on_disk =
        std::fs::read_to_string(project_root().join("wit").join("formatter.wit")).unwrap();
    assert_eq!(printed, on_disk.trim_end());
}

// Quiet the unused-helpers warnings when only one #[test] runs.
#[allow(dead_code)]
fn _unused(_p: &Path) {}
