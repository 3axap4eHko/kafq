#![cfg(feature = "e2e")]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

const KAFQ: &str = env!("CARGO_BIN_EXE_kafq");

fn brokers() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn now_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

fn cli(args: &[&str]) -> String {
    let output = Command::new(KAFQ)
        .args(args)
        .args(["-b", &brokers()])
        .output()
        .expect("failed to spawn kafq");
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

fn cli_allow_fail(args: &[&str]) -> (i32, String, String) {
    let output = Command::new(KAFQ)
        .args(args)
        .args(["-b", &brokers()])
        .output()
        .expect("failed to spawn kafq");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8(output.stdout).unwrap().trim().to_string(),
        String::from_utf8(output.stderr).unwrap().trim().to_string(),
    )
}

fn cli_with_stdin(args: &[&str], input: &str) {
    let mut child = Command::new(KAFQ)
        .args(args)
        .args(["-b", &brokers()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kafq");
    child.stdin.as_mut().unwrap().write_all(input.as_bytes()).unwrap();
    drop(child.stdin.take());
    let output = child.wait_with_output().expect("failed to wait for kafq");
    if !output.status.success() {
        panic!(
            "kafq {args:?} stdin-driven run failed: status={:?}\nstderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

fn parse_jsonl(s: &str) -> Vec<Value> {
    if s.is_empty() {
        return Vec::new();
    }
    s.lines().map(|line| serde_json::from_str(line).unwrap()).collect()
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

fn tmp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kafq-e2e-{}", now_millis()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(path: &Path, content: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
}

fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap()
}

fn obj(v: &Value) -> &serde_json::Map<String, Value> {
    v.as_object().expect("expected JSON object")
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

#[test]
fn cli_e2e_full_lifecycle() {
    let test_topic = format!("e2e-test-{}", now_millis());
    let test_group = format!("e2e-group-{}", now_millis());
    let _topic_guard = ScopedTopic { name: test_topic.clone() };
    let tmp = tmp_dir();

    // metadata
    {
        let output = cli(&["metadata"]);
        let parsed = parse_jsonl(&output);
        assert_eq!(parsed.len(), 1);
        let m = obj(&parsed[0]);
        assert!(m.contains_key("brokers"));
        let cluster_id = m["clusterId"].as_str().expect("clusterId is a string");
        assert!(!cluster_id.is_empty(), "clusterId must not be empty");
        let controller_id = m["controllerId"].as_i64().expect("controllerId is an integer");
        assert!(controller_id >= 0, "controllerId must be a real broker id, got {controller_id}");
        let brokers = m["brokers"].as_array().expect("brokers is array");
        assert!(!brokers.is_empty());
        let b0 = obj(&brokers[0]);
        assert!(b0.contains_key("host"));
        assert!(b0.contains_key("port"));
    }

    // topic:create
    {
        let output = cli(&["topic:create", &test_topic]);
        let topics = parse_jsonl(&output);
        assert_eq!(topics.len(), 1);
        let t = obj(&topics[0]);
        assert_eq!(t["name"].as_str(), Some(test_topic.as_str()));
        assert_eq!(t["partitions"].as_i64(), Some(1));
        assert_eq!(t["replicas"].as_i64(), Some(1));
    }

    // list
    {
        let topics: Vec<String> = parse_jsonl(&cli(&["list"]))
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert!(topics.contains(&test_topic), "list missing {test_topic}");

        let ls: Vec<String> = parse_jsonl(&cli(&["ls"]))
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert!(ls.contains(&test_topic), "ls missing {test_topic}");

        let all: Vec<String> = parse_jsonl(&cli(&["list", "--all"]))
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert!(all.contains(&test_topic), "list --all missing {test_topic}");
    }

    // config
    {
        let topic_cfg = parse_jsonl(&cli(&["config", "-r", "topic", "-n", &test_topic]));
        assert!(!topic_cfg.is_empty());
        assert!(!obj(&topic_cfg[0])["configs"].as_array().unwrap().is_empty());

        let broker_cfg = parse_jsonl(&cli(&["config", "-r", "broker", "-n", "1"]));
        assert!(!broker_cfg.is_empty());
        assert!(!obj(&broker_cfg[0])["configs"].as_array().unwrap().is_empty());
    }

    // produce — single message (offset 0)
    cli_with_stdin(
        &["produce"],
        &format!(
            r#"{{"topic":"{test_topic}","messages":[{{"key":"test-key","value":"test-value","headers":{{"h1":"v1"}}}}]}}"#
        ),
    );

    // produce — three messages in one batch line (offsets 1-3)
    cli_with_stdin(
        &["produce"],
        &format!(
            r#"{{"topic":"{test_topic}","messages":[{{"key":"key-0","value":"value-0"}},{{"key":"key-1","value":"value-1"}},{{"key":"key-2","value":"value-2"}}]}}"#
        ),
    );

    // produce — static headers (offset 4)
    cli_with_stdin(
        &["produce", "-H", "x-source:test", "-H", "x-env:e2e"],
        &format!(
            r#"{{"topic":"{test_topic}","messages":[{{"key":"header-key","value":"header-value"}}]}}"#
        ),
    );

    // produce — from file (offsets 5-6)
    let input_path = tmp.join("produce-input.jsonl");
    write_file(
        &input_path,
        &format!(
            r#"{{"topic":"{test_topic}","messages":[{{"key":"file-key-0","value":"file-value-0"}},{{"key":"file-key-1","value":"file-value-1"}}]}}"#
        ),
    );
    cli(&["produce", "--input", input_path.to_str().unwrap()]);

    // produce — raw + wait (smoke only)
    cli_with_stdin(
        &["produce", "-d", "raw"],
        &format!(r#"{{"topic":"{test_topic}","messages":[{{"key":"raw-key","value":"raw-value"}}]}}"#),
    );
    cli_with_stdin(
        &["produce", "--wait", "10"],
        &format!(r#"{{"topic":"{test_topic}","messages":[{{"key":"wait-key","value":"wait-value"}}]}}"#),
    );

    // consume — from beginning, 4 messages
    {
        let group = format!("{test_group}-from0");
        let batches = parse_jsonl(&cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", "0", "--count", "4",
        ]));
        let messages = flatten(&batches);
        assert_eq!(messages.len(), 4);
        let m0 = obj(&messages[0]);
        assert_eq!(m0["key"].as_str(), Some("test-key"));
        assert_eq!(m0["value"].as_str(), Some("test-value"));
        let headers = m0["headers"].as_object().expect("headers object");
        assert_eq!(headers["h1"].as_str(), Some("v1"));
        for key in ["offset", "timestamp", "ahead"] {
            assert!(m0.contains_key(key), "missing {key} in {m0:?}");
        }
        let b0 = obj(&batches[0]);
        assert_eq!(b0["topic"].as_str(), Some(test_topic.as_str()));
        assert!(b0.contains_key("partition"), "batch line missing partition");
    }

    // consume --skip
    {
        let group = format!("{test_group}-skip");
        let messages = flatten(&parse_jsonl(&cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", "0", "--skip", "2", "--count", "2",
        ])));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["key"].as_str(), Some("key-1"));
        assert_eq!(messages[1]["key"].as_str(), Some("key-2"));
    }

    // consume --from ISO
    {
        let past_ms = now_millis() as i64 - 120_000;
        let past_iso = format_iso(past_ms);
        let group = format!("{test_group}-iso");
        let messages = flatten(&parse_jsonl(&cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", &past_iso, "--count", "1",
        ])));
        assert_eq!(messages.len(), 1);
        let m = obj(&messages[0]);
        assert!(m.contains_key("key"));
        assert!(m.contains_key("value"));
    }

    // consume --output file
    {
        let out = tmp.join("consume-output.json");
        let group = format!("{test_group}-file");
        cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", "0", "--count", "2",
            "--output", out.to_str().unwrap(),
        ]);
        assert!(out.exists(), "output file missing");
        let messages = flatten(&parse_jsonl(read_file(&out).trim()));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["key"].as_str(), Some("test-key"));
    }

    // consume -d raw (value is the JSON-encoded string from producer)
    {
        let group = format!("{test_group}-raw");
        let messages = flatten(&parse_jsonl(&cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", "0", "--count", "1", "-d", "raw",
        ])));
        assert_eq!(messages.len(), 1);
        let m = obj(&messages[0]);
        assert_eq!(m["key"].as_str(), Some("test-key"));
        assert_eq!(m["value"].as_str(), Some("\"test-value\""));
    }

    // consume — static headers at offset 4
    {
        let group = format!("{test_group}-headers");
        let messages = flatten(&parse_jsonl(&cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", "0", "--skip", "4", "--count", "1",
        ])));
        assert_eq!(messages.len(), 1);
        let m = obj(&messages[0]);
        assert_eq!(m["key"].as_str(), Some("header-key"));
        let h = m["headers"].as_object().unwrap();
        assert_eq!(h["x-source"].as_str(), Some("test"));
        assert_eq!(h["x-env"].as_str(), Some("e2e"));
    }

    // consume — file-produced messages at offsets 5-6
    {
        let group = format!("{test_group}-fileinput");
        let messages = flatten(&parse_jsonl(&cli(&[
            "-t", "10000", "consume", &test_topic, "-g", &group, "--from", "0", "--skip", "5", "--count", "2",
        ])));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["key"].as_str(), Some("file-key-0"));
        assert_eq!(messages[1]["key"].as_str(), Some("file-key-1"));
    }

    // consume timeout — fresh group on a brand-new empty topic
    {
        let empty_topic = format!("e2e-empty-{}", now_millis());
        let _g = ScopedTopic { name: empty_topic.clone() };
        cli(&["topic:create", &empty_topic]);
        let group = format!("{test_group}-timeout");
        let (code, _stdout, stderr) =
            cli_allow_fail(&["-t", "2000", "consume", &empty_topic, "-g", &group]);
        assert_ne!(code, 0, "expected non-zero exit on timeout");
        assert!(stderr.contains("TIMEOUT"), "stderr was: {stderr}");
    }

    // SIGINT — exit cleanly without TIMEOUT, after delivering at least one message
    {
        let group = format!("{test_group}-sigint");
        let child: Child = Command::new(KAFQ)
            .args([
                "-b", &brokers(),
                "consume", &test_topic, "-g", &group, "--from", "0", "--count", "1",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn consume for SIGINT");
        std::thread::sleep(Duration::from_millis(800));
        // Send SIGINT
        let pid = child.id() as i32;
        unsafe { libc_kill(pid, SIGINT) };
        let output = child.wait_with_output().expect("wait sigint child");
        let code = output.status.code().unwrap_or(-1);
        assert_eq!(code, 0, "sigint exit not clean: status={:?}", output.status);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("TIMEOUT"), "stderr was: {stderr}");
    }

    // topic:offsets — high/low
    {
        let offsets = parse_jsonl(&cli(&["topic:offsets", &test_topic]));
        assert!(!offsets.is_empty());
        let o0 = obj(&offsets[0]);
        for key in ["partition", "offset", "high", "low"] {
            assert!(o0.contains_key(key));
        }
        let high: i64 = o0["high"].as_str().unwrap().parse().unwrap();
        assert!(high >= 4, "expected high >= 4, got {high}");
    }

    // topic:offsets -g
    {
        let group = format!("{test_group}-from0");
        let offsets = parse_jsonl(&cli(&["topic:offsets", &test_topic, "-g", &group]));
        assert_eq!(offsets.len(), 1);
        let o = obj(&offsets[0]);
        assert_eq!(o["topic"].as_str(), Some(test_topic.as_str()));
        assert!(o["partitions"].is_array());
    }

    // topic:offsets by timestamp
    {
        let ts_iso = format_iso(now_millis() as i64 - 60_000);
        let offsets = parse_jsonl(&cli(&["topic:offsets", &test_topic, &ts_iso]));
        assert!(!offsets.is_empty());
        let o = obj(&offsets[0]);
        assert!(o.contains_key("partition"));
        assert!(o.contains_key("offset"));
        assert!(!o.contains_key("high"));
        assert!(!o.contains_key("low"));
    }

    // topic:delete
    {
        cli(&["topic:delete", &test_topic]);
        // give the broker a moment to reflect the deletion
        std::thread::sleep(Duration::from_millis(500));
        let topics: Vec<String> = parse_jsonl(&cli(&["list"]))
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        assert!(!topics.contains(&test_topic), "topic still present after delete");
    }
}

fn format_iso(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms).unwrap().to_rfc3339()
}

const SIGINT: i32 = 2;

#[link(name = "c")]
unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

unsafe fn libc_kill(pid: i32, sig: i32) {
    unsafe { let _ = kill(pid, sig); }
}
