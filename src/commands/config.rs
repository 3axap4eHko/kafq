use std::io::stdout;

use anyhow::{Result, bail};
use clap::Args as ClapArgs;
use rdkafka::admin::{AdminOptions, ConfigSource, ResourceSpecifier};
use serde::Serialize;
use serde_json::Value;

use crate::client::{GlobalOptions, build_client_config, create_admin};
use crate::output::write_jsonl;

#[derive(ClapArgs)]
pub struct Args {
    /// resource type: topic, broker, broker_logger
    #[arg(short = 'r', long)]
    pub resource: String,
    /// resource name (topic name or broker ID)
    #[arg(short = 'n', long = "resourceName")]
    pub resource_name: String,
}

#[derive(Serialize)]
struct ConfigEntry {
    name: String,
    value: Value,
    #[serde(rename = "readOnly")]
    read_only: bool,
    #[serde(rename = "isDefault")]
    is_default: bool,
    sensitive: bool,
    source: &'static str,
    synonyms: Vec<Value>,
}

#[derive(Serialize)]
struct ResourceResult {
    #[serde(rename = "resourceType")]
    resource_type: String,
    #[serde(rename = "resourceName")]
    resource_name: String,
    configs: Vec<ConfigEntry>,
}

fn source_label(source: ConfigSource) -> &'static str {
    match source {
        ConfigSource::Unknown => "UNKNOWN_CONFIG",
        ConfigSource::DynamicTopic => "DYNAMIC_TOPIC_CONFIG",
        ConfigSource::DynamicBroker => "DYNAMIC_BROKER_CONFIG",
        ConfigSource::DynamicDefaultBroker => "DYNAMIC_DEFAULT_BROKER_CONFIG",
        ConfigSource::StaticBroker => "STATIC_BROKER_CONFIG",
        ConfigSource::Default => "DEFAULT_CONFIG",
    }
}

fn normalize_resource(resource: &str) -> Result<&'static str> {
    match resource.to_ascii_lowercase().as_str() {
        "topic" => Ok("TOPIC"),
        "broker" => Ok("BROKER"),
        "broker_logger" | "logger" => Ok("BROKER_LOGGER"),
        "any" => Ok("UNKNOWN"),
        other => bail!("Unsupported resource type: {other}"),
    }
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let resource_kind = normalize_resource(&args.resource)?;
    let config = build_client_config(&globals)?;
    let admin = create_admin(&config, &globals)?;

    let specifier = match resource_kind {
        "TOPIC" => ResourceSpecifier::Topic(&args.resource_name),
        "BROKER" => {
            let id: i32 = args
                .resource_name
                .parse()
                .map_err(|_| anyhow::anyhow!("Broker resource name must be a numeric ID"))?;
            ResourceSpecifier::Broker(id)
        }
        _ => bail!("Resource type {resource_kind} is not supported by rdkafka admin client"),
    };

    let opts = AdminOptions::new().request_timeout(Some(globals.operation_timeout()));
    let results = admin.describe_configs([&specifier], &opts).await?;

    let mut out = stdout().lock();
    let mut output_results: Vec<ResourceResult> = Vec::new();
    for result in results {
        match result {
            Ok(config_resource) => {
                let configs = config_resource
                    .entries
                    .into_iter()
                    .map(|entry| ConfigEntry {
                        name: entry.name,
                        value: entry.value.map(Value::String).unwrap_or(Value::Null),
                        read_only: entry.is_read_only,
                        is_default: entry.is_default,
                        sensitive: entry.is_sensitive,
                        source: source_label(entry.source),
                        synonyms: Vec::new(),
                    })
                    .collect();
                output_results.push(ResourceResult {
                    resource_type: resource_kind.to_string(),
                    resource_name: args.resource_name.clone(),
                    configs,
                });
            }
            Err(err) => {
                bail!("Failed to describe configs: {err:?}");
            }
        }
    }

    for r in &output_results {
        write_jsonl(&mut out, r)?;
    }
    Ok(0)
}
