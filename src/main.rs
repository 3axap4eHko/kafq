use anyhow::Result;
use clap::{Parser, Subcommand};

mod client;
mod commands;
mod formatter;
mod output;
mod timestamp;
#[cfg(feature = "wasm")]
mod wasm_formatter;

use client::GlobalOptions;

#[derive(Parser)]
#[command(
    name = "kafq",
    about = "A command-line interface for Apache Kafka operations",
    version
)]
struct Cli {
    /// comma-separated list of bootstrap broker addresses
    #[arg(
        short = 'b',
        long,
        env = "KAFKA_BROKERS",
        default_value = "localhost:9092",
        global = true
    )]
    brokers: String,

    /// operation timeout in milliseconds (0 = no timeout)
    #[arg(
        short = 't',
        long,
        env = "KAFKA_TIMEOUT",
        default_value_t = 0,
        global = true
    )]
    timeout: u64,

    /// enable TLS connection
    #[arg(long, default_value_t = false, global = true)]
    ssl: bool,

    /// disable TLS certificate verification (requires --ssl)
    #[arg(long, default_value_t = false, global = true)]
    insecure: bool,

    /// SASL mechanism: plain, scram-sha-256, scram-sha-512, oauthbearer
    #[arg(long, env = "KAFKA_MECHANISM", global = true)]
    mechanism: Option<String>,

    /// SASL username (for plain/scram mechanisms)
    #[arg(long, env = "KAFKA_USERNAME", global = true)]
    username: Option<String>,

    /// SASL password (for plain/scram mechanisms)
    #[arg(long, env = "KAFKA_PASSWORD", global = true)]
    password: Option<String>,

    /// SASL OAuth bearer token (static; mutually exclusive with --oidc-*)
    #[arg(
        long,
        env = "KAFKA_OAUTH_BEARER",
        global = true,
        requires_all = ["oauth_principal", "oauth_expiry_ms"],
        conflicts_with_all = [
            "oidc_token_url",
            "oidc_client_id",
            "oidc_client_secret",
            "oidc_scope",
            "oidc_extensions"
        ]
    )]
    oauth_bearer: Option<String>,

    /// Kafka principal associated with a static OAUTHBEARER token
    #[arg(
        long,
        env = "KAFKA_OAUTH_PRINCIPAL",
        global = true,
        requires = "oauth_bearer"
    )]
    oauth_principal: Option<String>,

    /// static OAUTHBEARER token expiration as Unix milliseconds
    #[arg(
        long,
        env = "KAFKA_OAUTH_EXPIRY_MS",
        global = true,
        requires = "oauth_bearer"
    )]
    oauth_expiry_ms: Option<i64>,

    /// OIDC token endpoint URL (enables OAUTHBEARER OIDC flow)
    #[arg(long, env = "KAFKA_OIDC_TOKEN_URL", global = true)]
    oidc_token_url: Option<String>,

    /// OIDC client id
    #[arg(long, env = "KAFKA_OIDC_CLIENT_ID", global = true)]
    oidc_client_id: Option<String>,

    /// OIDC client secret
    #[arg(long, env = "KAFKA_OIDC_CLIENT_SECRET", global = true)]
    oidc_client_secret: Option<String>,

    /// OIDC scope
    #[arg(long, env = "KAFKA_OIDC_SCOPE", global = true)]
    oidc_scope: Option<String>,

    /// SASL OAUTHBEARER extensions (key=value,key=value)
    #[arg(long, env = "KAFKA_OIDC_EXTENSIONS", global = true)]
    oidc_extensions: Option<String>,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    fn global(&self) -> GlobalOptions {
        GlobalOptions {
            brokers: self.brokers.clone(),
            timeout_ms: self.timeout,
            ssl: self.ssl,
            insecure: self.insecure,
            mechanism: self.mechanism.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            oauth_bearer: self.oauth_bearer.clone(),
            oauth_principal: self.oauth_principal.clone(),
            oauth_expiry_ms: self.oauth_expiry_ms,
            oidc_token_url: self.oidc_token_url.clone(),
            oidc_client_id: self.oidc_client_id.clone(),
            oidc_client_secret: self.oidc_client_secret.clone(),
            oidc_scope: self.oidc_scope.clone(),
            oidc_extensions: self.oidc_extensions.clone(),
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Consume messages from a Kafka topic. Outputs JSONL.
    Consume(commands::consume::Args),
    /// Produce messages to a Kafka topic from JSONL on stdin or a file.
    Produce(commands::produce::Args),
    /// Display cluster metadata: brokers, controller, topics, partitions, replicas, ISR
    Metadata,
    /// List topic names, one per line (JSONL).
    #[command(alias = "ls")]
    List(commands::list::Args),
    /// Describe the configuration of a Kafka resource.
    Config(commands::config::Args),
    /// Create a new Kafka topic.
    #[command(name = "topic:create")]
    TopicCreate(commands::create_topic::Args),
    /// Delete a Kafka topic.
    #[command(name = "topic:delete")]
    TopicDelete(commands::delete_topic::Args),
    /// Show topic partition offsets (watermarks, by timestamp, or committed by group).
    #[command(name = "topic:offsets")]
    TopicOffsets(commands::topic_offsets::Args),
    /// Copy messages from one topic to another (snapshot mode).
    #[command(name = "topic:copy")]
    TopicCopy(commands::copy_topic::Args),
    /// Dump messages from a topic to a JSONL file (snapshot mode).
    #[command(name = "topic:dump")]
    TopicDump(commands::dump_topic::Args),
    /// Print the WIT contract that WASM formatter plugins must implement.
    Contract,
}

#[tokio::main]
async fn main() {
    let exit_code = match run().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            1
        }
    };
    std::process::exit(exit_code);
}

async fn run() -> Result<i32> {
    let cli = Cli::parse();
    let globals = cli.global();
    match cli.command {
        Command::Consume(args) => commands::consume::run(globals, args).await,
        Command::Produce(args) => commands::produce::run(globals, args).await,
        Command::Metadata => commands::metadata::run(globals).await,
        Command::List(args) => commands::list::run(globals, args).await,
        Command::Config(args) => commands::config::run(globals, args).await,
        Command::TopicCreate(args) => commands::create_topic::run(globals, args).await,
        Command::TopicDelete(args) => commands::delete_topic::run(globals, args).await,
        Command::TopicOffsets(args) => commands::topic_offsets::run(globals, args).await,
        Command::TopicCopy(args) => commands::copy_topic::run(globals, args).await,
        Command::TopicDump(args) => commands::dump_topic::run(globals, args).await,
        Command::Contract => {
            use std::io::Write;
            let mut out = std::io::stdout().lock();
            out.write_all(commands::contract::WIT)?;
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap::error::ErrorKind;

    use super::Cli;

    #[test]
    fn zero_count_is_rejected_for_message_commands() {
        let cases: &[&[&str]] = &[
            &["kafq", "consume", "source", "--count", "0"],
            &["kafq", "topic:copy", "source", "dest", "--count", "0"],
            &[
                "kafq",
                "topic:dump",
                "source",
                "--output",
                "dump.jsonl",
                "--count",
                "0",
            ],
        ];

        for args in cases {
            let error = match Cli::try_parse_from(*args) {
                Ok(_) => panic!("zero count was accepted for {args:?}"),
                Err(error) => error,
            };

            assert_eq!(error.kind(), ErrorKind::ValueValidation);
        }
    }

    #[test]
    fn positive_and_omitted_counts_are_accepted() {
        let cases: &[&[&str]] = &[
            &["kafq", "consume", "source"],
            &["kafq", "consume", "source", "--count", "1"],
            &["kafq", "topic:copy", "source", "dest"],
            &["kafq", "topic:copy", "source", "dest", "--count", "1"],
            &["kafq", "topic:dump", "source", "--output", "dump.jsonl"],
            &[
                "kafq",
                "topic:dump",
                "source",
                "--output",
                "dump.jsonl",
                "--count",
                "1",
            ],
        ];

        for args in cases {
            if let Err(error) = Cli::try_parse_from(*args) {
                panic!("valid count was rejected for {args:?}: {error}");
            }
        }
    }
}
