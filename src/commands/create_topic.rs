use std::io::stdout;

use anyhow::{Result, bail};
use clap::Args as ClapArgs;
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
use serde::Serialize;

use crate::client::{GlobalOptions, build_client_config, create_admin};
use crate::output::write_jsonl;

#[derive(ClapArgs)]
pub struct Args {
    /// Topic name
    pub topic: String,
    /// number of partitions
    #[arg(short = 'p', long, default_value_t = 1)]
    pub partitions: i32,
    /// replication factor
    #[arg(short = 'r', long, default_value_t = 1)]
    pub replicas: i32,
}

#[derive(Serialize)]
struct CreateResult {
    name: String,
    partitions: i32,
    replicas: i32,
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let config = build_client_config(&globals)?;
    let admin = create_admin(&config, &globals)?;
    let opts = AdminOptions::new().request_timeout(Some(globals.operation_timeout()));
    let new_topic = NewTopic::new(
        &args.topic,
        args.partitions,
        TopicReplication::Fixed(args.replicas),
    );
    let results = admin.create_topics([&new_topic], &opts).await?;

    let mut out = stdout().lock();
    for result in results {
        match result {
            Ok(name) => {
                write_jsonl(
                    &mut out,
                    &CreateResult {
                        name,
                        partitions: args.partitions,
                        replicas: args.replicas,
                    },
                )?;
            }
            Err((name, code)) => {
                bail!("Failed to create topic {name}: {code:?}");
            }
        }
    }
    Ok(0)
}
