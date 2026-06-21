use std::io::stdout;

use anyhow::Result;
use clap::Args as ClapArgs;
use rdkafka::consumer::{BaseConsumer, Consumer};

use crate::client::{GlobalOptions, build_client_config};
use crate::output::write_jsonl;

#[derive(ClapArgs)]
pub struct Args {
    /// include internal topics (e.g. __consumer_offsets)
    #[arg(short = 'a', long)]
    pub all: bool,
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let config = build_client_config(&globals)?;
    let consumer: BaseConsumer = config.create()?;
    let metadata = consumer.fetch_metadata(None, globals.operation_timeout())?;

    let mut out = stdout().lock();
    let mut topics: Vec<&str> = metadata
        .topics()
        .iter()
        .map(|t| t.name())
        .filter(|name| args.all || !name.starts_with("__"))
        .collect();
    topics.sort_unstable();
    for topic in topics {
        write_jsonl(&mut out, &topic)?;
    }
    Ok(0)
}
