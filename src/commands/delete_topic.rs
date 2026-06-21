use anyhow::{Result, bail};
use clap::Args as ClapArgs;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;

use crate::client::{GlobalOptions, build_client_config};

#[derive(ClapArgs)]
pub struct Args {
    /// Topic name
    pub topic: String,
}

pub async fn run(globals: GlobalOptions, args: Args) -> Result<i32> {
    let config = build_client_config(&globals)?;
    let admin: AdminClient<DefaultClientContext> = config.create()?;
    let opts = AdminOptions::new().request_timeout(Some(globals.operation_timeout()));
    let results = admin.delete_topics(&[&args.topic], &opts).await?;
    for result in results {
        if let Err((name, code)) = result {
            bail!("Failed to delete topic {name}: {code:?}");
        }
    }
    Ok(0)
}
