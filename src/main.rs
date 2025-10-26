mod metrics;
mod transport;
mod util;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::transport::{Discovery, IrohGossip, Transport};

/// Command-line interface for iroh-gossip reliability experiments
#[derive(Parser, Debug)]
#[command(version, about = "Reliability lab for UDP vs iroh-gossip")]
struct Args {
    /// Role: sender or receiver
    #[arg(long, value_parser = ["sender","receiver"])]
    role: String,

    /// Path to JSONL log file
    #[arg(long, default_value = "lab.log.jsonl")]
    log: String,

    /// Total number of messages to send
    #[arg(long, default_value_t = 2000)]
    num: u64,

    /// Send rate (messages per second)
    #[arg(long, default_value_t = 50)]
    rate: u64,

    /// Payload size in bytes
    #[arg(long, default_value_t = 256)]
    size: usize,

    /// Milliseconds to wait after receive before summarizing
    #[arg(long, default_value_t = 3000)]
    idle_report_ms: u64,

    /// Topic hex ID (optional)
    #[arg(long)]
    topic_hex: Option<String>,

    /// Topic name (used if no hex is given)
    #[arg(long)]
    topic_name: Option<String>,
    #[arg(long)]

    /// Optional secret key (32-byte hex)
    secret_hex: Option<String>,

    /// Comma-separated list of bootstrap node IDs (for relay discovery)
    #[arg(long, default_value = "")]
    bootstrap: String,

    #[arg(long, default_value = "direct", value_parser = ["direct", "relay"])]
    discovery: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    let mut logger = metrics::JsonWriter::new(&args.log)?;

    // Determine discovery mode
    let discovery = match args.discovery.as_str() {
        "direct" => Discovery::Direct,
        "relay" => Discovery::Relay,
        _ => anyhow::bail!("Invalid discovery mode, use 'direct' or 'relay'."),
    };

    match (args.role.as_str()) {
        "sender" => {
            let gossip = IrohGossip::connect(
                args.topic_hex.clone(),
                args.topic_name.clone(),
                args.secret_hex.clone(),
                args.bootstrap
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect(),
                discovery,
            )
            .await?;

            logger.write(&metrics::LogEvent {
                ts_ms: util::now_ms(),
                role: "setup",
                peer_id: &gossip.id(),
                event: "setup",
                seq: None,
                extra: serde_json::json!({
                    "discovery": args.discovery,
                    "num": args.num,
                    "rate": args.rate,
                    "size": args.size
                }),
            })?;

            transport::run_sender(gossip, &mut logger, args.num, args.rate, args.size).await?;
        }

        "receiver" => {
            let gossip = IrohGossip::connect(
                args.topic_hex.clone(),
                args.topic_name.clone(),
                args.secret_hex.clone(),
                args.bootstrap
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect(),
                discovery,
            )
            .await?;

            logger.write(&metrics::LogEvent {
                ts_ms: util::now_ms(),
                role: "setup",
                peer_id: &gossip.id(),
                event: "setup",
                seq: None,
                extra: serde_json::json!({
                    "discovery": args.discovery
                }),
            })?;

            let summary = transport::run_receiver(gossip, &mut logger, args.idle_report_ms).await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }

        _ => anyhow::bail!("invalid role, must be 'sender' or 'receiver'"),
    }
    Ok(())
}
