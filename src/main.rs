mod metrics;
mod transport;
mod util;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Clone, clap::ValueEnum, Debug)]
enum TransportKind {
    Udp,
    Gossip,
}

#[derive(Parser, Debug)]
#[command(version, about = "Reliability lab for UDP vs iroh-gossip")]
struct Args {
    #[arg(long, value_enum, default_value_t=TransportKind::Gossip)]
    transport: TransportKind,

    #[arg(long, value_parser = ["sender","receiver"])]
    role: String,

    #[arg(long, default_value = "lab.log.jsonl")]
    log: String,

    #[arg(long, default_value_t = 2000)]
    num: u64,
    #[arg(long, default_value_t = 50)]
    rate: u64,
    #[arg(long, default_value_t = 256)]
    size: usize,
    #[arg(long, default_value_t = 3000)]
    idle_report_ms: u64,

    #[arg(long, default_value = "0.0.0.0:4000")]
    udp_bind: String,
    #[arg(long, default_value = "127.0.0.1:4001")]
    udp_peers: String,

    #[arg(long)]
    topic_hex: Option<String>,
    #[arg(long)]
    topic_name: Option<String>,
    #[arg(long)]
    secret_hex: Option<String>,
    #[arg(long, default_value = "")]
    bootstrap: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    let mut logger = metrics::JsonWriter::new(&args.log)?;

    match (args.transport, args.role.as_str()) {
        (TransportKind::Udp, "sender") => {
            let peers = args
                .udp_peers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            let transport = transport::UdpBaseline::bind(&args.udp_bind, peers).await?;
            transport::run_sender(
                transport,
                "udp",
                &mut logger,
                args.num,
                args.rate,
                args.size,
            )
            .await?;
        }
        (TransportKind::Udp, "receiver") => {
            let peers: Vec<String> = vec![];
            let transport = transport::UdpBaseline::bind(&args.udp_bind, peers).await?;
            let summary =
                transport::run_receiver(transport, "udp", &mut logger, args.idle_report_ms).await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        (TransportKind::Gossip, "sender") => {
            let transport = transport::IrohGossip::connect(
                args.topic_hex.clone(),
                args.topic_name.clone(),
                args.secret_hex.clone(),
                args.bootstrap
                    .split(',')
                    .filter(|sender| !sender.is_empty())
                    .map(|sender| sender.to_string())
                    .collect(),
            )
            .await?;
            transport::run_sender(
                transport,
                "gossip",
                &mut logger,
                args.num,
                args.rate,
                args.size,
            )
            .await?;
        }
        (TransportKind::Gossip, "receiver") => {
            let transport = transport::IrohGossip::connect(
                args.topic_hex.clone(),
                args.topic_name.clone(),
                args.secret_hex.clone(),
                args.bootstrap
                    .split(',')
                    .filter(|sender| !sender.is_empty())
                    .map(|sender| sender.to_string())
                    .collect(),
            )
            .await?;
            let summary =
                transport::run_receiver(transport, "gossip", &mut logger, args.idle_report_ms)
                    .await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        _ => anyhow::bail!("invalid role"),
    }
    Ok(())
}
