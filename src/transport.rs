use crate::metrics::{DataMsg, LogEvent, Stats, Summary};
use crate::util::{now_ms, pad_payload, topic_from_name};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use iroh::NodeId;
use iroh::{Endpoint, RelayMode, SecretKey, protocol::Router};
use iroh_gossip::api::GossipTopic;
use iroh_gossip::{ALPN, api::Event, net::Gossip, proto::TopicId};
use postcard;
use rand::RngCore;
use std::str::FromStr;
use tokio::{
    select,
    time::{Duration, sleep},
};
use tokio_stream::StreamExt;
use tracing::warn;

/// Defines the discovery mode for gossip peers.
#[derive(Debug, Clone, Copy)]
pub enum Discovery {
    /// Direct discovery (peers connect directly to each other)
    Direct,
    /// Relay-assisted discovery using one or more bootstrap peers
    Relay,
}

/// Trait for irop-gossip transport
#[async_trait]
pub trait Transport: Send + Sync {
    /// Returns a human-readable identifier for this transport instance.
    fn id(&self) -> String;

    /// Broadcasts a byte buffer to all peers in the topic.
    async fn broadcast(&self, bytes: Bytes) -> Result<()>;

    /// Waits for and returns the next recieved message (or an error).
    async fn next(&mut self) -> Option<Result<Bytes>>;
}

/// Implementation of the gossip-based transport using the 'iroh-gossip' protocol.
pub struct IrohGossip {
    /// Local node ID (as string for logging)
    id: String,
    /// Stream for receiving messages
    rx: tokio_stream::wrappers::ReceiverStream<anyhow::Result<Bytes>>,
    /// Channel for outgoing messages
    tx: tokio::sync::mpsc::Sender<Bytes>,
    /// Underlying iroh endpoint
    _endpoint: Endpoint,
    /// Router integrating gossip into the iroh protocol stack
    _router: Router,
    /// The gossip protocol instance itself
    _gossip: Gossip,
    /// Discovery mode (direct or relay)
    _discovery: Discovery,
}

impl IrohGossip {
    /// Establishes an iroh-gossip connection for the given topic and discovery mode.
    ///
    /// # Arguments
    /// * `topic_hex` – Optional topic ID in hexadecimal format.
    /// * `topic_name` – Optional topic name (used if `topic_hex` is not given).
    /// * `secret_hex` – Optional secret key for reproducible node IDs.
    /// * `bootstrap` – List of bootstrap node IDs (only used for relay mode).
    /// * `discovery` – Discovery type (`Direct` or `Relay`).
    pub async fn connect(
        topic_hex: Option<String>,
        topic_name: Option<String>,
        secret_hex: Option<String>,
        bootstrap: Vec<String>,
        discovery: Discovery,
    ) -> Result<Self> {
        // Optional secret key ensures a deterministic node ID.
        let mut builder = Endpoint::builder();

        if let Some(ref hex) = secret_hex {
            let bytes = hex::decode(hex)?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
            let secret_key = SecretKey::from_bytes(&arr);
            builder = builder.secret_key(secret_key);
        }

        // Configure Discovery depending on selected mode
        builder = match discovery {
            Discovery::Direct => builder
                .discovery_local_network()
                .relay_mode(RelayMode::Disabled),
            Discovery::Relay => builder.discovery_n0().relay_mode(RelayMode::Default),
        };

        // Create endpoint and print node_id for scripts/orchestration.
        let endpoint = builder.bind().await?;
        let id = endpoint.node_id().to_string();
        eprintln!("node_id={}", id);

        // Initialize gossip and router.
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(ALPN, gossip.clone())
            .spawn();

        // Determine topic ID (hex, name-derived, or random).
        let topic = if let Some(h) = topic_hex {
            TopicId::from_str(&h)?
        } else if let Some(n) = topic_name {
            TopicId::from_bytes(topic_from_name(&n))
        } else {
            let mut rnd = [0u8; 32];
            rand::rng().fill_bytes(&mut rnd);
            TopicId::from_bytes(rnd)
        };

        // Optional bootstrap peers (used only in relay discovery).
        let node_ids: Vec<NodeId> = bootstrap
            .into_iter()
            .filter_map(|b| b.parse::<NodeId>().ok())
            .collect();

        if !node_ids.is_empty() {
            eprintln!("bootstraps_parsed={}", node_ids.len());
        }

        // Join the topic and get a handle for message broadcast + receive.
        let mut topic_handle: GossipTopic = gossip.subscribe_and_join(topic, node_ids).await?;
        topic_handle.joined().await.ok();
        let (sender, mut receiver) = topic_handle.split();

        //  Create two channels:
        //  * tx/mpsc_rx for outgoing messages,
        //  * mpsc_tx2/mpsc_rx2 for forwarding received messages to the app.
        let (tx, mut mpsc_rx) = tokio::sync::mpsc::channel::<Bytes>(128);
        let (mpsc_tx2, mpsc_rx2) = tokio::sync::mpsc::channel::<anyhow::Result<Bytes>>(1024);

        tokio::spawn(async move {
            while let Some(broadcast) = mpsc_rx.recv().await {
                if let Err(e) = sender.broadcast(broadcast).await {
                    warn!("gossip broadcast error: {e:?}");
                }
            }
        });

        // Task handling outgoing gossip broadcasts.
        tokio::spawn(async move {
            loop {
                match receiver.next().await {
                    Some(Ok(Event::Received(m))) => {
                        let _ = mpsc_tx2.send(Ok(Bytes::from(m.content))).await;
                    }
                    Some(Ok(Event::Lagged)) => {
                        let _ = mpsc_tx2.send(Err(anyhow::anyhow!("lagged"))).await;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        let _ = mpsc_tx2.send(Err(e.into())).await;
                    }
                    None => break,
                }
            }
        });

        Ok(Self {
            id,
            rx: tokio_stream::wrappers::ReceiverStream::new(mpsc_rx2),
            tx,
            _endpoint: endpoint,
            _router: router,
            _gossip: gossip,
            _discovery: discovery,
        })
    }
}

#[async_trait]
impl Transport for IrohGossip {
    fn id(&self) -> String {
        format!("gossip:{}", self.id)
    }
    async fn broadcast(&self, bytes: Bytes) -> Result<()> {
        self.tx.send(bytes).await?;
        Ok(())
    }
    async fn next(&mut self) -> Option<Result<Bytes>> {
        self.rx.next().await.map(|r| r.map_err(|e| e.into()))
    }
}

/// Runs the sender role: generates `DataMsg`s, sends them at a given rate,
/// and logs each send event.
///
/// # Parameters
/// * `transport` – Active gossip transport.
/// * `log` – JSONL writer for structured logs.
/// * `test_total` – Total number of messages to send.
/// * `rate_per_sec` – Send rate in messages per second.
/// * `payload_size` – Size of each payload in bytes.
pub async fn run_sender<T: Transport>(
    transport: T,
    log: &mut crate::metrics::JsonWriter,
    test_total: u64,
    rate_per_sec: u64,
    payload_size: usize,
) -> anyhow::Result<()> {
    let mut test_id = [0u8; 16];
    rand::rng().fill_bytes(&mut test_id);

    // Determine inter-send interval (in ms) based on desired rate.
    let interval = std::cmp::max(1, (1000 / rate_per_sec.max(1)) as i64) as u64;

    for seq in 0..test_total {
        // Build message.
        let msg = DataMsg {
            test_id,
            seq,
            sent_ms: now_ms(),
            total: test_total,
            pad: vec![],
        };

        // Serialize and pad message payload
        let mut bytes = postcard::to_allocvec(&msg)?;
        bytes = pad_payload(bytes, payload_size);

        // Broadcast to gossip peers.
        transport.broadcast(Bytes::from(bytes)).await?;

        // Log the event.
        log.write(&LogEvent {
            ts_ms: now_ms(),
            role: "sender",
            peer_id: &transport.id(),
            event: "send",
            seq: Some(seq),
            extra: serde_json::json!({"total": test_total}),
        })?;

        // Maintain the configured send rate.
        sleep(Duration::from_millis(interval)).await;
    }
    Ok(())
}

/// Runs the receiver role: continuously listens for incoming messages,
/// records statistics, and returns a summarized `Summary` after idle timeout.
///
/// # Parameters
/// * `transport` – Active gossip transport.
/// * `log` – JSONL writer for structured logs.
/// * `report_after_idle_ms` – Duration of inactivity after which to summarize.
pub async fn run_receiver<T: Transport>(
    mut transport: T,
    log: &mut crate::metrics::JsonWriter,
    report_after_idle_ms: u64,
) -> anyhow::Result<Summary> {
    let mut last = now_ms();
    let mut stats = Stats::default();
    let mut current_test: Option<[u8; 16]> = None;

    loop {
        select! {
            // Check periodically for idle timeout.
            biased;
            _ = sleep(Duration::from_millis(50)) => {},
            msg = transport.next() => {
                match msg {
                    // Message received successfully.
                    Some(Ok(b)) => {
                        last = now_ms();
                        if let Ok(m) = postcard::from_bytes::<DataMsg>(&b) {
                            if current_test.is_none() { current_test = Some(m.test_id); }
                            if Some(m.test_id) == current_test {
                                stats.record(&m);
                                log.write(&LogEvent {
                                    ts_ms: now_ms(),
                                    role: "receiver",
                                    peer_id: &transport.id(),
                                    event: "recv",
                                    seq: Some(m.seq),
                                    extra: serde_json::json!({}),
                                })?;
                            }
                        }
                    }

                    // Lagged event reported by gossip.
                    Some(Err(e)) => {
                        if e.to_string().contains("lagged") {
                            stats.lagged_events += 1;
                            log.write(&LogEvent {
                                ts_ms: now_ms(),
                                role: "receiver",
                                peer_id: &transport.id(),
                                event: "lagged",
                                seq: None,
                                extra: serde_json::json!({}),
                            })?;
                        }
                    }

                    // No more messages -> channel closed.
                    None => break,
                }
            }
        }

        // If idle for a given duration and message were received, stop.
        if now_ms().saturating_sub(last) > report_after_idle_ms && stats.total_expected > 0 {
            break;
        }
    }

    // Return final metrics summary.
    Ok(stats.summarize())
}
