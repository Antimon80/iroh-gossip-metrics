use crate::metrics::TransportEvent;
use crate::metrics::{DataMsg, LogEvent, Stats, Summary};
use crate::util::{now_ms, pad_payload, topic_from_name};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use iroh::NodeId;
use iroh::{Endpoint, RelayMode, SecretKey, protocol::Router};
use iroh_gossip::proto::DeliveryScope;
use iroh_gossip::{ALPN, api::Event, net::Gossip, proto::TopicId};
use postcard;
use rand::RngCore;
use std::str::FromStr;
use tokio::{
    select,
    time::{Duration, sleep, timeout},
};
use tokio_stream::StreamExt;
use tracing::warn;

/// Defines the discovery mode for gossip peers.
#[derive(Debug, Clone, Copy)]
pub enum Discovery {
    /// Direct discovery (peers connect directly to each other, typically via local LAN discovery like mDNS).
    Direct,
    /// Relay-assisted discovery using one or more bootstrap peers
    /// This mode relies on iroh's n0 discovery and can traverse NATs via relays.
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
    async fn next(&mut self) -> Option<Result<TransportEvent>>;

    /// Whether this peer observed a confirmed gossip join.
    ///
    /// For degraded scenarios, join may time out. In that case
    /// transports should return `false` and the benchmark must
    /// still terminate and log this fact.
    fn joined(&self) -> bool {
        true
    }

    /// How long we waited for join confirmation (ms).
    fn join_wait_ms(&self) -> u64 {
        0
    }
}

/// Implementation of the gossip-based transport using the 'iroh-gossip' protocol.
pub struct IrohGossip {
    /// Local node ID (as string for logging)
    id: String,
    /// Stream for receiving messages
    rx: tokio_stream::wrappers::ReceiverStream<anyhow::Result<TransportEvent>>,
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
    /// join status for metrics/termination
    joined: bool,
    join_wait_ms: u64,
}

impl IrohGossip {
    /// Establishes an iroh-gossip connection for the given topic and discovery mode.
    ///
    /// This version is fully robust:
    /// - `subscribe_and_join` is wrapped in a timeout (prevents hanging forever)
    /// - If join fails or times out, a functional Transport is still returned
    /// - The receiver loop will then run with an empty RX-stream and terminate cleanly
    /// - This guarantees that test runs *always* finish and always produce logs.
    ///
    /// The `joined` flag and `join_wait_ms` reflect whether the join actually succeeded.
    pub async fn connect(
        topic_hex: Option<String>,
        topic_name: Option<String>,
        secret_hex: Option<String>,
        bootstrap: Vec<String>,
        discovery: Discovery,
    ) -> Result<Self> {
        // -------------------------------------------------------------
        // 1) Build endpoint (optional deterministic secret key)
        // -------------------------------------------------------------
        let mut builder = Endpoint::builder();

        if let Some(ref hex) = secret_hex {
            let bytes = hex::decode(hex)?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
            let secret_key = SecretKey::from_bytes(&arr);
            builder = builder.secret_key(secret_key);
        }

        // Select discovery mode
        builder = match discovery {
            Discovery::Direct => builder
                .discovery_local_network()
                .relay_mode(RelayMode::Disabled),
            Discovery::Relay => builder.discovery_n0().relay_mode(RelayMode::Default),
        };

        // Create endpoint
        let endpoint = builder.bind().await?;
        let id = endpoint.node_id().to_string();
        eprintln!("node_id={}", id);

        // Start gossip + router
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(ALPN, gossip.clone())
            .spawn();

        // -------------------------------------------------------------
        // 2) Determine topic ID
        // -------------------------------------------------------------
        let topic = if let Some(h) = topic_hex {
            TopicId::from_str(&h)?
        } else if let Some(n) = topic_name {
            TopicId::from_bytes(topic_from_name(&n))
        } else {
            let mut rnd = [0u8; 32];
            rand::rng().fill_bytes(&mut rnd);
            TopicId::from_bytes(rnd)
        };

        // Parse bootstrap NodeIDs
        let node_ids: Vec<NodeId> = bootstrap
            .into_iter()
            .filter_map(|b| b.parse::<NodeId>().ok())
            .collect();

        if !node_ids.is_empty() {
            eprintln!("bootstraps_parsed={}", node_ids.len());
        }

        // -------------------------------------------------------------
        // 3) subscribe_and_join MUST NOT HANG → wrap in timeout
        // -------------------------------------------------------------
        let join_timeout = Duration::from_secs(30);

        let join_start = now_ms();

        let topic_handle_result =
            timeout(join_timeout, gossip.subscribe_and_join(topic, node_ids)).await;

        // Outgoing and incoming channels (always created so caller can run)
        let (tx, mut tx_rx) = tokio::sync::mpsc::channel::<Bytes>(128);
        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<anyhow::Result<TransportEvent>>(1024);

        // Values to return
        let joined: bool;
        let join_wait_ms: u64;

        match topic_handle_result {
            // Case A: subscribe_and_join completed (success or error)
            Ok(Ok(mut topic_handle)) => {
                // Now wait on topic_handle.joined() but also time-limited
                joined = match timeout(join_timeout, topic_handle.joined()).await {
                    Ok(Ok(())) => {
                        eprintln!("joined=1");
                        true
                    }
                    _ => {
                        eprintln!("joined=0 timeout_ms={}", join_timeout.as_millis());
                        false
                    }
                };
                join_wait_ms = now_ms().saturating_sub(join_start);

                let (sender, mut receiver) = topic_handle.split();

                // Spawn outgoing broadcast task
                let ev_tx_out = ev_tx.clone();
                tokio::spawn(async move {
                    while let Some(msg) = tx_rx.recv().await {
                        if let Err(e) = sender.broadcast(msg).await {
                            warn!("broadcast error: {e:?}");
                            let _ = ev_tx_out.send(Err(e.into())).await;
                        }
                    }
                });

                // Spawn incoming event task
                tokio::spawn(async move {
                    while let Some(item) = receiver.next().await {
                        match item {
                            Ok(Event::Received(m)) => {
                                // Extract LDH from DeliveryScope without importing Round.
                                let ldh = match m.scope {
                                    DeliveryScope::Swarm(round) => {
                                        // Use serde_json to convert Round -> u16
                                        let v = serde_json::to_value(&round).unwrap();
                                        Some(v.as_u64().unwrap_or(0) as u16)
                                    }
                                    DeliveryScope::Neighbors => None,
                                };

                                let _ = ev_tx
                                    .send(Ok(TransportEvent::Msg {
                                        bytes: Bytes::from(m.content),
                                        ldh,
                                    }))
                                    .await;
                            }

                            Ok(Event::Lagged) => {
                                let _ = ev_tx.send(Ok(TransportEvent::Lagged)).await;
                            }
                            Ok(Event::NeighborUp(_)) => {
                                let _ = ev_tx.send(Ok(TransportEvent::Reconnect)).await;
                            }
                            Ok(Event::NeighborDown(_)) => {
                                let _ = ev_tx.send(Ok(TransportEvent::Disconnect)).await;
                            }
                            Err(e) => {
                                let _ = ev_tx.send(Err(e.into())).await;
                            }
                        }
                    }
                });
            }

            // Case B: subscribe_and_join returned an error immediately
            Ok(Err(e)) => {
                eprintln!("joined=0 subscribe_error={e:?}");
                joined = false;
                join_wait_ms = now_ms().saturating_sub(join_start);

                // Closing sender terminates RX immediately
                drop(ev_tx);
            }

            // Case C: subscribe_and_join timed out entirely
            Err(_) => {
                eprintln!("joined=0 subscribe_timeout_ms={}", join_timeout.as_millis());
                joined = false;
                join_wait_ms = now_ms().saturating_sub(join_start);

                drop(ev_tx);
            }
        }

        // -------------------------------------------------------------
        // 4) Always return a fully usable Transport object
        // -------------------------------------------------------------
        Ok(Self {
            id,
            rx: tokio_stream::wrappers::ReceiverStream::new(ev_rx),
            tx,
            _endpoint: endpoint,
            _router: router,
            _gossip: gossip,
            _discovery: discovery,
            joined,
            join_wait_ms,
        })
    }
}

#[async_trait]
impl Transport for IrohGossip {
    fn id(&self) -> String {
        format!("gossip:{}", self.id)
    }

    fn joined(&self) -> bool {
        self.joined
    }

    fn join_wait_ms(&self) -> u64 {
        self.join_wait_ms
    }

    async fn broadcast(&self, bytes: Bytes) -> Result<()> {
        self.tx.send(bytes).await?;
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<TransportEvent>> {
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
            lat_ms: None,
            ldh: None,
            extra: serde_json::json!({"total": test_total}),
        })?;

        // Maintain the configured send rate.
        sleep(Duration::from_millis(interval)).await;
    }
    Ok(())
}

/// Runs the receiver role: continuously listens for incoming messages,
/// records statistics, and returns a summarized `Summary`.
///
/// Termination is two-stage:
/// 1) If we have seen at least one valid test message (total_expected > 0),
///    we stop after `report_after_idle_ms` without further valid test messages.
/// 2) If we have NOT seen any valid test message at all,
///    we still stop after `report_after_idle_ms` since start.
///    This prevents "forever receivers" under high loss/partition.
///
/// # Parameters
/// * `transport` – Active gossip transport.
/// * `log` – JSONL writer for structured logs.
/// * `report_after_idle_ms` – Idle timeout (ms).
pub async fn run_receiver<T: Transport>(
    mut transport: T,
    log: &mut crate::metrics::JsonWriter,
    report_after_idle_ms: u64,
) -> anyhow::Result<Summary> {
    let start_ms = now_ms();

    // Last time we saw a valid message for the active test.
    let mut last_valid_ms = start_ms;

    let mut stats = Stats::default();
    let mut current_test: Option<[u8; 16]> = None;
    let mut connected_peers: u64 = 0;

    // Write log event if receiver couldn't join topic
    if !transport.joined() {
        log.write(&LogEvent {
            ts_ms: now_ms(),
            role: "receiver",
            peer_id: &transport.id(),
            event: "no_join",
            seq: None,
            lat_ms: None,
            ldh: None,
            extra: serde_json::json!({
                "join_wait_ms": transport.join_wait_ms(),
            }),
        })?;
    }

    loop {
        select! {
            biased;
            _ = sleep(Duration::from_millis(50)) => {},

            event = transport.next() => {
                match event {
                    Some(Ok(TransportEvent::Msg { bytes, ldh })) => {
                        let recv_ts = now_ms();

                        if let Ok(m) = postcard::from_bytes::<DataMsg>(&bytes) {

                            // First valid DataMsg defines the active test.
                            if current_test.is_none() {
                                current_test = Some(m.test_id);
                            }

                            // Only record messages for the active test.
                            if Some(m.test_id) == current_test {
                                last_valid_ms = recv_ts;
                                stats.record(&m, ldh, recv_ts);

                                let lat_ms = recv_ts.saturating_sub(m.sent_ms);

                                log.write(&LogEvent {
                                    ts_ms: recv_ts,
                                    role: "receiver",
                                    peer_id: &transport.id(),
                                    event: "recv",
                                    seq: Some(m.seq),
                                    lat_ms: Some(lat_ms),
                                    ldh,
                                    extra: serde_json::json!({}),
                                })?;
                            }
                        }
                    }

                    Some(Ok(TransportEvent::Lagged)) => {
                        stats.note_lagged();
                        log.write(&LogEvent {
                            ts_ms: now_ms(),
                            role: "receiver",
                            peer_id: &transport.id(),
                            event: "lagged",
                            seq: None,
                            lat_ms: None,
                            ldh: None,
                            extra: serde_json::json!({}),
                        })?;
                    }

                    Some(Ok(TransportEvent::Disconnect)) => {
                        let ts = now_ms();
                        if connected_peers > 0 {
                            connected_peers -= 1;
                        }
                        stats.record_peer_view(ts, connected_peers, connected_peers);
                        stats.note_disconnect(ts);

                        log.write(&LogEvent {
                            ts_ms: ts,
                            role: "receiver",
                            peer_id: &transport.id(),
                            event: "neighbor_down",
                            seq: None,
                            lat_ms: None,
                            ldh: None,
                            extra: serde_json::json!({
                                "connected": connected_peers,
                                "reachable": connected_peers
                            }),
                        })?;
                    }

                    Some(Ok(TransportEvent::Reconnect)) => {
                        let ts = now_ms();
                        connected_peers += 1;
                        stats.record_peer_view(ts, connected_peers, connected_peers);
                        stats.note_reconnect();

                        log.write(&LogEvent {
                            ts_ms: ts,
                            role: "receiver",
                            peer_id: &transport.id(),
                            event: "neighbor_up",
                            seq: None,
                            lat_ms: None,
                            ldh: None,
                            extra: serde_json::json!({
                                "connected": connected_peers,
                                "reachable": connected_peers
                            }),
                        })?;
                    }

                    Some(Err(e)) => {
                        warn!("transport error: {e:?}");
                    }

                    None => break,
                }
            }
        }

        let now = now_ms();

        // Case 1: test seen -> idle based on valid test data.
        if stats.total_expected > 0 && now.saturating_sub(last_valid_ms) > report_after_idle_ms {
            break;
        }

        // Case 2: no test seen at all -> wall-clock timeout since start.
        if stats.total_expected == 0 && now.saturating_sub(start_ms) > report_after_idle_ms {
            break;
        }
    }

    let mut summary = stats.summarize();

    summary.joined = transport.joined();
    summary.join_wait_ms = transport.join_wait_ms();
    summary.saw_test = summary.total_expected > 0;
    summary.timed_out_no_data = !summary.saw_test;

    Ok(summary)
}
