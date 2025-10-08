use crate::metrics::DataMsg;
use crate::metrics::LogEvent;
use crate::metrics::{Stats, Summary};
use crate::util::{now_ms, pad_payload, topic_from_name};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use iroh::{Endpoint, SecretKey, protocol::Router};
use iroh_gossip::{
    ALPN,
    api::{Event, GossipTopic},
    net::Gossip,
    proto::TopicId,
};
use postcard;
use rand::RngCore;
use std::str::FromStr;
use tokio::{
    net::UdpSocket,
    select,
    time::{Duration, sleep},
};
use tokio_stream::StreamExt;
use tracing::warn;

#[async_trait]
pub trait Transport: Send + Sync {
    fn id(&self) -> String;

    async fn broadcast(&self, bytes: Bytes) -> Result<()>;

    async fn next(&mut self) -> Option<Result<Bytes>>;

    fn note_lagged(&mut self) {}
}

pub struct UdpBaseline {
    me: String,
    socket: UdpSocket,
    peers: Vec<String>,
}

impl UdpBaseline {
    pub async fn bind(bind: &str, peers: Vec<String>) -> Result<Self> {
        let socket = UdpSocket::bind(bind).await?;
        for peer in &peers {
            let _ = peer.parse::<std::net::SocketAddr>()?;
        }
        Ok(Self {
            me: format!("udp:{bind}"),
            socket,
            peers,
        })
    }
}

#[async_trait]
impl Transport for UdpBaseline {
    fn id(&self) -> String {
        self.me.clone()
    }

    async fn broadcast(&self, bytes: Bytes) -> Result<()> {
        for peer in &self.peers {
            let addr = peer.parse::<std::net::SocketAddr>()?;
            let _ = self.socket.send_to(&bytes, addr).await?;
        }
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<Bytes>> {
        let mut buffer = vec![0u8; 2048];
        match self.socket.recv_from(&mut buffer).await {
            Ok((n, _from)) => Some(Ok(Bytes::copy_from_slice(&buffer[..n]))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub struct IrohGossip {
    id: String,
    rx: tokio_stream::wrappers::ReceiverStream<anyhow::Result<Bytes>>,
    tx: tokio::sync::mpsc::Sender<Bytes>,
}

impl IrohGossip {
    pub async fn connect(
        topic_hex: Option<String>,
        topic_name: Option<String>,
        secret_hex: Option<String>,
        bootstrap: Vec<String>,
    ) -> Result<Self> {
        let builder = if let Some(hex) = secret_hex {
            let bytes = hex::decode(hex)?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
            let secret_key = SecretKey::from_bytes(&arr);
            Endpoint::builder().secret_key(secret_key)
        } else {
            Endpoint::builder()
        };
        let endpoint = builder.bind().await?;
        let id = endpoint.node_id().to_string();

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let _router = Router::builder(endpoint.clone())
            .accept(ALPN, gossip.clone())
            .spawn();

        let topic = if let Some(h) = topic_hex {
            TopicId::from_str(&h)?
        } else if let Some(n) = topic_name {
            TopicId::from_bytes(topic_from_name(&n))
        } else {
            let mut rnd = [0u8; 32];
            rand::rng().fill_bytes(&mut rnd);
            TopicId::from_bytes(rnd)
        };

        let mut node_ids = vec![];
        for b in bootstrap {
            if let Ok(node) = b.parse() {
                node_ids.push(node);
            }
        }

        let mut topic_handle: GossipTopic = gossip.subscribe_and_join(topic, node_ids).await?;
        topic_handle.joined().await.ok();
        let (sender, mut receiver) = topic_handle.split();

        let (tx, mut mpsc_rx) = tokio::sync::mpsc::channel::<Bytes>(128);
        let (mpsc_tx2, mpsc_rx2) = tokio::sync::mpsc::channel::<anyhow::Result<Bytes>>(1024);

        tokio::spawn(async move {
            while let Some(broadcast) = mpsc_rx.recv().await {
                if let Err(e) = sender.broadcast(broadcast).await {
                    warn!("gossip broadcast error: {e:?}");
                }
            }
        });

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
    fn note_lagged(&mut self) {}
}

pub async fn run_sender<T: Transport>(
    transport: T,
    transport_name: &str,
    log: &mut crate::metrics::JsonWriter,
    test_total: u64,
    rate_per_sec: u64,
    payload_size: usize,
) -> anyhow::Result<()> {
    let mut test_id = [0u8; 16];
    rand::rng().fill_bytes(&mut test_id);
    let interval = std::cmp::max(1, (1000 / rate_per_sec.max(1)) as i64) as u64;

    for seq in 0..test_total {
        let msg = DataMsg {
            test_id,
            seq,
            sent_ms: now_ms(),
            total: test_total,
            pad: vec![],
        };
        let mut bytes = postcard::to_allocvec(&msg)?;
        bytes = pad_payload(bytes, payload_size);
        transport.broadcast(Bytes::from(bytes)).await?;

        log.write(&LogEvent {
            ts_ms: now_ms(),
            role: "sender",
            transport: transport_name,
            peer_id: &transport.id(),
            event: "send",
            seq: Some(seq),
            extra: serde_json::json!({"total": test_total}),
        })?;

        sleep(Duration::from_millis(interval)).await;
    }
    Ok(())
}

pub async fn run_receiver<T: Transport>(
    mut transport: T,
    transport_name: &str,
    log: &mut crate::metrics::JsonWriter,
    report_after_idle_ms: u64,
) -> anyhow::Result<Summary> {
    let mut last = now_ms();
    let mut stats = Stats::default();
    let mut current_test: Option<[u8; 16]> = None;

    loop {
        select! {
            biased;
            _ = sleep(Duration::from_millis(50)) => {},
            msg = transport.next() => {
                match msg {
                    Some(Ok(b)) => {
                        last = now_ms();
                        if let Ok(m) = postcard::from_bytes::<DataMsg>(&b) {
                            if current_test.is_none() { current_test = Some(m.test_id); }
                            if Some(m.test_id) == current_test {
                                stats.record(&m);
                                log.write(&LogEvent {
                                    ts_ms: now_ms(),
                                    role: "receiver",
                                    transport: transport_name,
                                    peer_id: &transport.id(),
                                    event: "recv",
                                    seq: Some(m.seq),
                                    extra: serde_json::json!({}),
                                })?;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        if e.to_string().contains("lagged") {
                            stats.lagged_events += 1;
                            log.write(&LogEvent {
                                ts_ms: now_ms(),
                                role: "receiver",
                                transport: transport_name,
                                peer_id: &transport.id(),
                                event: "lagged",
                                seq: None,
                                extra: serde_json::json!({}),
                            })?;
                        }
                    }
                    None => break,
                }
            }
        }
        if now_ms().saturating_sub(last) > report_after_idle_ms && stats.total_expected > 0 {
            break;
        }
    }
    Ok(stats.summarize())
}
