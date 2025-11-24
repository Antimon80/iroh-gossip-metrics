use crate::util::now_ms;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs::File, io::Write, path::Path};

/// Application-level payload sent during benchmarks.
///
/// The message carries:
/// - a test run identifier (`test_id`) so concurrent runs can be separated,
/// - a monotonically increasing sequence number (`seq`),
/// - the sender timestamp (`sent_ms`) for end-to-end latency,
/// - the expected total number of messages in this test (`total`),
/// - optional padding (`pad`) to reach a fixed payload size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMsg {
    /// Random per-test identifier (shared by all messages of one run).
    pub test_id: [u8; 16],
    /// Sequence number within the test, starting at 0.
    pub seq: u64,
    /// Sender-side timestamp in milliseconds since epoch.
    pub sent_ms: u64,
    /// Total messages expected in this test run.
    pub total: u64,
    /// Padding bytes (used to normalize payload size).
    pub pad: Vec<u8>,
}

// Transport-level events as seen by the benchmark harness.
///
/// These events are produced by a backend transport (e.g., gossip)
/// and consumed by the receiver loop to update metrics.
#[derive(Debug, Clone)]
pub enum TransportEvent {
    /// A data message was received.
    Msg(Bytes),
    /// The transport reported that it lagged behind (buffer overrun / dropped events).
    Lagged,
    /// A neighbor/peer disconnected.
    Disconnect,
    /// A neighbor/peer reconnected or became reachable again.
    Reconnect,
}

/// One structured log line written as JSONL.
///
/// Lifetimes are used so we can reference static role/event strings
/// without allocating new `String`s for every log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent<'a> {
    /// Timestamp of the log event (ms since epoch).
    pub ts_ms: u64,
    /// Role of the logging process ("sender" or "receiver").
    pub role: &'a str,
    /// Identifier of the local peer/transport instance.
    pub peer_id: &'a str,
    /// Event type, e.g. "send", "recv", "neighbor_up".
    pub event: &'a str,
    /// Optional sequence number (present for send/recv events).
    pub seq: Option<u64>,
    /// Additional structured metadata.
    pub extra: serde_json::Value,
}

/// Simple JSONL writer for benchmark logs.
///
/// Each call to `write` appends a single JSON object as one line.
pub struct JsonWriter {
    file: File,
}

impl JsonWriter {
    /// Create a new JSONL writer that truncates/creates the given file path.
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        Ok(Self {
            file: File::create(path)?,
        })
    }

    /// Append a single event as one JSON line.
    pub fn write(&mut self, ev: &LogEvent) -> anyhow::Result<()> {
        let line = serde_json::to_string(ev)?;
        writeln!(self.file, "{}", line)?;
        Ok(())
    }
}

/// Accumulates per-run receiver statistics.
///
/// This struct is intentionally stateful and updated incrementally
/// as messages and transport events arrive.
#[derive(Default, Clone)]
pub struct Stats {
    // delivery/duplicates/order
    seen: HashSet<u64>,
    max_seq_seen: i64,
    recv_total: u64,
    pub duplicates: u64,
    pub out_of_order: u64,

    // lag/end-to-end-delay (E2E)
    pub lagged_events: u64,
    lats: Vec<u64>,

    // expected total messages
    pub total_expected: u64,

    // convergence time (CT)
    first_sent_ms: Option<u64>,
    last_recv_ts_ms: Option<u64>,
    convergence_time_ms: Option<u64>,

    // peer reachability (PR)
    pr_last_ts: Option<u64>,
    pr_last_ratio: f64,
    pr_acc_ms: f64,
    pr_total_ms: f64,

    // reconnect time (RT)
    last_disconnect_ts: Option<u64>,
    waiting_first_after_reconnect: bool,
    reconnect_time_ms: Vec<u64>,
}

/// Final summarized metrics for one receiver run.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    // delivery
    pub received_unique: u64,
    pub recv_total: u64,
    pub total_expected: u64,
    pub delivery_rate: f64,

    // duplicates
    pub duplicate_rate: f64,
    pub duplicates: u64,
    pub out_of_order: u64,

    // lag/E2E
    pub lagged_events: u64,
    pub lat_min: Option<u64>,
    pub lat_p50: Option<u64>,
    pub lat_p90: Option<u64>,
    pub lat_p99: Option<u64>,
    pub lat_max: Option<u64>,

    // convergence time
    pub convergence_time_ms: Option<u64>,

    // peer reachability
    pub pr_avg_ratio: Option<f64>,

    // reconnect times
    pub rt_avg_ms: Option<u64>,
    pub rt_p50_ms: Option<u64>,
    pub rt_p90_ms: Option<u64>,
    pub rt_max_ms: Option<u64>,

    // startup/termination flags
    pub joined: bool,
    pub join_wait_ms: u64,
    pub saw_test: bool,
    pub timed_out_no_data: bool,
}

impl Stats {
    /// Record a successfully decoded DataMsg and update all relevant metrics.
    pub fn record(&mut self, message: &DataMsg) {
        // Track expected total for this test (monotonic max in case of reordering).
        self.total_expected = self.total_expected.max(message.total);
        // Count every received message, including duplicates.
        self.recv_total += 1;

        // Duplicate detection by sequence number.
        if !self.seen.insert(message.seq) {
            self.duplicates += 1;
        }

        // Out-of-order detection relative to maximum observed sequence.
        if (message.seq as i64) < self.max_seq_seen {
            self.out_of_order += 1;
        } else {
            self.max_seq_seen = message.seq as i64;
        }

        // End-to-end latency based on sender timestamp.
        let now = now_ms();
        let lat = now.saturating_sub(message.sent_ms);
        self.lats.push(lat);

        // Track first send time and last receive time for convergence.
        self.first_sent_ms = Some(
            self.first_sent_ms
                .map_or(message.sent_ms, |m| m.min(message.sent_ms)),
        );
        self.last_recv_ts_ms = Some(now);

        // If we have seen all expected messages, compute convergence time once.
        if self.convergence_time_ms.is_none()
            && self.total_expected > 0
            && self.seen.len() as u64 >= self.total_expected
        {
            if let (Some(first_sent), Some(last_recv)) = (self.first_sent_ms, self.last_recv_ts_ms)
            {
                self.convergence_time_ms = Some(last_recv.saturating_sub(first_sent));
            }
        }

        // If a reconnect happened and this is the first post-reconnect message,
        // measure time since disconnect.
        if self.waiting_first_after_reconnect {
            if let Some(disc) = self.last_disconnect_ts {
                let rt = now.saturating_sub(disc);
                self.reconnect_time_ms.push(rt);
            }
            self.waiting_first_after_reconnect = false;
        }
    }

    /// Note a lagged transport event (buffer overrun / skipped events).
    pub fn note_lagged(&mut self) {
        self.lagged_events += 1;
    }

    /// Record a new snapshot of peer connectivity and reachability.
    ///
    /// The ratio is reachable/connected.
    /// This method maintains a time-weighted average over the run.
    pub fn record_peer_view(&mut self, ts_ms: u64, connected: u64, reachable: u64) {
        let ratio = if connected == 0 {
            1.0
        } else {
            (reachable as f64) / (connected as f64)
        };

        // Accumulate time-weighted ratio since last update.
        if let Some(prev_ts) = self.pr_last_ts {
            let dur = ts_ms.saturating_sub(prev_ts) as f64;
            self.pr_acc_ms += dur * self.pr_last_ratio;
            self.pr_total_ms += dur;
        }
        self.pr_last_ts = Some(ts_ms);
        self.pr_last_ratio = ratio;
    }

    /// Note that a disconnect happened at the given timestamp.
    ///
    /// This arms reconnect-time measurement to start from this time.
    pub fn note_disconnect(&mut self, ts_ms: u64) {
        self.last_disconnect_ts = Some(ts_ms);
        self.waiting_first_after_reconnect = false;
    }

    /// Note that a reconnect happened at the given timestamp.
    ///
    /// We only compute reconnect time once the first message arrives after reconnect,
    /// because that's when the system is effectively usable again.
    pub fn note_reconnect(&mut self) {
        if self.last_disconnect_ts.is_some() {
            self.waiting_first_after_reconnect = true;
        } else {
            self.waiting_first_after_reconnect = false;
        }
    }

    /// Return the quantile value from a sorted slice using nearest-rank rounding.
    fn quantil(sorted: &[u64], quantil: f64) -> Option<u64> {
        if sorted.is_empty() {
            return None;
        }

        let idx = ((sorted.len() - 1) as f64 * quantil).round() as usize;
        sorted.get(idx).copied()
    }

    /// Produce a Summary from the accumulated stats.
    ///
    /// This sorts latency and reconnect-time samples, finalizes reachability
    /// averaging, and computes all derived rates.
    pub fn summarize(&mut self) -> Summary {
        // latencies
        self.lats.sort_unstable();

        // delivery
        let received_unique = self.seen.len() as u64;
        let total_expected = self.total_expected.max(received_unique);
        let delivery = if total_expected == 0 {
            0.0
        } else {
            received_unique as f64 / total_expected as f64
        };

        // duplicate rate
        let dup_rate = if self.recv_total == 0 {
            0.0
        } else {
            self.duplicates as f64 / self.recv_total as f64
        };

        // Finalize peer reachability by accounting for time since last update.
        if let Some(prev_ts) = self.pr_last_ts {
            let now = now_ms();
            let dur = now.saturating_sub(prev_ts) as f64;
            self.pr_acc_ms += dur * self.pr_last_ratio;
            self.pr_total_ms += dur;
            self.pr_last_ts = Some(now);
        }

        let pr_avg = if self.pr_total_ms > 0.0 {
            Some(self.pr_acc_ms / self.pr_total_ms)
        } else {
            None
        };

        // reconnet times
        let mut rts = self.reconnect_time_ms.clone();
        rts.sort_unstable();

        Summary {
            // delivery
            received_unique,
            recv_total: self.recv_total,
            total_expected,
            delivery_rate: delivery,

            // duplicates/order
            duplicate_rate: dup_rate,
            duplicates: self.duplicates,
            out_of_order: self.out_of_order,

            // lag/E2E
            lagged_events: self.lagged_events,
            lat_min: self.lats.first().copied(),
            lat_p50: Self::quantil(&self.lats, 0.50),
            lat_p90: Self::quantil(&self.lats, 0.90),
            lat_p99: Self::quantil(&self.lats, 0.99),
            lat_max: self.lats.last().copied(),

            // CT
            convergence_time_ms: self.convergence_time_ms,

            // PR
            pr_avg_ratio: pr_avg,

            // RT
            rt_avg_ms: if rts.is_empty() {
                None
            } else {
                Some((rts.iter().sum::<u64>() / rts.len() as u64) as u64)
            },
            rt_p50_ms: Self::quantil(&rts, 0.50),
            rt_p90_ms: Self::quantil(&rts, 0.90),
            rt_max_ms: rts.last().copied(),

            // startup/termination flags (defaults)
            joined: false,
            join_wait_ms: 0,
            saw_test: false,
            timed_out_no_data: false,
        }
    }
}
