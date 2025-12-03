use crate::util::now_ms;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
    /// Optional end-to-end latency in milliseconds (for recv events).
    pub lat_ms: Option<u64>,
    /// Optional last-delivery-hop (overlay hop count) at the receiver.
    pub ldh: Option<u16>,
    /// Additional structured metadata.
    pub extra: serde_json::Value,
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

    // LDH (Last Delivery Hop)
    ldhs: Vec<u64>,

    // expected total messages
    pub total_expected: u64,

    // peer reachability (PR)
    pr_last_ts: Option<u64>,
    pr_last_ratio: f64,
    pr_acc_ms: f64,
    pr_total_ms: f64,

    // count neighbours in active view
    neighbour_down: u64,
    neighbour_up: u64,

    // connectivity-level (active neighbors)
    conn_last_connected: u64,
    conn_acc_ms: f64,
    conn_total_ms: f64,

    // downtime-periods (connected_peers == 0)
    downtime_started_at: Option<u64>,
    downtime_periods: u64,
    downtime_duration_ms: Vec<u64>,
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

    // LDH (overlay hop counts)
    pub ldh_min: Option<u64>,
    pub ldh_p50: Option<u64>,
    pub ldh_p90: Option<u64>,
    pub ldh_p99: Option<u64>,
    pub ldh_max: Option<u64>,

    // peer reachability
    pub pr_avg_ratio: Option<f64>,

    // active neighbours
    pub avg_connected_peers: Option<f64>,

    // periods with zero active neighbours
    pub downtime_total_ms: u64,
    pub downtime_periods: u64,
    pub downtime_p50_ms: Option<u64>,
    pub downtime_p90_ms: Option<u64>,
    pub downtime_max_ms: Option<u64>,

    // neighbour in active view counts
    pub neighbour_down: u64,
    pub neighbour_up: u64,

    // startup/termination flags
    pub joined: bool,
    pub join_wait_ms: u64,
    pub saw_test: bool,
    pub timed_out_no_data: bool,
}

impl Stats {
    /// Record a successfully decoded DataMsg and update all relevant metrics.
    ///
    /// `ldh` is the last-delivery-hop value (if known),
    /// `recv_ts_ms` is the local receive timestamp in ms.
    pub fn record(&mut self, message: &DataMsg, ldh: Option<u16>, recv_ts_ms: u64) {
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
        let lat = recv_ts_ms.saturating_sub(message.sent_ms);
        self.lats.push(lat);

        // LDH sample (if known).
        if let Some(h) = ldh {
            self.ldhs.push(h as u64);
        }
    }

    /// Note a lagged transport event (buffer overrun / skipped events).
    pub fn note_lagged(&mut self) {
        self.lagged_events += 1;
    }

    // Note a neighbour is removed from the active view set
    pub fn note_neighbour_down(&mut self) {
        self.neighbour_down +=1;
    }

    // Note a neighbour is added to the active view set
    pub fn note_neighbour_up(&mut self) {
        self.neighbour_up += 1;
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

        if let Some(prev_ts) = self.pr_last_ts {
            let dur = ts_ms.saturating_sub(prev_ts) as f64;

            // Accumulate time-weighted ratio since last update.
            self.pr_acc_ms += dur * self.pr_last_ratio;
            self.pr_total_ms += dur;

            // Accumulate time-weighted mean of connected_peers
            self.conn_acc_ms += dur * (self.conn_last_connected as f64);
            self.conn_total_ms += dur;
        }

        // detect downtime periods
        if self.conn_last_connected > 0 && connected == 0 {
            self.downtime_started_at = Some(ts_ms);
            self.downtime_periods += 1;
        }

        // end of downtime period
        if self.conn_last_connected == 0 && connected > 0 {
            if let Some(start) = self.downtime_started_at.take() {
                let dur = ts_ms.saturating_sub(start);
                self.downtime_duration_ms.push(dur);
            }
        }

        self.pr_last_ts = Some(ts_ms);
        self.pr_last_ratio = ratio;
        self.conn_last_connected = connected;
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
    /// This sorts latency samples, finalizes reachability and connectivity
    /// averaging, and computes all derived rates.
    pub fn summarize(&mut self) -> Summary {
        // latencies
        self.lats.sort_unstable();
        // LDH samples
        self.ldhs.sort_unstable();

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

            // PR
            self.pr_acc_ms += dur * self.pr_last_ratio;
            self.pr_total_ms += dur;

            // av connected_peers
            self.conn_acc_ms += dur * (self.conn_last_connected as f64);
            self.conn_total_ms += dur;

            if let Some(start) = self.downtime_started_at.take() {
                let dur_ms = now.saturating_sub(start);
                self.downtime_duration_ms.push(dur_ms);
            }

            self.pr_last_ts = Some(now);
        }

        let pr_avg = if self.pr_total_ms > 0.0 {
            Some(self.pr_acc_ms / self.pr_total_ms)
        } else {
            None
        };

        let avg_connected_peers = if self.conn_total_ms > 0.0 {
            Some(self.conn_acc_ms / self.conn_total_ms)
        } else {
            None
        };

        // downtime stats
        let mut downtime_sorted = self.downtime_duration_ms.clone();
        downtime_sorted.sort_unstable();
        let downtime_total_ms: u64 = downtime_sorted.iter().copied().sum();
        let downtime_p50 = Self::quantil(&downtime_sorted, 0.50);
        let downtime_p90 = Self::quantil(&downtime_sorted, 0.90);
        let downtime_max = downtime_sorted.last().copied();

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

            // LDH
            ldh_min: self.ldhs.first().copied(),
            ldh_p50: Self::quantil(&self.ldhs, 0.50),
            ldh_p90: Self::quantil(&self.ldhs, 0.90),
            ldh_p99: Self::quantil(&self.ldhs, 0.99),
            ldh_max: self.ldhs.last().copied(),

            // PR
            pr_avg_ratio: pr_avg,

            // count neighbours in active view
            neighbour_down: self.neighbour_down,
            neighbour_up: self.neighbour_up,

            // connectivity
            avg_connected_peers,
            downtime_total_ms,
            downtime_periods: self.downtime_periods,
            downtime_p50_ms: downtime_p50,
            downtime_p90_ms: downtime_p90,
            downtime_max_ms: downtime_max,

            // startup/termination flags (defaults)
            joined: false,
            join_wait_ms: 0,
            saw_test: false,
            timed_out_no_data: false,
        }
    }
}
