use crate::util::now_ms;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs::File, io::Write, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMsg {
    pub test_id: [u8; 16],
    pub seq: u64,
    pub sent_ms: u64,
    pub total: u64,
    pub pad: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent <'a> {
    pub ts_ms: u64,
    pub role: &'a str,
    pub transport: &'a str,
    pub peer_id: &'a str,
    pub event: &'a str,
    pub seq: Option<u64>,
    pub extra: serde_json::Value,
}

pub struct JsonWriter {
    file: File,
}

impl JsonWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        Ok(Self {
            file: File::create(path)?,
        })
    }

    pub fn write(&mut self, ev: &LogEvent) -> anyhow::Result<()> {
        let line = serde_json::to_string(ev)?;
        writeln!(self.file, "{}", line)?;
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct Stats {
    seen: HashSet<u64>,
    max_seq_seen: i64,
    pub duplicates: u64,
    pub out_of_order: u64,
    pub lagged_events: u64,
    lats: Vec<u64>,
    pub total_expected: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub received_unique: u64,
    pub total_expected: u64,
    pub delivery_rate: f64,
    pub duplicates: u64,
    pub out_of_order: u64,
    pub lagged_events: u64,
    pub lat_min: Option<u64>,
    pub lat_p50: Option<u64>,
    pub lat_p90: Option<u64>,
    pub lat_p99: Option<u64>,
    pub lat_max: Option<u64>,
}

impl Stats {
    pub fn record(&mut self, message: &DataMsg) {
        self.total_expected = self.total_expected.max(message.total);
        if !self.seen.insert(message.seq) {
            self.duplicates += 1;
        }

        if (message.seq as i64) < self.max_seq_seen {
            self.out_of_order += 1;
        } else {
            self.max_seq_seen = message.seq as i64;
        }

        let lat = now_ms().saturating_sub(message.sent_ms);
        self.lats.push(lat);
    }

    fn quantil(sorted: &[u64], quantil: f64) -> Option<u64> {
        if sorted.is_empty() {
            return None;
        }

        let idx = ((sorted.len() - 1) as f64 * quantil).round() as usize;
        sorted.get(idx).copied()
    }

    pub fn summarize(&mut self) -> Summary {
        self.lats.sort_unstable();
        let received_unique = self.seen.len() as u64;
        let total_expected = self.total_expected.max(received_unique);
        let delivery = if total_expected == 0 {
            0.0
        } else {
            received_unique as f64 / total_expected as f64
        };

        Summary {
            received_unique,
            total_expected,
            delivery_rate: delivery,
            duplicates: self.duplicates,
            out_of_order: self.out_of_order,
            lagged_events: self.lagged_events,
            lat_min: self.lats.first().copied(),
            lat_p50: Self::quantil(&self.lats, 0.50),
            lat_p90: Self::quantil(&self.lats, 0.90),
            lat_p99: Self::quantil(&self.lats, 0.99),
            lat_max: self.lats.last().copied(),
        }
    }
}
