use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs::File, io::Write, path::Path};
use crate::metrics::LogEvent;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn topic_from_name(name: &str) -> [u8; 32] {
    *blake3::hash(name.as_bytes()).as_bytes()
}

pub fn pad_payload(mut v: Vec<u8>, target_size: usize) -> Vec<u8> {
    if v.len() < target_size {
        v.resize(target_size, 0);
    }
    v
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

