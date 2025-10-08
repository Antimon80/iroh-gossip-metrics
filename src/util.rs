use std::time::{SystemTime, UNIX_EPOCH};

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
