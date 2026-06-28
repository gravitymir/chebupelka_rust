//! Простейшая статистика в памяти: атомарные счётчики, потокобезопасно.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct Stats {
    requests: Arc<AtomicU64>,
    tokens: Arc<AtomicU64>,
    total_ms: Arc<AtomicU64>,
    start: Instant,
}

#[derive(Serialize)]
pub struct Snapshot {
    pub requests: u64,
    pub tokens: u64,
    pub avg_tok_per_s: f64,
    pub uptime_s: u64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(AtomicU64::new(0)),
            tokens: Arc::new(AtomicU64::new(0)),
            total_ms: Arc::new(AtomicU64::new(0)),
            start: Instant::now(),
        }
    }

    pub fn record(&self, tokens: u64, ms: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.tokens.fetch_add(tokens, Ordering::Relaxed);
        self.total_ms.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Snapshot {
        let tokens = self.tokens.load(Ordering::Relaxed);
        let total_ms = self.total_ms.load(Ordering::Relaxed);
        let avg = if total_ms == 0 {
            0.0
        } else {
            tokens as f64 / (total_ms as f64 / 1000.0)
        };
        Snapshot {
            requests: self.requests.load(Ordering::Relaxed),
            tokens,
            avg_tok_per_s: avg,
            uptime_s: self.start.elapsed().as_secs(),
        }
    }
}
