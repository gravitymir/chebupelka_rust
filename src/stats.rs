//! Простейшая статистика в памяти: атомарные счётчики + кольцо последних
//! скоростей (для графика). Потокобезопасно.

use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const RECENT_CAP: usize = 60; // сколько последних запросов держим для графика

#[derive(Clone)]
pub struct Stats {
    requests: Arc<AtomicU64>,
    tokens: Arc<AtomicU64>,
    total_ms: Arc<AtomicU64>,
    /// tok/s последних запросов — для графика скорости
    recent: Arc<Mutex<VecDeque<f64>>>,
    start: Instant,
}

#[derive(Serialize)]
pub struct Snapshot {
    pub requests: u64,
    pub tokens: u64,
    pub avg_tok_per_s: f64,
    pub uptime_s: u64,
    /// скорости последних запросов (tok/s), от старых к новым
    pub recent: Vec<f64>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(AtomicU64::new(0)),
            tokens: Arc::new(AtomicU64::new(0)),
            total_ms: Arc::new(AtomicU64::new(0)),
            recent: Arc::new(Mutex::new(VecDeque::with_capacity(RECENT_CAP))),
            start: Instant::now(),
        }
    }

    pub fn record(&self, tokens: u64, ms: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.tokens.fetch_add(tokens, Ordering::Relaxed);
        self.total_ms.fetch_add(ms, Ordering::Relaxed);

        let tps = if ms == 0 {
            0.0
        } else {
            tokens as f64 / (ms as f64 / 1000.0)
        };
        let mut r = self.recent.lock().unwrap();
        if r.len() == RECENT_CAP {
            r.pop_front();
        }
        r.push_back(tps);
    }

    pub fn snapshot(&self) -> Snapshot {
        let tokens = self.tokens.load(Ordering::Relaxed);
        let total_ms = self.total_ms.load(Ordering::Relaxed);
        let avg = if total_ms == 0 {
            0.0
        } else {
            tokens as f64 / (total_ms as f64 / 1000.0)
        };
        let recent: Vec<f64> = self.recent.lock().unwrap().iter().copied().collect();
        Snapshot {
            requests: self.requests.load(Ordering::Relaxed),
            tokens,
            avg_tok_per_s: avg,
            uptime_s: self.start.elapsed().as_secs(),
            recent,
        }
    }
}
