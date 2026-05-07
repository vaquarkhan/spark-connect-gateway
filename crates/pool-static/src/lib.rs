//! Backend pool whose membership is fixed at startup. Phase 1 only —
//! Phase 2 introduces dynamic K8s service-watch and Consul-backed pools.

use scg_routing::Pool;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, thiserror::Error)]
pub enum StaticPoolError {
    #[error("static pool: at least one backend address required")]
    Empty,
}

pub struct StaticPool {
    backends: Vec<String>,
    cursor: AtomicU64,
}

impl StaticPool {
    pub fn new<I, S>(addresses: I) -> Result<Self, StaticPoolError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let backends: Vec<String> = addresses.into_iter().map(Into::into).collect();
        if backends.is_empty() {
            return Err(StaticPoolError::Empty);
        }
        Ok(Self {
            backends,
            cursor: AtomicU64::new(0),
        })
    }

    pub fn all(&self) -> Vec<String> {
        self.backends.clone()
    }
}

impl Pool for StaticPool {
    fn pick(&self) -> String {
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed);
        let n = self.backends.len() as u64;
        self.backends[(idx % n) as usize].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin() {
        let p = StaticPool::new(["a", "b", "c"]).unwrap();
        let got: Vec<_> = (0..4).map(|_| p.pick()).collect();
        assert_eq!(got, vec!["a", "b", "c", "a"]);
    }

    #[test]
    fn empty_rejected() {
        assert!(StaticPool::new(Vec::<String>::new()).is_err());
    }
}
