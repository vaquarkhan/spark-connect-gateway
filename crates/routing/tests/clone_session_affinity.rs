use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use scg_routing::{AffinityStore, Pool, Router, SessionKey};
use scg_store_memory::MemoryStore;

// Round-robin over ["A","B"], same shape as StaticPool.
struct TwoBackends {
    n: AtomicU64,
}
impl Pool for TwoBackends {
    fn pick(&self) -> Option<String> {
        let i = self.n.fetch_add(1, Ordering::SeqCst);
        Some(["A", "B"][(i % 2) as usize].to_string())
    }
    fn all_healthy(&self) -> Vec<String> {
        vec!["A".into(), "B".into()]
    }
}

fn router() -> Router {
    Router::single_pool(
        Arc::new(TwoBackends {
            n: AtomicU64::new(0),
        }),
        Arc::new(MemoryStore::new()) as Arc<dyn AffinityStore>,
    )
}

// This test encodes the CloneSession flow as the handler performs it.
// It FAILS on current main (no remember_session) and PASSES after the fix.
#[tokio::test]
async fn cloned_session_stays_on_parent_backend() {
    let r = router();

    // 1) Parent session pins to a backend on its first RPC.
    let parent = SessionKey::with_tenant("t", "alice", "orig");
    let parent_addr = r.resolve_session(&parent).await.unwrap().unwrap();

    // 2) CloneSession: handler resolves the parent again (hit), forwards,
    //    and (with the fix) pins the new session id to the same backend.
    let clone_backend = r.resolve_session(&parent).await.unwrap().unwrap();
    assert_eq!(clone_backend, parent_addr);
    let new_sid = "cloned";
    r.remember_session(
        SessionKey::with_tenant("t", "alice", new_sid),
        clone_backend.clone(),
    )
    .await;

    // 3) Follow-up RPC on the cloned session must reach the same backend.
    let followup = r
        .resolve_session(&SessionKey::with_tenant("t", "alice", new_sid))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        followup, clone_backend,
        "cloned session must stay on the parent's backend"
    );
}
