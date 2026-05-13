//! Integration test for structured audit logging.
//!
//! Drives RPCs through a real gRPC server with an enabled
//! [`AuditLogger`] and asserts the four default event types fire
//! end-to-end:
//!
//! * `session.create` — first RPC on a fresh `(tenant, user, session)`
//! * `session.release` — `ReleaseSession` succeeds
//! * `auth.failure` — auth interceptor rejects a missing token
//! * `rpc.error` — handler returns a non-OK Status
//!
//! Events are captured via a `tracing_subscriber::Layer` that filters
//! on `target = "scg::audit"` — the same target the production JSON
//! formatter would pick up. We don't install the JSON formatter here
//! because we want structured access to fields, not stringified lines.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use parking_lot::Mutex;
use scg_audit::{AuditConfig, AuditLogger};
use scg_auth::token::{StaticTokenAuthenticator, TokenEntry};
use scg_auth::{AnonymousAuthenticator, AuthInterceptor};
use scg_genproto::pb;
use scg_observability::Metrics;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router, TenantRouter};
use scg_store_memory::MemoryStore;
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

/// Backend that either OKs the request (Config / ReleaseSession) or
/// returns a non-OK status the gateway must surface as `rpc.error`.
#[derive(Default)]
struct PartialBackend;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for PartialBackend {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        let body = req.into_inner();
        Ok(Response::new(pb::ConfigResponse {
            session_id: body.session_id,
            ..Default::default()
        }))
    }
    async fn release_session(
        &self,
        req: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        let _ = req.into_inner();
        Ok(Response::new(pb::ReleaseSessionResponse::default()))
    }
    async fn analyze_plan(
        &self,
        _: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        // Deliberately fail so we can verify `rpc.error` fires.
        Err(Status::internal("boom"))
    }
    async fn artifact_status(
        &self,
        _: Request<pb::ArtifactStatusesRequest>,
    ) -> Result<Response<pb::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn interrupt(
        &self,
        _: Request<pb::InterruptRequest>,
    ) -> Result<Response<pb::InterruptResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn release_execute(
        &self,
        _: Request<pb::ReleaseExecuteRequest>,
    ) -> Result<Response<pb::ReleaseExecuteResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn fetch_error_details(
        &self,
        _: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn clone_session(
        &self,
        _: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn get_status(
        &self,
        _: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn execute_plan(
        &self,
        _: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn reattach_execute(
        &self,
        _: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn add_artifacts(
        &self,
        _: Request<tonic::Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

#[derive(Clone, Default)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

#[derive(Debug, Clone)]
struct CapturedEvent {
    fields: HashMap<String, String>,
}

impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "scg::audit" {
            return;
        }
        let mut fields = HashMap::new();
        struct Vis<'a>(&'a mut HashMap<String, String>);
        impl<'a> tracing::field::Visit for Vis<'a> {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.insert(f.name().into(), format!("{:?}", v));
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                self.0.insert(f.name().into(), v.into());
            }
        }
        event.record(&mut Vis(&mut fields));
        self.events.lock().push(CapturedEvent { fields });
    }
}

impl CaptureLayer {
    fn snapshot(&self) -> Vec<CapturedEvent> {
        self.events.lock().clone()
    }
}

/// Install the capture layer for the lifetime of the returned guard.
/// The guard scopes the subscriber to the current thread / task —
/// dropping it restores the previous subscriber.
fn install_capture() -> (CaptureLayer, DefaultGuard) {
    let cap = CaptureLayer::default();
    let sub = tracing_subscriber::registry().with(cap.clone());
    let guard = tracing::subscriber::set_default(sub);
    (cap, guard)
}

struct Rig {
    channel: Channel,
    _be_shutdown: tokio::sync::oneshot::Sender<()>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn spawn_rig(auth: AuthInterceptor) -> Rig {
    let be_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let be_addr = be_lis.local_addr().unwrap().to_string();
    let (be_tx, be_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(
                pb::spark_connect_service_server::SparkConnectServiceServer::new(PartialBackend),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(be_lis), async {
                let _ = be_rx.await;
            })
            .await
            .ok();
    });

    let metrics = Metrics::new().unwrap();
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![be_addr]).unwrap());
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let tr = TenantRouter::single(pool);
    let router = Arc::new(Router::new(tr, store));
    let dialer = Dialer::new();
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromMetadata {
            header: "x-tenant".into(),
        },
        on_missing: OnMissing::UseDefault,
        default_name: "default".into(),
    });
    let audit = AuditLogger::new(AuditConfig {
        enabled: true,
        log_successful_rpcs: false,
    });
    let proxy = SparkConnectProxy::with_all(router, dialer, auth, metrics, resolver, None, audit);

    let gw_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gw_addr = gw_lis.local_addr().unwrap();
    let (gw_tx, gw_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::spark_connect_service_server::SparkConnectServiceServer::new(proxy))
            .serve_with_incoming_shutdown(TcpListenerStream::new(gw_lis), async {
                let _ = gw_rx.await;
            })
            .await
            .ok();
    });

    let endpoint = Endpoint::from_shared(format!("http://{}", gw_addr)).unwrap();
    let channel = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    Rig {
        channel,
        _be_shutdown: be_tx,
        _gw_shutdown: gw_tx,
    }
}

fn config_request(session: &str, tenant: &str) -> Request<pb::ConfigRequest> {
    let mut req = Request::new(pb::ConfigRequest {
        session_id: session.into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from(tenant).unwrap());
    req
}

fn analyze_request(session: &str, tenant: &str) -> Request<pb::AnalyzePlanRequest> {
    let mut req = Request::new(pb::AnalyzePlanRequest {
        session_id: session.into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from(tenant).unwrap());
    req
}

fn release_request(session: &str, tenant: &str) -> Request<pb::ReleaseSessionRequest> {
    let mut req = Request::new(pb::ReleaseSessionRequest {
        session_id: session.into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from(tenant).unwrap());
    req
}

fn count_events(events: &[CapturedEvent], event_name: &str) -> usize {
    events
        .iter()
        .filter(|e| e.fields.get("event").map(|s| s.as_str()) == Some(event_name))
        .count()
}

fn find_event<'a>(events: &'a [CapturedEvent], event_name: &str) -> Option<&'a CapturedEvent> {
    events
        .iter()
        .find(|e| e.fields.get("event").map(|s| s.as_str()) == Some(event_name))
}

#[tokio::test]
async fn session_create_fires_once_per_binding() {
    let (cap, _guard) = install_capture();
    let auth = AuthInterceptor::new(Arc::new(AnonymousAuthenticator));
    let rig = spawn_rig(auth).await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    // First Config on (default, anonymous, sess-1) → session.create fires.
    c.config(config_request("sess-1", "default")).await.unwrap();
    // Second Config on the same key → router hits the cached binding;
    // session.create must NOT fire again.
    c.config(config_request("sess-1", "default")).await.unwrap();
    // A different session id → fresh binding → another session.create.
    c.config(config_request("sess-2", "default")).await.unwrap();

    let events = cap.snapshot();
    assert_eq!(
        count_events(&events, "session.create"),
        2,
        "expected exactly two session.create events (sess-1 + sess-2), got events: {:?}",
        events.iter().map(|e| &e.fields).collect::<Vec<_>>(),
    );
    let first = find_event(&events, "session.create").unwrap();
    assert_eq!(
        first.fields.get("tenant").map(|s| s.as_str()),
        Some("default")
    );
    assert_eq!(
        first.fields.get("session_id").map(|s| s.as_str()),
        Some("sess-1")
    );
    // Anonymous auth → empty groups. The field still appears (as ""),
    // so consumers can rely on its presence regardless of auth shape.
    assert_eq!(first.fields.get("groups").map(|s| s.as_str()), Some(""));
}

#[tokio::test]
async fn session_create_carries_groups_from_token() {
    // Static-token auth with a token that declares groups. The audit
    // event must carry them so operators querying the audit stream
    // can see who has which memberships.
    let (cap, _guard) = install_capture();
    let auth_inner = StaticTokenAuthenticator::new(vec![TokenEntry {
        token: "dev-token".into(),
        user_id: "alice".into(),
        tenant: None,
        groups: vec!["devs".into(), "admins".into()],
    }])
    .unwrap();
    let auth = AuthInterceptor::new(Arc::new(auth_inner));
    let rig = spawn_rig(auth).await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    let mut req = config_request("sess-grp", "default");
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer dev-token").unwrap(),
    );
    c.config(req).await.unwrap();

    let events = cap.snapshot();
    let evt = find_event(&events, "session.create").unwrap();
    assert_eq!(evt.fields.get("user_id").map(|s| s.as_str()), Some("alice"));
    assert_eq!(
        evt.fields.get("groups").map(|s| s.as_str()),
        Some("devs,admins"),
    );
}

#[tokio::test]
async fn release_session_emits_release_event() {
    let (cap, _guard) = install_capture();
    let auth = AuthInterceptor::new(Arc::new(AnonymousAuthenticator));
    let rig = spawn_rig(auth).await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    c.config(config_request("sess-rel", "default"))
        .await
        .unwrap();
    c.release_session(release_request("sess-rel", "default"))
        .await
        .unwrap();

    let events = cap.snapshot();
    assert_eq!(count_events(&events, "session.release"), 1);
    let rel = find_event(&events, "session.release").unwrap();
    assert_eq!(
        rel.fields.get("session_id").map(|s| s.as_str()),
        Some("sess-rel")
    );
}

#[tokio::test]
async fn auth_failure_emits_audit_event() {
    let (cap, _guard) = install_capture();
    // Static-token auth with one valid token; an unauthenticated
    // client request should fail.
    let auth_inner = StaticTokenAuthenticator::new(vec![TokenEntry {
        token: "secret".into(),
        user_id: "alice".into(),
        tenant: None,
        groups: vec![],
    }])
    .unwrap();
    let auth = AuthInterceptor::new(Arc::new(auth_inner));
    let rig = spawn_rig(auth).await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    // No Authorization metadata → Unauthenticated → auth.failure.
    let err = c
        .config(config_request("sess-x", "default"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let events = cap.snapshot();
    assert_eq!(count_events(&events, "auth.failure"), 1);
    let auth_evt = find_event(&events, "auth.failure").unwrap();
    assert_eq!(
        auth_evt.fields.get("rpc").map(|s| s.as_str()),
        Some("Config")
    );
    assert!(auth_evt.fields.contains_key("reason"));
    // Pre-identity failure: no rpc.error follow-up (auth.failure
    // already covers this).
    assert_eq!(count_events(&events, "rpc.error"), 0);
}

#[tokio::test]
async fn rpc_error_emits_audit_event_with_code() {
    let (cap, _guard) = install_capture();
    let auth = AuthInterceptor::new(Arc::new(AnonymousAuthenticator));
    let rig = spawn_rig(auth).await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    // Backend returns Internal("boom") on AnalyzePlan.
    let err = c
        .analyze_plan(analyze_request("sess-err", "default"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);

    let events = cap.snapshot();
    assert_eq!(count_events(&events, "rpc.error"), 1);
    let rpc_err = find_event(&events, "rpc.error").unwrap();
    assert_eq!(
        rpc_err.fields.get("rpc").map(|s| s.as_str()),
        Some("AnalyzePlan")
    );
    assert_eq!(
        rpc_err.fields.get("code").map(|s| s.as_str()),
        Some("Internal")
    );
}
