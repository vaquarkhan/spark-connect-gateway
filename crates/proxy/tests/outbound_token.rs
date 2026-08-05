//! Integration tests for the gateway→backend bearer token.
//!
//! Backends started with `spark.connect.authenticate.token` require
//! `authorization: Bearer <token>` on every request and reject
//! everything else with `UNAUTHENTICATED` (Spark's
//! `PreSharedKeyAuthenticationInterceptor` compares the exact
//! string). The mock backend here reproduces that check, so these
//! tests cover both halves of the trust boundary at the process
//! level: a direct connection without the token is refused, and the
//! same request through a token-configured gateway succeeds.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scg_genproto::pb;
use scg_proxy::{BackendTokens, Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router, TenantRouter};
use scg_store_memory::MemoryStore;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Records the `authorization` metadata of every `Config` call and,
/// when `expected` is set, enforces it exactly the way Spark's
/// `PreSharedKeyAuthenticationInterceptor` does.
struct RecordingBackend {
    seen: Arc<Mutex<Vec<Option<String>>>>,
    expected: Option<String>,
}

impl RecordingBackend {
    fn check<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        self.seen.lock().unwrap().push(header.clone());
        match &self.expected {
            None => Ok(()),
            Some(want) => match header {
                None => Err(Status::unauthenticated("No authentication token provided")),
                Some(got) if &got == want => Ok(()),
                Some(_) => Err(Status::unauthenticated("Invalid authentication token")),
            },
        }
    }
}

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for RecordingBackend {
    type ExecutePlanStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>,
    >;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        self.check(&req)?;
        let body = req.into_inner();
        Ok(Response::new(pb::ConfigResponse {
            session_id: body.session_id,
            ..Default::default()
        }))
    }

    async fn analyze_plan(
        &self,
        _: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("n/a"))
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
    async fn release_session(
        &self,
        _: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn fetch_error_details(
        &self,
        _: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn get_status(
        &self,
        _: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn clone_session(
        &self,
        _: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
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

type Seen = Arc<Mutex<Vec<Option<String>>>>;

async fn spawn_backend(expected: Option<&str>) -> (String, Seen, tokio::sync::oneshot::Sender<()>) {
    let seen: Seen = Arc::default();
    let backend = RecordingBackend {
        seen: seen.clone(),
        expected: expected.map(str::to_string),
    };
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::spark_connect_service_server::SparkConnectServiceServer::new(backend))
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    (addr, seen, tx)
}

struct Rig {
    channel: Channel,
    backend_addr: String,
    seen: Seen,
    _backend_shutdown: tokio::sync::oneshot::Sender<()>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn spawn_rig(backend_expected: Option<&str>, tokens: BackendTokens) -> Rig {
    let (backend_addr, seen, backend_tx) = spawn_backend(backend_expected).await;

    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let pool: Arc<dyn Pool> =
        Arc::new(scg_pool_static::StaticPool::new(vec![backend_addr.clone()]).unwrap());
    let router = Arc::new(Router::new(TenantRouter::single(pool), store));
    let proxy = SparkConnectProxy::new(router, Dialer::new()).with_backend_tokens(tokens);

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

    let channel = Endpoint::from_shared(format!("http://{}", gw_addr))
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();

    Rig {
        channel,
        backend_addr,
        seen,
        _backend_shutdown: backend_tx,
        _gw_shutdown: gw_tx,
    }
}

fn client(
    channel: Channel,
) -> pb::spark_connect_service_client::SparkConnectServiceClient<Channel> {
    pb::spark_connect_service_client::SparkConnectServiceClient::new(channel)
}

fn config_req(session: &str) -> pb::ConfigRequest {
    pb::ConfigRequest {
        session_id: session.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn default_token_reaches_the_backend() {
    let tokens = BackendTokens::new(Some("hunter2".into()), HashMap::new()).unwrap();
    let rig = spawn_rig(None, tokens).await;
    client(rig.channel.clone())
        .config(config_req("s1"))
        .await
        .unwrap();
    assert_eq!(
        rig.seen.lock().unwrap().as_slice(),
        &[Some("Bearer hunter2".to_string())]
    );
}

#[tokio::test]
async fn no_token_configured_sends_no_header() {
    let rig = spawn_rig(None, BackendTokens::none()).await;
    client(rig.channel.clone())
        .config(config_req("s1"))
        .await
        .unwrap();
    assert_eq!(rig.seen.lock().unwrap().as_slice(), &[None]);
}

#[tokio::test]
async fn inbound_client_credential_is_not_forwarded() {
    // The gateway's own inbound auth may be bearer-based too; the
    // client's credential must never leak to the backend — the
    // backend sees the pool token or nothing.
    let rig = spawn_rig(None, BackendTokens::none()).await;
    let mut req = Request::new(config_req("s1"));
    req.metadata_mut()
        .insert("authorization", "Bearer client-secret".parse().unwrap());
    client(rig.channel.clone()).config(req).await.unwrap();
    assert_eq!(rig.seen.lock().unwrap().as_slice(), &[None]);
}

#[tokio::test]
async fn enforcing_backend_rejects_direct_but_accepts_gateway() {
    // The trust-boundary property, at process level: the same
    // backend that refuses a tokenless direct connection accepts
    // the gateway's, because only the gateway holds the token.
    let tokens = BackendTokens::new(Some("gw-only".into()), HashMap::new()).unwrap();
    let rig = spawn_rig(Some("Bearer gw-only"), tokens).await;

    // Direct to the backend, no token: UNAUTHENTICATED.
    let direct = Endpoint::from_shared(format!("http://{}", rig.backend_addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let err = client(direct)
        .config(config_req("s-direct"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Through the gateway: OK.
    client(rig.channel.clone())
        .config(config_req("s-gw"))
        .await
        .unwrap();
}
