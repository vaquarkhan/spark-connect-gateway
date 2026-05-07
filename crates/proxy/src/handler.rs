//! `SparkConnectService` implementation that forwards every RPC to a
//! backend chosen by the [`Router`].

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use scg_genproto::pb;
use scg_routing::{Router, SessionKey};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::dial::Dialer;

/// gRPC handler implementing `SparkConnectService` as a forwarding proxy.
pub struct SparkConnectProxy {
    router: Arc<Router>,
    dialer: Arc<Dialer>,
}

impl SparkConnectProxy {
    pub fn new(router: Arc<Router>, dialer: Arc<Dialer>) -> Self {
        Self { router, dialer }
    }

    fn client(
        &self,
        addr: &str,
    ) -> Result<
        pb::spark_connect_service_client::SparkConnectServiceClient<tonic::transport::Channel>,
        Status,
    > {
        let ch = self
            .dialer
            .channel(addr)
            .map_err(|e| Status::unavailable(format!("dial backend {}: {}", addr, e)))?;
        Ok(pb::spark_connect_service_client::SparkConnectServiceClient::new(ch))
    }
}

fn key_from(session_id: &str, uc: Option<&pb::UserContext>) -> SessionKey {
    SessionKey::new(uc.map(|u| u.user_id.as_str()).unwrap_or(""), session_id)
}

/// Forward a tonic streaming response to a fresh server-stream sent through
/// `tx`. We don't pipe directly to the inbound `Streaming` because we need
/// to return early on the first error and report it as a Status.
type StreamItem<T> = Result<T, Status>;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for SparkConnectProxy {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = StreamItem<pb::ExecutePlanResponse>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    // ----- Unary RPCs ----------------------------------------------------

    async fn analyze_plan(
        &self,
        req: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        c.analyze_plan(Request::new(body)).await
    }

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        c.config(Request::new(body)).await
    }

    async fn artifact_status(
        &self,
        req: Request<pb::ArtifactStatusesRequest>,
    ) -> Result<Response<pb::ArtifactStatusesResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        c.artifact_status(Request::new(body)).await
    }

    async fn interrupt(
        &self,
        req: Request<pb::InterruptRequest>,
    ) -> Result<Response<pb::InterruptResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        // Interrupt may target a specific operation id (one of several
        // InterruptType variants); when present, route by op id.
        let op_id = match body.interrupt.as_ref() {
            Some(pb::interrupt_request::Interrupt::OperationId(id)) => id.clone(),
            _ => String::new(),
        };
        let addr = self.router.resolve_op(&op_id, &key);
        let mut c = self.client(&addr)?;
        c.interrupt(Request::new(body)).await
    }

    async fn release_execute(
        &self,
        req: Request<pb::ReleaseExecuteRequest>,
    ) -> Result<Response<pb::ReleaseExecuteResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let op_id = body.operation_id.clone();
        let addr = self.router.resolve_op(&op_id, &key);
        let mut c = self.client(&addr)?;
        let resp = c.release_execute(Request::new(body)).await?;
        // On a successful release the server has dropped the operation, so
        // we drop our reverse-index entry too.
        self.router.forget_op(&op_id);
        Ok(resp)
    }

    async fn release_session(
        &self,
        req: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        let resp = c.release_session(Request::new(body)).await?;
        self.router.forget_session(&key);
        Ok(resp)
    }

    async fn fetch_error_details(
        &self,
        req: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        c.fetch_error_details(Request::new(body)).await
    }

    async fn clone_session(
        &self,
        req: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        c.clone_session(Request::new(body)).await
    }

    async fn get_status(
        &self,
        req: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;
        c.get_status(Request::new(body)).await
    }

    // ----- Server-streaming RPCs ----------------------------------------

    async fn execute_plan(
        &self,
        req: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_session(&key);

        // Bind operation_id → backend so a follow-up ReattachExecute reaches
        // the same driver even if its session id is missing or has been
        // forgotten by the affinity cache.
        if let Some(op_id) = body.operation_id.clone() {
            if !op_id.is_empty() {
                self.router.remember_op(op_id, addr.clone());
            }
        }

        let mut c = self.client(&addr)?;
        let upstream = c.execute_plan(Request::new(body)).await?.into_inner();
        Ok(Response::new(forward_server_stream(upstream)))
    }

    async fn reattach_execute(
        &self,
        req: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let body = req.into_inner();
        let key = key_from(&body.session_id, body.user_context.as_ref());
        let addr = self.router.resolve_op(&body.operation_id, &key);
        let mut c = self.client(&addr)?;
        let upstream = c.reattach_execute(Request::new(body)).await?.into_inner();
        Ok(Response::new(forward_server_stream(upstream)))
    }

    // ----- Client-streaming RPCs ----------------------------------------

    async fn add_artifacts(
        &self,
        req: Request<Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        let mut inbound = req.into_inner();

        // We need the first message to make the routing decision, then we
        // forward it plus the remainder to the chosen backend.
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("AddArtifacts: empty client stream"))?;

        let key = key_from(&first.session_id, first.user_context.as_ref());
        let addr = self.router.resolve_session(&key);
        let mut c = self.client(&addr)?;

        let (tx, rx) = tokio::sync::mpsc::channel::<pb::AddArtifactsRequest>(8);
        // First message goes through the channel before we await the RPC,
        // so the upstream sees it as the very first request.
        tx.send(first)
            .await
            .map_err(|_| Status::cancelled("backend closed"))?;

        // Spawn a task that drains the inbound client stream onto the
        // outbound channel. When inbound ends, dropping `tx` signals
        // end-of-stream to the backend.
        tokio::spawn(async move {
            while let Ok(Some(m)) = inbound.message().await {
                if tx.send(m).await.is_err() {
                    break;
                }
            }
        });

        let resp = c
            .add_artifacts(Request::new(ReceiverStream::new(rx)))
            .await?;
        Ok(resp)
    }
}

fn forward_server_stream<T: Send + 'static>(
    upstream: Streaming<T>,
) -> Pin<Box<dyn Stream<Item = StreamItem<T>> + Send + 'static>> {
    Box::pin(upstream.map(|res| match res {
        Ok(msg) => Ok(msg),
        Err(status) => Err(status),
    }))
}
