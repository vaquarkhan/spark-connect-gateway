//! `AnonymousAuthenticator` — used when auth is explicitly disabled.
//!
//! Returns a fixed `Identity { user_id: "anonymous", … }`: the
//! gateway accepts every request without authentication. Fine for
//! trusted in-cluster networks, never for external exposure.

use async_trait::async_trait;
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::{Authenticator, Identity};

#[derive(Debug, Default)]
pub struct AnonymousAuthenticator;

#[async_trait]
impl Authenticator for AnonymousAuthenticator {
    async fn authenticate(&self, _metadata: &MetadataMap) -> Result<Identity, Status> {
        Ok(Identity::user("anonymous"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn always_accepts() {
        let a = AnonymousAuthenticator;
        let id = a.authenticate(&MetadataMap::new()).await.unwrap();
        assert_eq!(id.user_id, "anonymous");
    }
}
