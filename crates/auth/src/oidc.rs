//! `OidcAuthenticator` — verifies JWTs against a remote JWKS endpoint.
//!
//! On startup we fetch the JWKS once (or, if `discovery_url` is set,
//! we resolve the OIDC discovery document first to find the JWKS URL).
//! Keys are cached in memory; if a token's `kid` is missing from the
//! cache, we refresh once before failing the request — that handles
//! the IdP rotating keys without us paying a network round-trip per
//! request.
//!
//! Validation rules (issuer, audience, claim mapping) are reused from
//! [`crate::jwt`] — JWKS only changes *where* the verification key
//! comes from.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use parking_lot::RwLock;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::{debug, warn};

use crate::{bearer_token, Authenticator, Identity};

#[derive(Debug, Clone, Deserialize)]
pub struct OidcConfig {
    /// JWKS URL. Either this or `discovery_url` must be set.
    #[serde(default)]
    pub jwks_url: Option<String>,
    /// OIDC discovery document URL. We resolve `jwks_uri` from it.
    #[serde(default)]
    pub discovery_url: Option<String>,
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    #[serde(default)]
    pub tenant_claim: Option<String>,
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// Minimum interval between forced refreshes when an unknown `kid`
    /// is seen. Prevents a barrage of bogus tokens from hammering the
    /// IdP. Defaults to 60s.
    #[serde(default = "default_refresh_floor_secs")]
    pub refresh_floor_secs: u64,
}

fn default_user_id_claim() -> String {
    "sub".into()
}
fn default_refresh_floor_secs() -> u64 {
    60
}

#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("oidc: must set jwks_url or discovery_url")]
    NoJwksSource,
    #[error("oidc: at least one algorithm must be configured")]
    NoAlgorithms,
    #[error("oidc: unknown algorithm {0}")]
    UnknownAlg(String),
    #[error("oidc: HTTP error fetching {url}: {source}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("oidc: discovery document at {url} missing jwks_uri")]
    DiscoveryMissingJwks { url: String },
    #[error("oidc: parse JWKS at {url}: {source}")]
    JwksParse {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("oidc: bad PEM in JWKS for kid {kid}: {source}")]
    BadKey {
        kid: String,
        #[source]
        source: jsonwebtoken::errors::Error,
    },
}

#[derive(Debug, Deserialize)]
struct DiscoveryDoc {
    jwks_uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<JwksKey>,
}

#[derive(Debug, Deserialize)]
struct JwksKey {
    kid: Option<String>,
    kty: String,
    // We deserialize but don't enforce `alg` per-key; per-token alg is
    // already constrained by the configured algorithms list.
    #[serde(default, rename = "alg")]
    _alg: Option<String>,
    // RSA modulus / exponent
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    // EC coordinates
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
}

struct CachedKeys {
    keys: HashMap<String, DecodingKey>,
    last_refreshed: Instant,
}

pub struct OidcAuthenticator {
    jwks_url: String,
    http: reqwest::Client,
    cache: Arc<RwLock<CachedKeys>>,
    refresh_lock: Arc<AsyncMutex<()>>,
    refresh_floor: Duration,

    algorithms: Vec<Algorithm>,
    issuer: Option<String>,
    audience: Option<String>,
    user_id_claim: String,
    tenant_claim: Option<String>,
    groups_claim: Option<String>,
}

impl OidcAuthenticator {
    /// Build the authenticator and perform an initial JWKS fetch.
    pub async fn new(cfg: OidcConfig) -> Result<Self, OidcError> {
        if cfg.algorithms.is_empty() {
            return Err(OidcError::NoAlgorithms);
        }
        let algorithms: Vec<Algorithm> = cfg
            .algorithms
            .iter()
            .map(|a| parse_algorithm(a))
            .collect::<Result<_, _>>()?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client");

        let jwks_url = match (&cfg.jwks_url, &cfg.discovery_url) {
            (Some(u), _) => u.clone(),
            (None, Some(u)) => resolve_jwks_url(&http, u).await?,
            (None, None) => return Err(OidcError::NoJwksSource),
        };

        let keys = fetch_jwks(&http, &jwks_url).await?;
        let cache = Arc::new(RwLock::new(CachedKeys {
            keys,
            last_refreshed: Instant::now(),
        }));

        Ok(Self {
            jwks_url,
            http,
            cache,
            refresh_lock: Arc::new(AsyncMutex::new(())),
            refresh_floor: Duration::from_secs(cfg.refresh_floor_secs),
            algorithms,
            issuer: cfg.issuer,
            audience: cfg.audience,
            user_id_claim: cfg.user_id_claim,
            tenant_claim: cfg.tenant_claim,
            groups_claim: cfg.groups_claim,
        })
    }

    /// Look up the decoding key for `kid`, refreshing the JWKS once
    /// (subject to the refresh floor) on miss.
    async fn key_for(&self, kid: &str) -> Option<DecodingKey> {
        if let Some(k) = self.cache.read().keys.get(kid) {
            return Some(k.clone());
        }
        // Miss → try to refresh, but rate-limited.
        let guard = self.refresh_lock.lock().await;
        // Re-check under lock — another caller may have refreshed already.
        if let Some(k) = self.cache.read().keys.get(kid) {
            return Some(k.clone());
        }
        let elapsed = self.cache.read().last_refreshed.elapsed();
        if elapsed < self.refresh_floor {
            debug!(?elapsed, "oidc: skipping refresh — within floor");
            return None;
        }
        match fetch_jwks(&self.http, &self.jwks_url).await {
            Ok(new_keys) => {
                let mut g = self.cache.write();
                g.keys = new_keys;
                g.last_refreshed = Instant::now();
                drop(guard);
                self.cache.read().keys.get(kid).cloned()
            }
            Err(e) => {
                warn!(error = %e, "oidc: JWKS refresh failed");
                None
            }
        }
    }

    fn validation(&self) -> Validation {
        let mut v = Validation::new(self.algorithms[0]);
        v.algorithms = self.algorithms.clone();
        if let Some(iss) = &self.issuer {
            v.set_issuer(std::slice::from_ref(iss));
        }
        if let Some(aud) = &self.audience {
            v.set_audience(std::slice::from_ref(aud));
        } else {
            v.validate_aud = false;
        }
        v
    }
}

#[async_trait]
impl Authenticator for OidcAuthenticator {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Identity, Status> {
        let token = bearer_token(metadata).ok_or_else(|| {
            Status::unauthenticated("missing or malformed Authorization: Bearer header")
        })?;

        let header = decode_header(token).map_err(|e| {
            debug!(error = %e, "oidc: malformed JWT header");
            Status::unauthenticated("invalid JWT header")
        })?;
        let kid = header
            .kid
            .ok_or_else(|| Status::unauthenticated("JWT missing kid; OIDC requires keyed JWTs"))?;
        let key = self.key_for(&kid).await.ok_or_else(|| {
            debug!(%kid, "oidc: no matching key");
            Status::unauthenticated("JWT kid not in JWKS")
        })?;

        let claims = decode::<HashMap<String, serde_json::Value>>(token, &key, &self.validation())
            .map_err(|e| {
                debug!(error = %e, "oidc: validation failed");
                Status::unauthenticated("invalid JWT")
            })?;
        let user_id = claims
            .claims
            .get(&self.user_id_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Status::unauthenticated(format!(
                    "JWT missing required claim {:?}",
                    self.user_id_claim
                ))
            })?
            .to_string();
        let tenant = self
            .tenant_claim
            .as_ref()
            .and_then(|c| claims.claims.get(c))
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let groups: Vec<String> = self
            .groups_claim
            .as_ref()
            .and_then(|c| claims.claims.get(c))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Identity {
            user_id,
            tenant,
            groups,
        })
    }
}

async fn resolve_jwks_url(http: &reqwest::Client, url: &str) -> Result<String, OidcError> {
    let resp = http.get(url).send().await.map_err(|e| OidcError::Http {
        url: url.into(),
        source: e,
    })?;
    let doc: DiscoveryDoc = resp.json().await.map_err(|e| OidcError::Http {
        url: url.into(),
        source: e,
    })?;
    doc.jwks_uri
        .ok_or_else(|| OidcError::DiscoveryMissingJwks { url: url.into() })
}

async fn fetch_jwks(
    http: &reqwest::Client,
    url: &str,
) -> Result<HashMap<String, DecodingKey>, OidcError> {
    let resp = http.get(url).send().await.map_err(|e| OidcError::Http {
        url: url.into(),
        source: e,
    })?;
    let jwks: Jwks = resp.json().await.map_err(|e| OidcError::JwksParse {
        url: url.into(),
        source: e,
    })?;
    let mut out = HashMap::with_capacity(jwks.keys.len());
    for k in jwks.keys {
        let kid = k.kid.clone().unwrap_or_default();
        if kid.is_empty() {
            // No kid → can't index; skip.
            continue;
        }
        let dk = match k.kty.as_str() {
            "RSA" => match (k.n.as_deref(), k.e.as_deref()) {
                (Some(n), Some(e)) => {
                    DecodingKey::from_rsa_components(n, e).map_err(|err| OidcError::BadKey {
                        kid: kid.clone(),
                        source: err,
                    })?
                }
                _ => continue,
            },
            "EC" => match (k.crv.as_deref(), k.x.as_deref(), k.y.as_deref()) {
                (Some(_crv), Some(x), Some(y)) => {
                    DecodingKey::from_ec_components(x, y).map_err(|err| OidcError::BadKey {
                        kid: kid.clone(),
                        source: err,
                    })?
                }
                _ => continue,
            },
            other => {
                debug!(kty = %other, "oidc: skipping unsupported key type");
                continue;
            }
        };
        out.insert(kid, dk);
    }
    Ok(out)
}

fn parse_algorithm(name: &str) -> Result<Algorithm, OidcError> {
    match name.to_ascii_uppercase().as_str() {
        "RS256" => Ok(Algorithm::RS256),
        "RS384" => Ok(Algorithm::RS384),
        "RS512" => Ok(Algorithm::RS512),
        "PS256" => Ok(Algorithm::PS256),
        "PS384" => Ok(Algorithm::PS384),
        "PS512" => Ok(Algorithm::PS512),
        "ES256" => Ok(Algorithm::ES256),
        "ES384" => Ok(Algorithm::ES384),
        "EDDSA" => Ok(Algorithm::EdDSA),
        other => Err(OidcError::UnknownAlg(other.into())),
    }
}
