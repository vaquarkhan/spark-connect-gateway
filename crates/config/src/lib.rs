//! YAML configuration for the gateway.
//!
//! Two equivalent forms are accepted for backend discovery, to keep
//! Phase 1 deployments unchanged while Phase 2 introduces dynamic
//! sources:
//!
//! ```yaml
//! # Phase 1 shorthand (still valid):
//! backends:
//!   - "host1:15002"
//!   - "host2:15002"
//! ```
//!
//! ```yaml
//! # Tagged form (Phase 2):
//! backend_discovery:
//!   type: static
//!   addresses: ["host1:15002", "host2:15002"]
//! ```
//!
//! ```yaml
//! # Tagged form (Phase 2, K8s):
//! backend_discovery:
//!   type: k8s
//!   namespace: spark-connect
//!   service_name: spark-connect
//!   port: 15002
//! ```

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse config {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("config: must specify either `backends` or `backend_discovery`")]
    NoDiscoverySource,
    #[error("config: cannot specify both `backends` and `backend_discovery`")]
    ConflictingDiscovery,
    #[error("config: static backend list must contain at least one address")]
    EmptyStatic,
}

/// One of the supported backend discovery sources.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendDiscovery {
    /// Fixed list of `host:port` addresses, configured at startup.
    Static { addresses: Vec<String> },
    /// Watch a Kubernetes Service's Endpoints object.
    K8s {
        namespace: String,
        service_name: String,
        port: u16,
    },
}

/// Raw YAML shape — accepts either the legacy `backends` shorthand or the
/// tagged `backend_discovery` form, never both.
#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default = "default_bind_addr")]
    bind_addr: String,
    #[serde(default)]
    backends: Option<Vec<String>>,
    #[serde(default)]
    backend_discovery: Option<BackendDiscovery>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub discovery: BackendDiscovery,
}

fn default_bind_addr() -> String {
    ":15003".into()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_str = path.as_ref().display().to_string();
        let data = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Io {
            path: path_str.clone(),
            source: e,
        })?;
        let raw: RawConfig = serde_yaml::from_str(&data).map_err(|e| ConfigError::Parse {
            path: path_str,
            source: e,
        })?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let discovery = match (raw.backends, raw.backend_discovery) {
            (Some(_), Some(_)) => return Err(ConfigError::ConflictingDiscovery),
            (None, None) => return Err(ConfigError::NoDiscoverySource),
            (Some(addrs), None) => {
                if addrs.is_empty() {
                    return Err(ConfigError::EmptyStatic);
                }
                BackendDiscovery::Static { addresses: addrs }
            }
            (None, Some(d)) => {
                if let BackendDiscovery::Static { addresses } = &d {
                    if addresses.is_empty() {
                        return Err(ConfigError::EmptyStatic);
                    }
                }
                d
            }
        };
        let bind_addr = if raw.bind_addr.is_empty() {
            default_bind_addr()
        } else {
            raw.bind_addr
        };
        Ok(Self {
            bind_addr,
            discovery,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(text: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{}", text).unwrap();
        f
    }

    #[test]
    fn loads_legacy_backends_shorthand() {
        let f = write(
            r#"
bind_addr: ":15003"
backends:
  - "127.0.0.1:15002"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
        match c.discovery {
            BackendDiscovery::Static { addresses } => {
                assert_eq!(addresses, vec!["127.0.0.1:15002"]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_tagged_static() {
        let f = write(
            r#"
backend_discovery:
  type: static
  addresses: ["a:1", "b:2"]
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.discovery {
            BackendDiscovery::Static { addresses } => {
                assert_eq!(addresses, vec!["a:1", "b:2"]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_tagged_k8s() {
        let f = write(
            r#"
backend_discovery:
  type: k8s
  namespace: spark-connect
  service_name: spark-connect
  port: 15002
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.discovery {
            BackendDiscovery::K8s {
                namespace,
                service_name,
                port,
            } => {
                assert_eq!(namespace, "spark-connect");
                assert_eq!(service_name, "spark-connect");
                assert_eq!(port, 15002);
            }
            other => panic!("expected K8s, got {:?}", other),
        }
    }

    #[test]
    fn empty_backends_shorthand_rejected() {
        let f = write("backends: []\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::EmptyStatic
        ));
    }

    #[test]
    fn empty_static_in_tagged_form_rejected() {
        let f = write(
            r#"
backend_discovery:
  type: static
  addresses: []
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::EmptyStatic
        ));
    }

    #[test]
    fn missing_discovery_rejected() {
        let f = write("bind_addr: ':15003'\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::NoDiscoverySource
        ));
    }

    #[test]
    fn conflicting_discovery_rejected() {
        let f = write(
            r#"
backends: ["a:1"]
backend_discovery:
  type: static
  addresses: ["b:2"]
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::ConflictingDiscovery
        ));
    }

    #[test]
    fn defaults_bind_addr() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
    }
}
