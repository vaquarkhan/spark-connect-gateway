//! YAML configuration for the gateway.

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
    #[error("config: at least one backend required in `backends`")]
    NoBackends,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default)]
    pub backends: Vec<String>,
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
        let mut c: Config = serde_yaml::from_str(&data).map_err(|e| ConfigError::Parse {
            path: path_str,
            source: e,
        })?;
        if c.backends.is_empty() {
            return Err(ConfigError::NoBackends);
        }
        if c.bind_addr.is_empty() {
            c.bind_addr = default_bind_addr();
        }
        Ok(c)
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
    fn loads_minimal() {
        let f = write(
            r#"
bind_addr: ":15003"
backends:
  - "127.0.0.1:15002"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
        assert_eq!(c.backends, vec!["127.0.0.1:15002"]);
    }

    #[test]
    fn empty_backends_rejected() {
        let f = write("backends: []\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::NoBackends
        ));
    }

    #[test]
    fn defaults_bind_addr() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
    }
}
