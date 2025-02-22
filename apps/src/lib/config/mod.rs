//! Node and client configuration

pub mod genesis;
pub mod global;
pub mod utils;

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use namada::types::chain::ChainId;
use namada::types::storage::BlockHeight;
use namada::types::time::Rfc3339String;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::cli;
use crate::facade::tendermint::Timeout;
use crate::facade::tendermint_config::net::Address as TendermintAddress;

/// Base directory contains global config and chain directories.
pub const DEFAULT_BASE_DIR: &str = ".namada";
/// Default WASM dir.
pub const DEFAULT_WASM_DIR: &str = "wasm";
/// The WASM checksums file contains the hashes of built WASMs. It is inside the
/// WASM dir.
pub const DEFAULT_WASM_CHECKSUMS_FILE: &str = "checksums.json";
/// Chain-specific Namada configuration. Nested in chain dirs.
pub const FILENAME: &str = "config.toml";
/// Chain-specific Tendermint configuration. Nested in chain dirs.
pub const TENDERMINT_DIR: &str = "tendermint";
/// Chain-specific Namada DB. Nested in chain dirs.
pub const DB_DIR: &str = "db";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub wasm_dir: PathBuf,
    pub ledger: Ledger,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TendermintMode {
    Full,
    Validator,
    Seed,
}

impl TendermintMode {
    pub fn to_str(&self) -> &str {
        match *self {
            TendermintMode::Full => "full",
            TendermintMode::Validator => "validator",
            TendermintMode::Seed => "seed",
        }
    }
}

impl From<String> for TendermintMode {
    fn from(mode: String) -> Self {
        match mode.as_str() {
            "full" => TendermintMode::Full,
            "validator" => TendermintMode::Validator,
            "seed" => TendermintMode::Seed,
            _ => panic!("Unrecognized mode"),
        }
    }
}

/// An action to be performed at a
/// certain block height.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    /// Stop the chain.
    Halt,
    /// Suspend consensus indefinitely.
    Suspend,
}

/// An action to be performed at a
/// certain block height along with the
/// given height.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionAtHeight {
    /// The height at which to take action.
    pub height: BlockHeight,
    /// The action to take.
    pub action: Action,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ledger {
    pub genesis_time: Rfc3339String,
    pub chain_id: ChainId,
    pub shell: Shell,
    pub tendermint: Tendermint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Shell {
    pub base_dir: PathBuf,
    pub ledger_address: SocketAddr,
    /// RocksDB block cache maximum size in bytes.
    /// When not set, defaults to 1/3 of the available memory.
    pub block_cache_bytes: Option<u64>,
    /// VP WASM compilation cache maximum size in bytes.
    /// When not set, defaults to 1/6 of the available memory.
    pub vp_wasm_compilation_cache_bytes: Option<u64>,
    /// Tx WASM compilation in-memory cache maximum size in bytes.
    /// When not set, defaults to 1/6 of the available memory.
    pub tx_wasm_compilation_cache_bytes: Option<u64>,
    /// When set, will limit the how many block heights in the past can the
    /// storage be queried for reading values.
    pub storage_read_past_height_limit: Option<u64>,
    /// Use the [`Ledger::db_dir()`] method to read the value.
    db_dir: PathBuf,
    /// Use the [`Ledger::tendermint_dir()`] method to read the value.
    tendermint_dir: PathBuf,
    /// An optional action to take when a given blockheight is reached.
    pub action_at_height: Option<ActionAtHeight>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tendermint {
    pub rpc_address: SocketAddr,
    pub p2p_address: SocketAddr,
    /// The persistent peers addresses must include node ID
    pub p2p_persistent_peers: Vec<TendermintAddress>,
    /// Turns the peer exchange reactor on or off. Validator node will want the
    /// pex turned off.
    pub p2p_pex: bool,
    /// Toggle to disable guard against peers connecting from the same IP
    pub p2p_allow_duplicate_ip: bool,
    /// Set `true` for strict address routability rules
    /// Set `false` for private or local networks
    pub p2p_addr_book_strict: bool,
    /// How long we wait after committing a block, before starting on the new
    /// height
    pub consensus_timeout_commit: Timeout,
    pub tendermint_mode: TendermintMode,
    pub instrumentation_prometheus: bool,
    pub instrumentation_prometheus_listen_addr: SocketAddr,
    pub instrumentation_namespace: String,
    /// Toggle to enable tx indexing
    pub tx_index: bool,
}

impl Ledger {
    pub fn new(
        base_dir: impl AsRef<Path>,
        chain_id: ChainId,
        mode: TendermintMode,
    ) -> Self {
        Self {
            genesis_time: Rfc3339String("1970-01-01T00:00:00Z".to_owned()),
            chain_id,
            shell: Shell {
                base_dir: base_dir.as_ref().to_owned(),
                ledger_address: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    26658,
                ),
                block_cache_bytes: None,
                vp_wasm_compilation_cache_bytes: None,
                tx_wasm_compilation_cache_bytes: None,
                // Default corresponds to 1 hour of past blocks at 1 block/sec
                storage_read_past_height_limit: Some(3600),
                db_dir: DB_DIR.into(),
                tendermint_dir: TENDERMINT_DIR.into(),
                action_at_height: None,
            },
            tendermint: Tendermint {
                rpc_address: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    26657,
                ),
                p2p_address: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    26656,
                ),
                p2p_persistent_peers: vec![],
                p2p_pex: true,
                p2p_allow_duplicate_ip: false,
                p2p_addr_book_strict: true,
                consensus_timeout_commit: Timeout::from_str("1s").unwrap(),
                tendermint_mode: mode,
                instrumentation_prometheus: false,
                instrumentation_prometheus_listen_addr: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    26661,
                ),
                instrumentation_namespace: "namadan_tm".to_string(),
                tx_index: false,
            },
        }
    }

    /// Get the chain directory path
    pub fn chain_dir(&self) -> PathBuf {
        self.shell.base_dir.join(self.chain_id.as_str())
    }

    /// Get the directory path to the DB
    pub fn db_dir(&self) -> PathBuf {
        self.shell.db_dir(&self.chain_id)
    }

    /// Get the directory path to Tendermint
    pub fn tendermint_dir(&self) -> PathBuf {
        self.shell.tendermint_dir(&self.chain_id)
    }
}

impl Shell {
    /// Get the directory path to the DB
    pub fn db_dir(&self, chain_id: &ChainId) -> PathBuf {
        self.base_dir.join(chain_id.as_str()).join(&self.db_dir)
    }

    /// Get the directory path to Tendermint
    pub fn tendermint_dir(&self, chain_id: &ChainId) -> PathBuf {
        self.base_dir
            .join(chain_id.as_str())
            .join(&self.tendermint_dir)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error while reading config: {0}")]
    ReadError(config::ConfigError),
    #[error("Error while deserializing config: {0}")]
    DeserializationError(config::ConfigError),
    #[error("Error while serializing to toml: {0}")]
    TomlError(toml::ser::Error),
    #[error("Error while writing config: {0}")]
    WriteError(std::io::Error),
    #[error("A config file already exists in {0}")]
    AlreadyExistingConfig(PathBuf),
    #[error(
        "Bootstrap peer {0} is not valid. Format needs to be \
         {{protocol}}/{{ip}}/tcp/{{port}}/p2p/{{peerid}}"
    )]
    BadBootstrapPeerFormat(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum SerdeError {
    // This is needed for serde https://serde.rs/error-handling.html
    #[error(
        "Bootstrap peer {0} is not valid. Format needs to be \
         {{protocol}}/{{ip}}/tcp/{{port}}/p2p/{{peerid}}"
    )]
    BadBootstrapPeerFormat(String),
    #[error("{0}")]
    Message(String),
}

impl Config {
    pub fn new(
        base_dir: impl AsRef<Path>,
        chain_id: ChainId,
        mode: TendermintMode,
    ) -> Self {
        Self {
            wasm_dir: DEFAULT_WASM_DIR.into(),
            ledger: Ledger::new(base_dir, chain_id, mode),
        }
    }

    /// Load config from expected path in the `base_dir` or generate a new one
    /// if it doesn't exist. Terminates with an error if the config loading
    /// fails.
    pub fn load(
        base_dir: impl AsRef<Path>,
        chain_id: &ChainId,
        mode: Option<TendermintMode>,
    ) -> Self {
        let base_dir = base_dir.as_ref();
        match Self::read(base_dir, chain_id, mode) {
            Ok(mut config) => {
                config.ledger.shell.base_dir = base_dir.to_path_buf();
                config
            }
            Err(err) => {
                eprintln!(
                    "Tried to read config in {} but failed with: {}",
                    base_dir.display(),
                    err
                );
                cli::safe_exit(1)
            }
        }
    }

    /// Read the config from a file, or generate a default one and write it to
    /// a file if it doesn't already exist. Keys that are expected but not set
    /// in the config file are filled in with default values.
    pub fn read(
        base_dir: &Path,
        chain_id: &ChainId,
        mode: Option<TendermintMode>,
    ) -> Result<Self> {
        let file_path = Self::file_path(base_dir, chain_id);
        let file_name = file_path.to_str().expect("Expected UTF-8 file path");
        let mode = mode.unwrap_or(TendermintMode::Full);
        if !file_path.exists() {
            return Self::generate(base_dir, chain_id, mode, true);
        };
        let defaults = config::Config::try_from(&Self::new(
            base_dir,
            chain_id.clone(),
            mode,
        ))
        .map_err(Error::ReadError)?;
        let mut config = config::Config::new();
        config
            .merge(defaults)
            .and_then(|c| c.merge(config::File::with_name(file_name)))
            .and_then(|c| {
                c.merge(
                    config::Environment::with_prefix("namada").separator("__"),
                )
            })
            .map_err(Error::ReadError)?;
        config.try_into().map_err(Error::DeserializationError)
    }

    /// Generate configuration and write it to a file.
    pub fn generate(
        base_dir: &Path,
        chain_id: &ChainId,
        mode: TendermintMode,
        replace: bool,
    ) -> Result<Self> {
        let config = Config::new(base_dir, chain_id.clone(), mode);
        config.write(base_dir, chain_id, replace)?;
        Ok(config)
    }

    /// Write configuration to a file.
    pub fn write(
        &self,
        base_dir: &Path,
        chain_id: &ChainId,
        replace: bool,
    ) -> Result<()> {
        let file_path = Self::file_path(base_dir, chain_id);
        let file_dir = file_path.parent().unwrap();
        create_dir_all(file_dir).map_err(Error::WriteError)?;
        if file_path.exists() && !replace {
            Err(Error::AlreadyExistingConfig(file_path))
        } else {
            let mut file =
                File::create(file_path).map_err(Error::WriteError)?;
            let toml = toml::ser::to_string(&self).map_err(|err| {
                if let toml::ser::Error::ValueAfterTable = err {
                    tracing::error!("{}", VALUE_AFTER_TABLE_ERROR_MSG);
                }
                Error::TomlError(err)
            })?;
            file.write_all(toml.as_bytes()).map_err(Error::WriteError)
        }
    }

    /// Get the file path to the config
    pub fn file_path(
        base_dir: impl AsRef<Path>,
        chain_id: &ChainId,
    ) -> PathBuf {
        // Join base dir to the chain ID
        base_dir.as_ref().join(chain_id.to_string()).join(FILENAME)
    }
}

pub const VALUE_AFTER_TABLE_ERROR_MSG: &str = r#"
Error while serializing to toml. It means that some nested structure is followed
 by simple fields.
This fails:
    struct Nested{
       i:int
    }

    struct Broken{
       nested:Nested,
       simple:int
    }
And this is correct
    struct Nested{
       i:int
    }

    struct Correct{
       simple:int
       nested:Nested,
    }
"#;
