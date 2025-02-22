//! The ledger shell connects the ABCI++ interface with the Namada ledger app.
//!
//! Any changes applied before [`Shell::finalize_block`] might have to be
//! reverted, so any changes applied in the methods [`Shell::prepare_proposal`]
//! and [`Shell::process_proposal`] must be also reverted
//! (unless we can simply overwrite them in the next block).
//! More info in <https://github.com/anoma/namada/issues/362>.
mod block_space_alloc;
mod finalize_block;
mod governance;
mod init_chain;
mod prepare_proposal;
mod process_proposal;
mod queries;
mod stats;

use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::mem;
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::rc::Rc;

use borsh::{BorshDeserialize, BorshSerialize};
use namada::ledger::events::log::EventLog;
use namada::ledger::events::Event;
use namada::ledger::gas::BlockGasMeter;
use namada::ledger::pos::namada_proof_of_stake::types::{
    ConsensusValidator, ValidatorSetUpdate,
};
use namada::ledger::storage::write_log::WriteLog;
use namada::ledger::storage::{
    DBIter, Sha256Hasher, Storage, StorageHasher, WlStorage, DB,
};
use namada::ledger::storage_api::{self, StorageRead};
use namada::ledger::{ibc, pos, protocol, replay_protection};
use namada::proof_of_stake::{self, read_pos_params, slash};
use namada::proto::{self, Tx};
use namada::types::address::{masp, masp_tx_key, Address};
use namada::types::chain::ChainId;
use namada::types::internal::WrapperTxInQueue;
use namada::types::key::*;
use namada::types::storage::{BlockHeight, Key, TxIndex};
use namada::types::time::{DateTimeUtc, TimeZone, Utc};
use namada::types::token::{self};
#[cfg(not(feature = "mainnet"))]
use namada::types::transaction::MIN_FEE;
use namada::types::transaction::{
    hash_tx, process_tx, verify_decrypted_correctly, AffineCurve, DecryptedTx,
    EllipticCurve, PairingEngine, TxType,
};
use namada::types::{address, hash};
use namada::vm::wasm::{TxCache, VpCache};
use namada::vm::WasmCacheRwAccess;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::{genesis, TendermintMode};
#[cfg(feature = "abcipp")]
use crate::facade::tendermint_proto::abci::response_verify_vote_extension::VerifyStatus;
use crate::facade::tendermint_proto::abci::{
    Misbehavior as Evidence, MisbehaviorType as EvidenceType, ValidatorUpdate,
};
use crate::facade::tendermint_proto::crypto::public_key;
use crate::facade::tendermint_proto::google::protobuf::Timestamp;
use crate::facade::tower_abci::{request, response};
use crate::node::ledger::shims::abcipp_shim_types::shim;
use crate::node::ledger::shims::abcipp_shim_types::shim::response::TxResult;
use crate::node::ledger::{storage, tendermint_node};
#[allow(unused_imports)]
use crate::wallet::ValidatorData;
use crate::{config, wallet};

fn key_to_tendermint(
    pk: &common::PublicKey,
) -> std::result::Result<public_key::Sum, ParsePublicKeyError> {
    match pk {
        common::PublicKey::Ed25519(_) => ed25519::PublicKey::try_from_pk(pk)
            .map(|pk| public_key::Sum::Ed25519(pk.try_to_vec().unwrap())),
        common::PublicKey::Secp256k1(_) => {
            secp256k1::PublicKey::try_from_pk(pk)
                .map(|pk| public_key::Sum::Secp256k1(pk.try_to_vec().unwrap()))
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error removing the DB data: {0}")]
    RemoveDB(std::io::Error),
    #[error("chain ID mismatch: {0}")]
    ChainId(String),
    #[error("Error decoding a transaction from bytes: {0}")]
    TxDecoding(proto::Error),
    #[error("Error trying to apply a transaction: {0}")]
    TxApply(protocol::Error),
    #[error("Gas limit exceeding while applying transactions in block")]
    GasOverflow,
    #[error("{0}")]
    Tendermint(tendermint_node::Error),
    #[error("Server error: {0}")]
    TowerServer(String),
    #[error("{0}")]
    Broadcaster(tokio::sync::mpsc::error::TryRecvError),
    #[error("Error executing proposal {0}: {1}")]
    BadProposal(u64, String),
    #[error("Error reading wasm: {0}")]
    ReadingWasm(#[from] eyre::Error),
    #[error("Error loading wasm: {0}")]
    LoadingWasm(String),
    #[error("Error reading from or writing to storage: {0}")]
    StorageApi(#[from] storage_api::Error),
}

impl From<Error> for TxResult {
    fn from(err: Error) -> Self {
        TxResult {
            code: 1,
            info: err.to_string(),
        }
    }
}

/// The different error codes that the ledger may
/// send back to a client indicating the status
/// of their submitted tx
#[derive(Debug, Copy, Clone, FromPrimitive, ToPrimitive, PartialEq)]
pub enum ErrorCodes {
    Ok = 0,
    InvalidDecryptedChainId = 1,
    ExpiredDecryptedTx = 2,
    WasmRuntimeError = 3,
    InvalidTx = 4,
    InvalidSig = 5,
    InvalidOrder = 6,
    ExtraTxs = 7,
    Undecryptable = 8,
    AllocationError = 9,
    ReplayTx = 10,
    InvalidChainId = 11,
    ExpiredTx = 12,
}

impl ErrorCodes {
    /// Checks if the given [`ErrorCodes`] value is a protocol level error,
    /// that can be recovered from at the finalize block stage.
    pub const fn is_recoverable(&self) -> bool {
        use ErrorCodes::*;
        // NOTE: pattern match on all `ErrorCodes` variants, in order
        // to catch potential bugs when adding new codes
        match self {
            Ok
            | InvalidDecryptedChainId
            | ExpiredDecryptedTx
            | WasmRuntimeError => true,
            InvalidTx | InvalidSig | InvalidOrder | ExtraTxs
            | Undecryptable | AllocationError | ReplayTx | InvalidChainId
            | ExpiredTx => false,
        }
    }
}

impl From<ErrorCodes> for u32 {
    fn from(code: ErrorCodes) -> u32 {
        code.to_u32().unwrap()
    }
}

impl From<ErrorCodes> for String {
    fn from(code: ErrorCodes) -> String {
        u32::from(code).to_string()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn reset(config: config::Ledger) -> Result<()> {
    // simply nuke the DB files
    let db_path = &config.db_dir();
    match std::fs::remove_dir_all(db_path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        res => res.map_err(Error::RemoveDB)?,
    };
    // reset Tendermint state
    tendermint_node::reset(config.tendermint_dir())
        .map_err(Error::Tendermint)?;
    Ok(())
}

pub fn rollback(config: config::Ledger) -> Result<()> {
    // Rollback Tendermint state
    tracing::info!("Rollback Tendermint state");
    let tendermint_block_height =
        tendermint_node::rollback(config.tendermint_dir())
            .map_err(Error::Tendermint)?;

    // Rollback Namada state
    let db_path = config.shell.db_dir(&config.chain_id);
    let mut db = storage::PersistentDB::open(db_path, None);
    tracing::info!("Rollback Namada state");

    db.rollback(tendermint_block_height)
        .map_err(|e| Error::StorageApi(storage_api::Error::new(e)))
}

#[derive(Debug)]
#[allow(dead_code, clippy::large_enum_variant)]
pub(super) enum ShellMode {
    Validator {
        data: ValidatorData,
        broadcast_sender: UnboundedSender<Vec<u8>>,
    },
    Full,
    Seed,
}

#[allow(dead_code)]
impl ShellMode {
    /// Get the validator address if ledger is in validator mode
    pub fn get_validator_address(&self) -> Option<&address::Address> {
        match &self {
            ShellMode::Validator { data, .. } => Some(&data.address),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum MempoolTxType {
    /// A transaction that has not been validated by this node before
    NewTransaction,
    /// A transaction that has been validated at some previous level that may
    /// need to be validated again
    RecheckTransaction,
}

#[derive(Debug)]
pub struct Shell<D = storage::PersistentDB, H = Sha256Hasher>
where
    D: DB + for<'iter> DBIter<'iter> + Sync + 'static,
    H: StorageHasher + Sync + 'static,
{
    /// The id of the current chain
    #[allow(dead_code)]
    chain_id: ChainId,
    /// The persistent storage with write log
    pub(super) wl_storage: WlStorage<D, H>,
    /// Gas meter for the current block
    gas_meter: BlockGasMeter,
    /// Byzantine validators given from ABCI++ `prepare_proposal` are stored in
    /// this field. They will be slashed when we finalize the block.
    byzantine_validators: Vec<Evidence>,
    /// Path to the base directory with DB data and configs
    #[allow(dead_code)]
    base_dir: PathBuf,
    /// Path to the WASM directory for files used in the genesis block.
    wasm_dir: PathBuf,
    /// Information about the running shell instance
    #[allow(dead_code)]
    mode: ShellMode,
    /// VP WASM compilation cache
    vp_wasm_cache: VpCache<WasmCacheRwAccess>,
    /// Tx WASM compilation cache
    tx_wasm_cache: TxCache<WasmCacheRwAccess>,
    /// Taken from config `storage_read_past_height_limit`. When set, will
    /// limit the how many block heights in the past can the storage be
    /// queried for reading values.
    storage_read_past_height_limit: Option<u64>,
    /// Proposal execution tracking
    pub proposal_data: HashSet<u64>,
    /// Log of events emitted by `FinalizeBlock` ABCI calls.
    event_log: EventLog,
}

impl<D, H> Shell<D, H>
where
    D: DB + for<'iter> DBIter<'iter> + Sync + 'static,
    H: StorageHasher + Sync + 'static,
{
    /// Create a new shell from a path to a database and a chain id. Looks
    /// up the database with this data and tries to load the last state.
    pub fn new(
        config: config::Ledger,
        wasm_dir: PathBuf,
        broadcast_sender: UnboundedSender<Vec<u8>>,
        db_cache: Option<&D::Cache>,
        vp_wasm_compilation_cache: u64,
        tx_wasm_compilation_cache: u64,
        native_token: Address,
    ) -> Self {
        let chain_id = config.chain_id;
        let db_path = config.shell.db_dir(&chain_id);
        let base_dir = config.shell.base_dir;
        let mode = config.tendermint.tendermint_mode;
        let storage_read_past_height_limit =
            config.shell.storage_read_past_height_limit;
        if !Path::new(&base_dir).is_dir() {
            std::fs::create_dir(&base_dir)
                .expect("Creating directory for Namada should not fail");
        }
        // load last state from storage
        let mut storage = Storage::open(
            db_path,
            chain_id.clone(),
            native_token,
            db_cache,
            config.shell.storage_read_past_height_limit,
        );
        storage
            .load_last_state()
            .map_err(|e| {
                tracing::error!("Cannot load the last state from the DB {}", e);
            })
            .expect("PersistentStorage cannot be initialized");

        let vp_wasm_cache_dir =
            base_dir.join(chain_id.as_str()).join("vp_wasm_cache");
        let tx_wasm_cache_dir =
            base_dir.join(chain_id.as_str()).join("tx_wasm_cache");
        // load in keys and address from wallet if mode is set to `Validator`
        let mode = match mode {
            TendermintMode::Validator => {
                #[cfg(not(feature = "dev"))]
                {
                    let wallet_path = &base_dir.join(chain_id.as_str());
                    let genesis_path =
                        &base_dir.join(format!("{}.toml", chain_id.as_str()));
                    tracing::debug!(
                        "{}",
                        wallet_path.as_path().to_str().unwrap()
                    );
                    let wallet = wallet::Wallet::load_or_new_from_genesis(
                        wallet_path,
                        genesis::genesis_config::open_genesis_config(
                            genesis_path,
                        )
                        .unwrap(),
                    );
                    wallet
                        .take_validator_data()
                        .map(|data| ShellMode::Validator {
                            data,
                            broadcast_sender,
                        })
                        .expect(
                            "Validator data should have been stored in the \
                             wallet",
                        )
                }
                #[cfg(feature = "dev")]
                {
                    let validator_keys = wallet::defaults::validator_keys();
                    ShellMode::Validator {
                        data: wallet::ValidatorData {
                            address: wallet::defaults::validator_address(),
                            keys: wallet::ValidatorKeys {
                                protocol_keypair: validator_keys.0,
                                dkg_keypair: Some(validator_keys.1),
                            },
                        },
                        broadcast_sender,
                    }
                }
            }
            TendermintMode::Full => ShellMode::Full,
            TendermintMode::Seed => ShellMode::Seed,
        };

        let wl_storage = WlStorage {
            storage,
            write_log: WriteLog::default(),
        };
        Self {
            chain_id,
            wl_storage,
            gas_meter: BlockGasMeter::default(),
            byzantine_validators: vec![],
            base_dir,
            wasm_dir,
            mode,
            vp_wasm_cache: VpCache::new(
                vp_wasm_cache_dir,
                vp_wasm_compilation_cache as usize,
            ),
            tx_wasm_cache: TxCache::new(
                tx_wasm_cache_dir,
                tx_wasm_compilation_cache as usize,
            ),
            storage_read_past_height_limit,
            proposal_data: HashSet::new(),
            // TODO: config event log params
            event_log: EventLog::default(),
        }
    }

    /// Return a reference to the [`EventLog`].
    #[inline]
    pub fn event_log(&self) -> &EventLog {
        &self.event_log
    }

    /// Return a mutable reference to the [`EventLog`].
    #[inline]
    pub fn event_log_mut(&mut self) -> &mut EventLog {
        &mut self.event_log
    }

    /// Iterate over the wrapper txs in order
    #[allow(dead_code)]
    fn iter_tx_queue(&mut self) -> impl Iterator<Item = &WrapperTxInQueue> {
        self.wl_storage.storage.tx_queue.iter()
    }

    /// Load the Merkle root hash and the height of the last committed block, if
    /// any. This is returned when ABCI sends an `info` request.
    pub fn last_state(&mut self) -> response::Info {
        let mut response = response::Info::default();
        let result = self.wl_storage.storage.get_state();

        match result {
            Some((root, height)) => {
                tracing::info!(
                    "Last state root hash: {}, height: {}",
                    root,
                    height
                );
                response.last_block_app_hash = root.0;
                response.last_block_height =
                    height.try_into().expect("Invalid block height");
            }
            None => {
                tracing::info!(
                    "No state could be found, chain is not initialized"
                );
            }
        };

        response
    }

    /// Takes the optional tendermint timestamp of the block: if it's Some than
    /// converts it to a [`DateTimeUtc`], otherwise retrieve from self the
    /// time of the last block committed
    pub fn get_block_timestamp(
        &self,
        tendermint_block_time: Option<Timestamp>,
    ) -> DateTimeUtc {
        if let Some(t) = tendermint_block_time {
            if let Ok(t) = t.try_into() {
                return t;
            }
        }
        // Default to last committed block time
        self.wl_storage
            .storage
            .get_last_block_timestamp()
            .expect("Failed to retrieve last block timestamp")
    }

    /// Read the value for a storage key dropping any error
    pub fn read_storage_key<T>(&self, key: &Key) -> Option<T>
    where
        T: Clone + BorshDeserialize,
    {
        let result = self.wl_storage.storage.read(key);

        match result {
            Ok((bytes, _gas)) => match bytes {
                Some(bytes) => match T::try_from_slice(&bytes) {
                    Ok(value) => Some(value),
                    Err(_) => None,
                },
                None => None,
            },
            Err(_) => None,
        }
    }

    /// Read the bytes for a storage key dropping any error
    pub fn read_storage_key_bytes(&self, key: &Key) -> Option<Vec<u8>> {
        let result = self.wl_storage.storage.read(key);

        match result {
            Ok((bytes, _gas)) => bytes,
            Err(_) => None,
        }
    }

    /// Apply PoS slashes from the evidence
    fn slash(&mut self) {
        if !self.byzantine_validators.is_empty() {
            let byzantine_validators =
                mem::take(&mut self.byzantine_validators);
            // TODO: resolve this unwrap() better
            let pos_params = read_pos_params(&self.wl_storage).unwrap();
            let current_epoch = self.wl_storage.storage.block.epoch;
            for evidence in byzantine_validators {
                tracing::info!("Processing evidence {evidence:?}.");
                let evidence_height = match u64::try_from(evidence.height) {
                    Ok(height) => height,
                    Err(err) => {
                        tracing::error!(
                            "Unexpected evidence block height {}",
                            err
                        );
                        continue;
                    }
                };
                let evidence_epoch = match self
                    .wl_storage
                    .storage
                    .block
                    .pred_epochs
                    .get_epoch(BlockHeight(evidence_height))
                {
                    Some(epoch) => epoch,
                    None => {
                        tracing::error!(
                            "Couldn't find epoch for evidence block height {}",
                            evidence_height
                        );
                        continue;
                    }
                };
                if evidence_epoch + pos_params.unbonding_len <= current_epoch {
                    tracing::info!(
                        "Skipping outdated evidence from epoch \
                         {evidence_epoch}"
                    );
                    continue;
                }
                let slash_type = match EvidenceType::from_i32(evidence.r#type) {
                    Some(r#type) => match r#type {
                        EvidenceType::DuplicateVote => {
                            pos::types::SlashType::DuplicateVote
                        }
                        EvidenceType::LightClientAttack => {
                            pos::types::SlashType::LightClientAttack
                        }
                        EvidenceType::Unknown => {
                            tracing::error!(
                                "Unknown evidence: {:#?}",
                                evidence
                            );
                            continue;
                        }
                    },
                    None => {
                        tracing::error!(
                            "Unexpected evidence type {}",
                            evidence.r#type
                        );
                        continue;
                    }
                };
                let validator_raw_hash = match evidence.validator {
                    Some(validator) => tm_raw_hash_to_string(validator.address),
                    None => {
                        tracing::error!(
                            "Evidence without a validator {:#?}",
                            evidence
                        );
                        continue;
                    }
                };
                let validator =
                    match proof_of_stake::find_validator_by_raw_hash(
                        &self.wl_storage,
                        &validator_raw_hash,
                    )
                    .expect("Must be able to read storage")
                    {
                        Some(validator) => validator,
                        None => {
                            tracing::error!(
                                "Cannot find validator's address from raw \
                                 hash {}",
                                validator_raw_hash
                            );
                            continue;
                        }
                    };
                tracing::info!(
                    "Slashing {} for {} in epoch {}, block height {}",
                    validator,
                    slash_type,
                    evidence_epoch,
                    evidence_height
                );
                if let Err(err) = slash(
                    &mut self.wl_storage,
                    &pos_params,
                    current_epoch,
                    evidence_epoch,
                    evidence_height,
                    slash_type,
                    &validator,
                ) {
                    tracing::error!("Error in slashing: {}", err);
                }
            }
        }
    }

    /// INVARIANT: This method must be stateless.
    #[cfg(feature = "abcipp")]
    pub fn extend_vote(
        &self,
        _req: request::ExtendVote,
    ) -> response::ExtendVote {
        Default::default()
    }

    /// INVARIANT: This method must be stateless.
    #[cfg(feature = "abcipp")]
    pub fn verify_vote_extension(
        &self,
        _req: request::VerifyVoteExtension,
    ) -> response::VerifyVoteExtension {
        response::VerifyVoteExtension {
            status: VerifyStatus::Accept as i32,
        }
    }

    /// Commit a block. Persist the application state and return the Merkle root
    /// hash.
    pub fn commit(&mut self) -> response::Commit {
        let mut response = response::Commit::default();
        // commit block's data from write log and store the in DB
        self.wl_storage.commit_block().unwrap_or_else(|e| {
            tracing::error!(
                "Encountered a storage error while committing a block {:?}",
                e
            )
        });

        let root = self.wl_storage.storage.merkle_root();
        tracing::info!(
            "Committed block hash: {}, height: {}",
            root,
            self.wl_storage.storage.last_height,
        );
        response.data = root.0;
        response
    }

    /// Validate a transaction request. On success, the transaction will
    /// included in the mempool and propagated to peers, otherwise it will be
    /// rejected.
    ///
    /// Error codes:
    ///    0: Ok
    ///    1: Invalid tx
    ///    2: Tx is invalidly signed
    ///    7: Replay attack
    ///    8: Invalid chain id in tx
    pub fn mempool_validate(
        &self,
        tx_bytes: &[u8],
        r#_type: MempoolTxType,
    ) -> response::CheckTx {
        let mut response = response::CheckTx::default();

        // Tx format check
        let tx = match Tx::try_from(tx_bytes).map_err(Error::TxDecoding) {
            Ok(t) => t,
            Err(msg) => {
                response.code = ErrorCodes::InvalidTx.into();
                response.log = msg.to_string();
                return response;
            }
        };

        // Tx chain id
        if tx.chain_id != self.chain_id {
            response.code = ErrorCodes::InvalidChainId.into();
            response.log = format!(
                "Tx carries a wrong chain id: expected {}, found {}",
                self.chain_id, tx.chain_id
            );
            return response;
        }

        // Tx expiration
        if let Some(exp) = tx.expiration {
            let last_block_timestamp = self.get_block_timestamp(None);

            if last_block_timestamp > exp {
                response.code = ErrorCodes::ExpiredTx.into();
                response.log = format!(
                    "Tx expired at {:#?}, last committed block time: {:#?}",
                    exp, last_block_timestamp
                );
                return response;
            }
        }

        // Tx signature check
        let tx_type = match process_tx(tx) {
            Ok(ty) => ty,
            Err(msg) => {
                response.code = ErrorCodes::InvalidSig.into();
                response.log = msg.to_string();
                return response;
            }
        };

        // Tx type check
        if let TxType::Wrapper(wrapper) = tx_type {
            // Replay protection check
            let inner_hash_key =
                replay_protection::get_tx_hash_key(&wrapper.tx_hash);
            if self
                .wl_storage
                .storage
                .has_key(&inner_hash_key)
                .expect("Error while checking inner tx hash key in storage")
                .0
            {
                response.code = ErrorCodes::ReplayTx.into();
                response.log = format!(
                    "Inner transaction hash {} already in storage, replay \
                     attempt",
                    wrapper.tx_hash
                );
                return response;
            }

            let tx =
                Tx::try_from(tx_bytes).expect("Deserialization shouldn't fail");
            let wrapper_hash = hash::Hash(tx.unsigned_hash());
            let wrapper_hash_key =
                replay_protection::get_tx_hash_key(&wrapper_hash);
            if self
                .wl_storage
                .storage
                .has_key(&wrapper_hash_key)
                .expect("Error while checking wrapper tx hash key in storage")
                .0
            {
                response.code = ErrorCodes::ReplayTx.into();
                response.log = format!(
                    "Wrapper transaction hash {} already in storage, replay \
                     attempt",
                    wrapper_hash
                );
                return response;
            }

            // Check balance for fee
            let fee_payer = if wrapper.pk != masp_tx_key().ref_to() {
                wrapper.fee_payer()
            } else {
                masp()
            };
            // check that the fee payer has sufficient balance
            let balance = self.get_balance(&wrapper.fee.token, &fee_payer);

            // In testnets with a faucet, tx is allowed to skip fees if
            // it includes a valid PoW
            #[cfg(not(feature = "mainnet"))]
            let has_valid_pow = self.has_valid_pow_solution(&wrapper);
            #[cfg(feature = "mainnet")]
            let has_valid_pow = false;

            if !has_valid_pow && self.get_wrapper_tx_fees() > balance {
                response.code = ErrorCodes::InvalidTx.into();
                response.log = String::from(
                    "The given address does not have a sufficient balance to \
                     pay fee",
                );
                return response;
            }
        } else {
            response.code = ErrorCodes::InvalidTx.into();
            response.log = "Unsupported tx type".to_string();
            return response;
        }

        response.log = "Mempool validation passed".to_string();

        response
    }

    #[allow(dead_code)]
    /// Simulate validation and application of a transaction.
    fn dry_run_tx(&self, tx_bytes: &[u8]) -> response::Query {
        let mut response = response::Query::default();
        let mut gas_meter = BlockGasMeter::default();
        let mut write_log = WriteLog::default();
        let mut vp_wasm_cache = self.vp_wasm_cache.read_only();
        let mut tx_wasm_cache = self.tx_wasm_cache.read_only();
        match Tx::try_from(tx_bytes) {
            Ok(tx) => {
                let tx = TxType::Decrypted(DecryptedTx::Decrypted {
                    tx,
                    #[cfg(not(feature = "mainnet"))]
                    // To be able to dry-run testnet faucet withdrawal, pretend 
                    // that we got a valid PoW
                    has_valid_pow: true,
                });
                match protocol::apply_tx(
                    tx,
                    tx_bytes.len(),
                    TxIndex::default(),
                    &mut gas_meter,
                    &mut write_log,
                    &self.wl_storage.storage,
                    &mut vp_wasm_cache,
                    &mut tx_wasm_cache,
                )
                .map_err(Error::TxApply)
                {
                    Ok(result) => response.info = result.to_string(),
                    Err(error) => {
                        response.code = 1;
                        response.log = format!("{}", error);
                    }
                }
                response
            }
            Err(err) => {
                response.code = 1;
                response.log = format!("{}", Error::TxDecoding(err));
                response
            }
        }
    }

    /// Lookup a validator's keypair for their established account from their
    /// wallet. If the node is not validator, this function returns None
    #[allow(dead_code)]
    fn get_account_keypair(&self) -> Option<common::SecretKey> {
        let wallet_path = &self.base_dir.join(self.chain_id.as_str());
        let genesis_path = &self
            .base_dir
            .join(format!("{}.toml", self.chain_id.as_str()));
        let mut wallet = wallet::Wallet::load_or_new_from_genesis(
            wallet_path,
            genesis::genesis_config::open_genesis_config(genesis_path).unwrap(),
        );
        self.mode.get_validator_address().map(|addr| {
            let sk: common::SecretKey = self
                .wl_storage
                .read(&pk_key(addr))
                .expect(
                    "A validator should have a public key associated with \
                     it's established account",
                )
                .expect(
                    "A validator should have a public key associated with \
                     it's established account",
                );
            let pk = sk.ref_to();
            wallet.find_key_by_pk(&pk).expect(
                "A validator's established keypair should be stored in its \
                 wallet",
            )
        })
    }

    #[cfg(not(feature = "mainnet"))]
    /// Check if the tx has a valid PoW solution. Unlike
    /// `apply_pow_solution_if_valid`, this won't invalidate the solution.
    fn has_valid_pow_solution(
        &self,
        tx: &namada::types::transaction::WrapperTx,
    ) -> bool {
        if let Some(solution) = &tx.pow_solution {
            if let Some(faucet_address) =
                namada::ledger::parameters::read_faucet_account_parameter(
                    &self.wl_storage,
                )
                .expect("Must be able to read faucet account parameter")
            {
                let source = Address::from(&tx.pk);
                return solution
                    .validate(&self.wl_storage, &faucet_address, source)
                    .expect("Must be able to validate PoW solutions");
            }
        }
        false
    }

    #[cfg(not(feature = "mainnet"))]
    /// Get fixed amount of fees for wrapper tx
    fn get_wrapper_tx_fees(&self) -> token::Amount {
        let fees = namada::ledger::parameters::read_wrapper_tx_fees_parameter(
            &self.wl_storage,
        )
        .expect("Must be able to read wrapper tx fees parameter");
        fees.unwrap_or(token::Amount::whole(MIN_FEE))
    }

    #[cfg(not(feature = "mainnet"))]
    /// Check if the tx has a valid PoW solution and if so invalidate it to
    /// prevent replay.
    fn invalidate_pow_solution_if_valid(
        &mut self,
        tx: &namada::types::transaction::WrapperTx,
    ) -> bool {
        if let Some(solution) = &tx.pow_solution {
            if let Some(faucet_address) =
                namada::ledger::parameters::read_faucet_account_parameter(
                    &self.wl_storage,
                )
                .expect("Must be able to read faucet account parameter")
            {
                let source = Address::from(&tx.pk);
                return solution
                    .invalidate_if_valid(
                        &mut self.wl_storage,
                        &faucet_address,
                        &source,
                    )
                    .expect("Must be able to validate PoW solutions");
            }
        }
        false
    }
}

/// Helper functions and types for writing unit tests
/// for the shell
#[cfg(test)]
mod test_utils {
    use std::ops::{Deref, DerefMut};
    use std::path::PathBuf;

    use namada::ledger::storage::mockdb::MockDB;
    use namada::ledger::storage::{update_allowed_conversions, Sha256Hasher};
    use namada::types::chain::ChainId;
    use namada::types::hash::Hash;
    use namada::types::key::*;
    use namada::types::storage::{BlockHash, Epoch, Epochs, Header};
    use namada::types::transaction::{Fee, WrapperTx};
    use tempfile::tempdir;
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;
    use crate::facade::tendermint_proto::abci::{
        RequestInitChain, RequestProcessProposal,
    };
    use crate::facade::tendermint_proto::google::protobuf::Timestamp;
    use crate::node::ledger::shims::abcipp_shim_types::shim::request::{
        FinalizeBlock, ProcessedTx,
    };
    use crate::node::ledger::storage::{PersistentDB, PersistentStorageHasher};

    #[derive(Error, Debug)]
    pub enum TestError {
        #[error("Proposal rejected with tx results: {0:?}")]
        #[allow(dead_code)]
        RejectProposal(Vec<ProcessedTx>),
    }

    /// Gets the absolute path to root directory
    pub fn top_level_directory() -> PathBuf {
        let mut current_path = std::env::current_dir()
            .expect("Current directory should exist")
            .canonicalize()
            .expect("Current directory should exist");
        while current_path.file_name().unwrap() != "apps" {
            current_path.pop();
        }
        current_path.pop();
        current_path
    }

    /// Generate a random public/private keypair
    pub(super) fn gen_keypair() -> common::SecretKey {
        use rand::prelude::ThreadRng;
        use rand::thread_rng;

        let mut rng: ThreadRng = thread_rng();
        ed25519::SigScheme::generate(&mut rng).try_to_sk().unwrap()
    }

    /// A wrapper around the shell that implements
    /// Drop so as to clean up the files that it
    /// generates. Also allows illegal state
    /// modifications for testing purposes
    pub(super) struct TestShell {
        pub shell: Shell<MockDB, Sha256Hasher>,
    }

    impl Deref for TestShell {
        type Target = Shell<MockDB, Sha256Hasher>;

        fn deref(&self) -> &Self::Target {
            &self.shell
        }
    }

    impl DerefMut for TestShell {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.shell
        }
    }

    #[derive(Clone)]
    /// Helper for testing process proposal which has very different
    /// input types depending on whether the ABCI++ feature is on or not.
    pub struct ProcessProposal {
        pub txs: Vec<Vec<u8>>,
    }

    impl TestShell {
        /// Returns a new shell paired with a broadcast receiver, which will
        /// receives any protocol txs sent by the shell.
        pub fn new() -> (Self, UnboundedReceiver<Vec<u8>>) {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let base_dir = tempdir().unwrap().as_ref().canonicalize().unwrap();
            let vp_wasm_compilation_cache = 50 * 1024 * 1024; // 50 kiB
            let tx_wasm_compilation_cache = 50 * 1024 * 1024; // 50 kiB
            (
                Self {
                    shell: Shell::<MockDB, Sha256Hasher>::new(
                        config::Ledger::new(
                            base_dir,
                            Default::default(),
                            TendermintMode::Validator,
                        ),
                        top_level_directory().join("wasm"),
                        sender,
                        None,
                        vp_wasm_compilation_cache,
                        tx_wasm_compilation_cache,
                        address::nam(),
                    ),
                },
                receiver,
            )
        }

        /// Forward a InitChain request and expect a success
        pub fn init_chain(
            &mut self,
            req: RequestInitChain,
            #[cfg(feature = "dev")] num_validators: u64,
        ) {
            self.shell
                .init_chain(req, num_validators)
                .expect("Test shell failed to initialize");
        }

        /// Forward a ProcessProposal request and extract the relevant
        /// response data to return
        pub fn process_proposal(
            &mut self,
            req: ProcessProposal,
        ) -> std::result::Result<Vec<ProcessedTx>, TestError> {
            let resp = self.shell.process_proposal(RequestProcessProposal {
                txs: req.txs.clone(),
                ..Default::default()
            });
            let results = resp
                .tx_results
                .into_iter()
                .zip(req.txs.into_iter())
                .map(|(res, tx_bytes)| ProcessedTx {
                    result: res,
                    tx: tx_bytes,
                })
                .collect();
            if resp.status != 1 {
                Err(TestError::RejectProposal(results))
            } else {
                Ok(results)
            }
        }

        /// Forward a FinalizeBlock request return a vector of
        /// the events created for each transaction
        pub fn finalize_block(
            &mut self,
            req: FinalizeBlock,
        ) -> Result<Vec<Event>> {
            match self.shell.finalize_block(req) {
                Ok(resp) => Ok(resp.events),
                Err(err) => Err(err),
            }
        }

        /// Add a wrapper tx to the queue of txs to be decrypted
        /// in the current block proposal
        #[cfg(test)]
        pub fn enqueue_tx(&mut self, wrapper: WrapperTx) {
            self.shell
                .wl_storage
                .storage
                .tx_queue
                .push(WrapperTxInQueue {
                    tx: wrapper,
                    #[cfg(not(feature = "mainnet"))]
                    has_valid_pow: false,
                });
        }
    }

    /// Start a new test shell and initialize it. Returns the shell paired with
    /// a broadcast receiver, which will receives any protocol txs sent by the
    /// shell.
    pub(super) fn setup(
        num_validators: u64,
    ) -> (TestShell, UnboundedReceiver<Vec<u8>>) {
        let (mut test, receiver) = TestShell::new();
        test.init_chain(
            RequestInitChain {
                time: Some(Timestamp {
                    seconds: 0,
                    nanos: 0,
                }),
                chain_id: ChainId::default().to_string(),
                ..Default::default()
            },
            num_validators,
        );
        (test, receiver)
    }

    /// This is just to be used in testing. It is not
    /// a meaningful default.
    impl Default for FinalizeBlock {
        fn default() -> Self {
            FinalizeBlock {
                hash: BlockHash([0u8; 32]),
                header: Header {
                    hash: Hash([0; 32]),
                    time: DateTimeUtc::now(),
                    next_validators_hash: Hash([0; 32]),
                },
                byzantine_validators: vec![],
                txs: vec![],
                proposer_address: vec![],
                votes: vec![],
            }
        }
    }

    /// We test that on shell shutdown, the tx queue gets persisted in a DB, and
    /// on startup it is read successfully
    #[test]
    fn test_tx_queue_persistence() {
        let base_dir = tempdir().unwrap().as_ref().canonicalize().unwrap();
        // we have to use RocksDB for this test
        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        let vp_wasm_compilation_cache = 50 * 1024 * 1024; // 50 kiB
        let tx_wasm_compilation_cache = 50 * 1024 * 1024; // 50 kiB
        let native_token = address::nam();
        let mut shell = Shell::<PersistentDB, PersistentStorageHasher>::new(
            config::Ledger::new(
                base_dir.clone(),
                Default::default(),
                TendermintMode::Validator,
            ),
            top_level_directory().join("wasm"),
            sender.clone(),
            None,
            vp_wasm_compilation_cache,
            tx_wasm_compilation_cache,
            native_token.clone(),
        );
        shell
            .wl_storage
            .storage
            .begin_block(BlockHash::default(), BlockHeight(1))
            .expect("begin_block failed");
        let keypair = gen_keypair();
        // enqueue a wrapper tx
        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            Some("transaction data".as_bytes().to_owned()),
            shell.chain_id.clone(),
            None,
        );
        let wrapper = WrapperTx::new(
            Fee {
                amount: 0.into(),
                token: native_token,
            },
            &keypair,
            Epoch(0),
            0.into(),
            tx,
            Default::default(),
            #[cfg(not(feature = "mainnet"))]
            None,
        );
        shell.wl_storage.storage.tx_queue.push(WrapperTxInQueue {
            tx: wrapper,
            #[cfg(not(feature = "mainnet"))]
            has_valid_pow: false,
        });
        // Artificially increase the block height so that chain
        // will read the new block when restarted
        let mut pred_epochs: Epochs = Default::default();
        pred_epochs.new_epoch(BlockHeight(1), 1000);
        update_allowed_conversions(&mut shell.wl_storage)
            .expect("update conversions failed");
        shell.wl_storage.commit_block().expect("commit failed");

        // Drop the shell
        std::mem::drop(shell);

        // Reboot the shell and check that the queue was restored from DB
        let shell = Shell::<PersistentDB, PersistentStorageHasher>::new(
            config::Ledger::new(
                base_dir,
                Default::default(),
                TendermintMode::Validator,
            ),
            top_level_directory().join("wasm"),
            sender,
            None,
            vp_wasm_compilation_cache,
            tx_wasm_compilation_cache,
            address::nam(),
        );
        assert!(!shell.wl_storage.storage.tx_queue.is_empty());
    }
}

/// Test the failure cases of [`mempool_validate`]
#[cfg(test)]
mod test_mempool_validate {
    use namada::proof_of_stake::Epoch;
    use namada::proto::SignedTxData;
    use namada::types::transaction::{Fee, WrapperTx};

    use super::test_utils::TestShell;
    use super::{MempoolTxType, *};

    /// Mempool validation must reject unsigned wrappers
    #[test]
    fn test_missing_signature() {
        let (shell, _) = TestShell::new();

        let keypair = super::test_utils::gen_keypair();

        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            Some("transaction data".as_bytes().to_owned()),
            shell.chain_id.clone(),
            None,
        );

        let mut wrapper = WrapperTx::new(
            Fee {
                amount: 100.into(),
                token: shell.wl_storage.storage.native_token.clone(),
            },
            &keypair,
            Epoch(0),
            0.into(),
            tx,
            Default::default(),
            #[cfg(not(feature = "mainnet"))]
            None,
        )
        .sign(&keypair, shell.chain_id.clone(), None)
        .expect("Wrapper signing failed");

        let unsigned_wrapper = if let Some(Ok(SignedTxData {
            data: Some(data),
            sig: _,
        })) = wrapper
            .data
            .take()
            .map(|data| SignedTxData::try_from_slice(&data[..]))
        {
            Tx::new(vec![], Some(data), shell.chain_id.clone(), None)
        } else {
            panic!("Test failed")
        };

        let mut result = shell.mempool_validate(
            unsigned_wrapper.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::InvalidSig));
        result = shell.mempool_validate(
            unsigned_wrapper.to_bytes().as_ref(),
            MempoolTxType::RecheckTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::InvalidSig));
    }

    /// Mempool validation must reject wrappers with an invalid signature
    #[test]
    fn test_invalid_signature() {
        let (shell, _) = TestShell::new();

        let keypair = super::test_utils::gen_keypair();

        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            Some("transaction data".as_bytes().to_owned()),
            shell.chain_id.clone(),
            None,
        );

        let mut wrapper = WrapperTx::new(
            Fee {
                amount: 100.into(),
                token: shell.wl_storage.storage.native_token.clone(),
            },
            &keypair,
            Epoch(0),
            0.into(),
            tx,
            Default::default(),
            #[cfg(not(feature = "mainnet"))]
            None,
        )
        .sign(&keypair, shell.chain_id.clone(), None)
        .expect("Wrapper signing failed");

        let invalid_wrapper = if let Some(Ok(SignedTxData {
            data: Some(data),
            sig,
        })) = wrapper
            .data
            .take()
            .map(|data| SignedTxData::try_from_slice(&data[..]))
        {
            let mut new_wrapper = if let TxType::Wrapper(wrapper) =
                <TxType as BorshDeserialize>::deserialize(&mut data.as_ref())
                    .expect("Test failed")
            {
                wrapper
            } else {
                panic!("Test failed")
            };

            // we mount a malleability attack to try and remove the fee
            new_wrapper.fee.amount = 0.into();
            let new_data = TxType::Wrapper(new_wrapper)
                .try_to_vec()
                .expect("Test failed");
            Tx::new(
                vec![],
                Some(
                    SignedTxData {
                        sig,
                        data: Some(new_data),
                    }
                    .try_to_vec()
                    .expect("Test failed"),
                ),
                shell.chain_id.clone(),
                None,
            )
        } else {
            panic!("Test failed");
        };

        let mut result = shell.mempool_validate(
            invalid_wrapper.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::InvalidSig));
        result = shell.mempool_validate(
            invalid_wrapper.to_bytes().as_ref(),
            MempoolTxType::RecheckTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::InvalidSig));
    }

    /// Mempool validation must reject non-wrapper txs
    #[test]
    fn test_wrong_tx_type() {
        let (shell, _) = TestShell::new();

        // Test Raw TxType
        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            None,
            shell.chain_id.clone(),
            None,
        );

        let result = shell.mempool_validate(
            tx.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::InvalidTx));
        assert_eq!(result.log, "Unsupported tx type")
    }

    /// Mempool validation must reject already applied wrapper and decrypted
    /// transactions
    #[test]
    fn test_replay_attack() {
        let (mut shell, _) = TestShell::new();

        let keypair = super::test_utils::gen_keypair();

        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            Some("transaction data".as_bytes().to_owned()),
            shell.chain_id.clone(),
            None,
        );

        let wrapper = WrapperTx::new(
            Fee {
                amount: 100.into(),
                token: shell.wl_storage.storage.native_token.clone(),
            },
            &keypair,
            Epoch(0),
            0.into(),
            tx,
            Default::default(),
            #[cfg(not(feature = "mainnet"))]
            None,
        )
        .sign(&keypair, shell.chain_id.clone(), None)
        .expect("Wrapper signing failed");

        let tx_type = match process_tx(wrapper.clone()).expect("Test failed") {
            TxType::Wrapper(t) => t,
            _ => panic!("Test failed"),
        };

        // Write wrapper hash to storage
        let wrapper_hash = hash::Hash(wrapper.unsigned_hash());
        let wrapper_hash_key =
            replay_protection::get_tx_hash_key(&wrapper_hash);
        shell
            .wl_storage
            .storage
            .write(&wrapper_hash_key, &wrapper_hash)
            .expect("Test failed");

        // Try wrapper tx replay attack
        let result = shell.mempool_validate(
            wrapper.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::ReplayTx));
        assert_eq!(
            result.log,
            format!(
                "Wrapper transaction hash {} already in storage, replay \
                 attempt",
                wrapper_hash
            )
        );

        let result = shell.mempool_validate(
            wrapper.to_bytes().as_ref(),
            MempoolTxType::RecheckTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::ReplayTx));
        assert_eq!(
            result.log,
            format!(
                "Wrapper transaction hash {} already in storage, replay \
                 attempt",
                wrapper_hash
            )
        );

        // Write inner hash in storage
        let inner_hash_key =
            replay_protection::get_tx_hash_key(&tx_type.tx_hash);
        shell
            .wl_storage
            .storage
            .write(&inner_hash_key, &tx_type.tx_hash)
            .expect("Test failed");

        // Try inner tx replay attack
        let result = shell.mempool_validate(
            wrapper.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::ReplayTx));
        assert_eq!(
            result.log,
            format!(
                "Inner transaction hash {} already in storage, replay attempt",
                tx_type.tx_hash
            )
        );

        let result = shell.mempool_validate(
            wrapper.to_bytes().as_ref(),
            MempoolTxType::RecheckTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::ReplayTx));
        assert_eq!(
            result.log,
            format!(
                "Inner transaction hash {} already in storage, replay attempt",
                tx_type.tx_hash
            )
        )
    }

    /// Check that a transaction with a wrong chain id gets discarded
    #[test]
    fn test_wrong_chain_id() {
        let (shell, _) = TestShell::new();

        let keypair = super::test_utils::gen_keypair();

        let wrong_chain_id = ChainId("Wrong chain id".to_string());
        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            Some("transaction data".as_bytes().to_owned()),
            wrong_chain_id.clone(),
            None,
        )
        .sign(&keypair);

        let result = shell.mempool_validate(
            tx.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::InvalidChainId));
        assert_eq!(
            result.log,
            format!(
                "Tx carries a wrong chain id: expected {}, found {}",
                shell.chain_id, wrong_chain_id
            )
        )
    }

    /// Check that an expired transaction gets rejected
    #[test]
    fn test_expired_tx() {
        let (shell, _) = TestShell::new();

        let keypair = super::test_utils::gen_keypair();

        let tx = Tx::new(
            "wasm_code".as_bytes().to_owned(),
            Some("transaction data".as_bytes().to_owned()),
            shell.chain_id.clone(),
            Some(DateTimeUtc::now()),
        )
        .sign(&keypair);

        let result = shell.mempool_validate(
            tx.to_bytes().as_ref(),
            MempoolTxType::NewTransaction,
        );
        assert_eq!(result.code, u32::from(ErrorCodes::ExpiredTx));
    }
}
