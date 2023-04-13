//! The ledger's protocol
use std::collections::BTreeSet;
use std::panic;

use namada_core::types::transaction::GasLimit;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use thiserror::Error;

use crate::ledger::eth_bridge::vp::EthBridge;
use crate::ledger::gas::{self, BlockGasMeter, VpGasMeter};
use crate::ledger::ibc::vp::{Ibc, IbcToken};
use crate::ledger::native_vp::governance::GovernanceVp;
use crate::ledger::native_vp::parameters::{self, ParametersVp};
use crate::ledger::native_vp::replay_protection::ReplayProtectionVp;
use crate::ledger::native_vp::slash_fund::SlashFundVp;
use crate::ledger::native_vp::{self, NativeVp};
use crate::ledger::pos::{self, PosVP};
use crate::ledger::storage::write_log::WriteLog;
use crate::ledger::storage::{DBIter, Storage, StorageHasher, DB};
use crate::proto::{self, Tx};
use crate::types::address::{Address, InternalAddress};
use crate::types::storage;
use crate::types::storage::TxIndex;
use crate::types::transaction::{DecryptedTx, TxResult, TxType, VpsResult};
use crate::vm::wasm::{TxCache, VpCache};
use crate::vm::{self, wasm, WasmCacheAccess};

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum Error {
    #[error("Storage error: {0}")]
    StorageError(crate::ledger::storage::Error),
    #[error("Error decoding a transaction from bytes: {0}")]
    TxDecodingError(proto::Error),
    #[error("Transaction runner error: {0}")]
    TxRunnerError(vm::wasm::run::Error),
    #[error("Txs must either be encrypted or a decryption of an encrypted tx")]
    TxTypeError,
    #[error("Gas error: {0}")]
    GasError(gas::Error),
    #[error("Error executing VP for addresses: {0:?}")]
    VpRunnerError(vm::wasm::run::Error),
    #[error("The address {0} doesn't exist")]
    MissingAddress(Address),
    #[error("IBC native VP: {0}")]
    IbcNativeVpError(crate::ledger::ibc::vp::Error),
    #[error("PoS native VP: {0}")]
    PosNativeVpError(pos::vp::Error),
    #[error("PoS native VP panicked")]
    PosNativeVpRuntime,
    #[error("Parameters native VP: {0}")]
    ParametersNativeVpError(parameters::Error),
    #[error("IBC Token native VP: {0}")]
    IbcTokenNativeVpError(crate::ledger::ibc::vp::IbcTokenError),
    #[error("Governance native VP error: {0}")]
    GovernanceNativeVpError(crate::ledger::native_vp::governance::Error),
    #[error("SlashFund native VP error: {0}")]
    SlashFundNativeVpError(crate::ledger::native_vp::slash_fund::Error),
    #[error("Ethereum bridge native VP error: {0}")]
    EthBridgeNativeVpError(crate::ledger::eth_bridge::vp::Error),
    #[error("Replay protection native VP error: {0}")]
    ReplayProtectionNativeVpError(
        crate::ledger::native_vp::replay_protection::Error,
    ),
    #[error("Access to an internal address {0} is forbidden")]
    AccessForbidden(InternalAddress),
}

/// Result of applying a transaction
pub type Result<T> = std::result::Result<T, Error>;

/// Apply a given transaction
///
/// The only Tx Types that should be input here are `Decrypted` and `Wrapper`
///
/// If the given tx is a successfully decrypted payload apply the necessary
/// vps. Otherwise, we include the tx on chain with the gas charge added
/// but no further validations.
#[allow(clippy::too_many_arguments)]
pub fn apply_tx<D, H, CA>(
    tx: TxType,
    tx_length: usize,
    tx_index: TxIndex,
    block_gas_meter: &mut BlockGasMeter,
    write_log: &mut WriteLog,
    storage: &Storage<D, H>,
    vp_wasm_cache: &mut VpCache<CA>,
    tx_wasm_cache: &mut TxCache<CA>,
) -> Result<TxResult>
where
    D: 'static + DB + for<'iter> DBIter<'iter> + Sync,
    H: 'static + StorageHasher + Sync,
    CA: 'static + WasmCacheAccess + Sync,
{
    // Base gas cost for applying the tx
    block_gas_meter
        .add_base_transaction_fee(tx_length)
        .map_err(Error::GasError)?;
    match tx {
        TxType::Raw(_) => Err(Error::TxTypeError),
        TxType::Decrypted(DecryptedTx::Decrypted {
            tx,
            #[cfg(not(feature = "mainnet"))]
            has_valid_pow,
        }) => {
            //FIXME: how to get the hash of the transaction? Isn't it signed?
            let verifiers = execute_tx(
                &tx,
                &tx_index,
                storage,
                block_gas_meter,
                write_log,
                vp_wasm_cache,
                tx_wasm_cache,
            )?;

            let vps_result = check_vps(
                &tx,
                &tx_index,
                storage,
                block_gas_meter,
                write_log,
                &verifiers,
                vp_wasm_cache,
                #[cfg(not(feature = "mainnet"))]
                has_valid_pow,
            )?;

            let gas_used = block_gas_meter
                .finalize_transaction()
                .map_err(Error::GasError)?;
            let initialized_accounts = write_log.get_initialized_accounts();
            let changed_keys = write_log.get_keys();
            let ibc_event = write_log.take_ibc_event();

            Ok(TxResult {
                gas_used,
                changed_keys,
                vps_result,
                initialized_accounts,
                ibc_event,
            })
        }
        _ => {
            let gas_used = block_gas_meter
                .finalize_transaction()
                .map_err(Error::GasError)?;
            Ok(TxResult {
                gas_used,
                ..Default::default()
            })
        }
    }
}

/// Execute a transaction code. Returns verifiers requested by the transaction.
fn execute_tx<D, H, CA>(
    tx: &Tx,
    tx_index: &TxIndex,
    storage: &Storage<D, H>,
    gas_meter: &mut BlockGasMeter,
    write_log: &mut WriteLog,
    vp_wasm_cache: &mut VpCache<CA>,
    tx_wasm_cache: &mut TxCache<CA>,
) -> Result<BTreeSet<Address>>
where
    D: 'static + DB + for<'iter> DBIter<'iter> + Sync,
    H: 'static + StorageHasher + Sync,
    CA: 'static + WasmCacheAccess + Sync,
{
    gas_meter
        .add_compiling_fee(tx.code.len())
        .map_err(Error::GasError)?;
    let empty = vec![];
    let tx_data = tx.data.as_ref().unwrap_or(&empty);
    wasm::run::tx(
        storage,
        write_log,
        gas_meter,
        tx_index,
        &tx.code,
        tx_data,
        vp_wasm_cache,
        tx_wasm_cache,
    )
    .map_err(Error::TxRunnerError)
}

/// Check the acceptance of a transaction by validity predicates
#[allow(clippy::too_many_arguments)]
fn check_vps<D, H, CA>(
    tx: &Tx,
    tx_index: &TxIndex,
    storage: &Storage<D, H>,
    gas_meter: &mut BlockGasMeter,
    write_log: &WriteLog,
    verifiers_from_tx: &BTreeSet<Address>,
    vp_wasm_cache: &mut VpCache<CA>,
    #[cfg(not(feature = "mainnet"))]
    // This is true when the wrapper of this tx contained a valid
    // `testnet_pow::Solution`
    has_valid_pow: bool,
) -> Result<VpsResult>
where
    D: 'static + DB + for<'iter> DBIter<'iter> + Sync,
    H: 'static + StorageHasher + Sync,
    CA: 'static + WasmCacheAccess + Sync,
{
    let (verifiers, keys_changed) =
        write_log.verifiers_and_changed_keys(verifiers_from_tx);

    let initial_gas = gas_meter.get_current_transaction_gas();

    let vps_result = execute_vps(
        verifiers,
        keys_changed,
        tx,
        tx_index,
        storage,
        write_log,
        gas_meter.block_gas_limit,
        initial_gas,
        vp_wasm_cache,
        #[cfg(not(feature = "mainnet"))]
        has_valid_pow,
    )?;
    tracing::debug!("Total VPs gas cost {:?}", vps_result.gas_used);

    gas_meter
        .add_vps_gas(&vps_result.gas_used)
        .map_err(Error::GasError)?;

    Ok(vps_result)
}

/// Execute verifiers' validity predicates
#[allow(clippy::too_many_arguments)]
fn execute_vps<D, H, CA>(
    verifiers: BTreeSet<Address>,
    keys_changed: BTreeSet<storage::Key>,
    tx: &Tx,
    tx_index: &TxIndex,
    storage: &Storage<D, H>,
    write_log: &WriteLog,
    max_block_gas: u64,
    initial_gas: u64,
    vp_wasm_cache: &mut VpCache<CA>,
    #[cfg(not(feature = "mainnet"))]
    // This is true when the wrapper of this tx contained a valid
    // `testnet_pow::Solution`
    has_valid_pow: bool,
) -> Result<VpsResult>
where
    D: 'static + DB + for<'iter> DBIter<'iter> + Sync,
    H: 'static + StorageHasher + Sync,
    CA: 'static + WasmCacheAccess + Sync,
{
    verifiers
        .par_iter()
        .try_fold(VpsResult::default, |mut result, addr| {
            let mut gas_meter = VpGasMeter::new(max_block_gas, initial_gas);
            let accept = match &addr {
                Address::Implicit(_) | Address::Established(_) => {
                    let (vp, gas) = storage
                        .validity_predicate(addr)
                        .map_err(Error::StorageError)?;
                    gas_meter.add(gas).map_err(Error::GasError)?;
                    let vp =
                        vp.ok_or_else(|| Error::MissingAddress(addr.clone()))?;

                    gas_meter
                        .add_compiling_fee(vp.len())
                        .map_err(Error::GasError)?;

                    wasm::run::vp(
                        vp,
                        tx,
                        tx_index,
                        addr,
                        storage,
                        write_log,
                        &mut gas_meter,
                        &keys_changed,
                        &verifiers,
                        vp_wasm_cache.clone(),
                        #[cfg(not(feature = "mainnet"))]
                        has_valid_pow,
                    )
                    .map_err(Error::VpRunnerError)
                }
                Address::Internal(internal_addr) => {
                    let ctx = native_vp::Ctx::new(
                        addr,
                        storage,
                        write_log,
                        tx,
                        tx_index,
                        gas_meter,
                        &keys_changed,
                        &verifiers,
                        vp_wasm_cache.clone(),
                    );
                    let tx_data = match tx.data.as_ref() {
                        Some(data) => &data[..],
                        None => &[],
                    };

                    let accepted: Result<bool> = match internal_addr {
                        InternalAddress::PoS => {
                            let pos = PosVP { ctx };
                            let verifiers_addr_ref = &verifiers;
                            let pos_ref = &pos;
                            // TODO this is temporarily ran in a new thread to
                            // avoid crashing the ledger (required `UnwindSafe`
                            // and `RefUnwindSafe` in
                            // shared/src/ledger/pos/vp.rs)
                            let keys_changed_ref = &keys_changed;
                            let result = match panic::catch_unwind(move || {
                                pos_ref
                                    .validate_tx(
                                        tx_data,
                                        keys_changed_ref,
                                        verifiers_addr_ref,
                                    )
                                    .map_err(Error::PosNativeVpError)
                            }) {
                                Ok(result) => result,
                                Err(err) => {
                                    tracing::error!(
                                        "PoS native VP failed with {:#?}",
                                        err
                                    );
                                    Err(Error::PosNativeVpRuntime)
                                }
                            };
                            // Take the gas meter back out of the context
                            gas_meter = pos.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::Ibc => {
                            let ibc = Ibc { ctx };
                            let result = ibc
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::IbcNativeVpError);
                            // Take the gas meter back out of the context
                            gas_meter = ibc.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::Parameters => {
                            let parameters = ParametersVp { ctx };
                            let result = parameters
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::ParametersNativeVpError);
                            // Take the gas meter back out of the context
                            gas_meter = parameters.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::PosSlashPool => {
                            // Take the gas meter back out of the context
                            gas_meter = ctx.gas_meter.into_inner();
                            Err(Error::AccessForbidden(
                                (*internal_addr).clone(),
                            ))
                        }
                        InternalAddress::Governance => {
                            let governance = GovernanceVp { ctx };
                            let result = governance
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::GovernanceNativeVpError);
                            gas_meter = governance.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::SlashFund => {
                            let slash_fund = SlashFundVp { ctx };
                            let result = slash_fund
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::SlashFundNativeVpError);
                            gas_meter = slash_fund.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::IbcToken(_)
                        | InternalAddress::IbcEscrow
                        | InternalAddress::IbcBurn
                        | InternalAddress::IbcMint => {
                            // validate the transfer
                            let ibc_token = IbcToken { ctx };
                            let result = ibc_token
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::IbcTokenNativeVpError);
                            gas_meter = ibc_token.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::EthBridge => {
                            let bridge = EthBridge { ctx };
                            let result = bridge
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::EthBridgeNativeVpError);
                            gas_meter = bridge.ctx.gas_meter.into_inner();
                            result
                        }
                        InternalAddress::ReplayProtection => {
                            let replay_protection_vp =
                                ReplayProtectionVp { ctx };
                            let result = replay_protection_vp
                                .validate_tx(tx_data, &keys_changed, &verifiers)
                                .map_err(Error::ReplayProtectionNativeVpError);
                            gas_meter =
                                replay_protection_vp.ctx.gas_meter.into_inner();
                            result
                        }
                    };

                    accepted
                }
            };

            // Returning error from here will short-circuit the VP parallel
            // execution. It's important that we only short-circuit gas
            // errors to get deterministic gas costs
            result.gas_used.set(&gas_meter).map_err(Error::GasError)?;
            match accept {
                Ok(accepted) => {
                    if !accepted {
                        result.rejected_vps.insert(addr.clone());
                    } else {
                        result.accepted_vps.insert(addr.clone());
                    }
                    Ok(result)
                }
                Err(err) => match err {
                    Error::GasError(_) => Err(err),
                    _ => {
                        result.rejected_vps.insert(addr.clone());
                        result.errors.push((addr.clone(), err.to_string()));
                        Ok(result)
                    }
                },
            }
        })
        .try_reduce(VpsResult::default, |a, b| {
            merge_vp_results(a, b, max_block_gas, initial_gas)
        })
}

/// Merge VP results from parallel runs
fn merge_vp_results(
    a: VpsResult,
    mut b: VpsResult,
    max_block_gas: u64,
    initial_gas: u64,
) -> Result<VpsResult> {
    let mut accepted_vps = a.accepted_vps;
    let mut rejected_vps = a.rejected_vps;
    accepted_vps.extend(b.accepted_vps);
    rejected_vps.extend(b.rejected_vps);
    let mut errors = a.errors;
    errors.append(&mut b.errors);
    let mut gas_used = a.gas_used;

    // Returning error from here will short-circuit the VP parallel execution.
    // It's important that we only short-circuit gas errors to get deterministic
    // gas costs

    gas_used
        .merge(&mut b.gas_used, max_block_gas, initial_gas)
        .map_err(Error::GasError)?;

    Ok(VpsResult {
        accepted_vps,
        rejected_vps,
        gas_used,
        errors,
    })
}
