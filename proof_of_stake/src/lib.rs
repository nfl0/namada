//! Proof of Stake system.
//!
//! TODO: We might need to storage both active and total validator set voting
//! power. For consensus, we only consider active validator set voting power,
//! but for other activities in which inactive validators can participate (e.g.
//! voting on a protocol parameter changes, upgrades, default VP changes) we
//! should use the total validator set voting power.

#![doc(html_favicon_url = "https://dev.namada.net/master/favicon.png")]
#![doc(html_logo_url = "https://dev.namada.net/master/rustdoc-logo.png")]
#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]

pub mod btree_set;
pub mod epoched;
pub mod parameters;
pub mod rewards;
pub mod storage;
pub mod types;
// pub mod validation;

#[cfg(test)]
mod tests;

use core::fmt::Debug;
use std::cmp::{self, Reverse};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::TryFromIntError;

use borsh::BorshDeserialize;
use namada_core::ledger::storage_api::collections::lazy_map::{
    NestedSubKey, SubKey,
};
use namada_core::ledger::storage_api::collections::{LazyCollection, LazySet};
use namada_core::ledger::storage_api::token::credit_tokens;
use namada_core::ledger::storage_api::{
    self, OptionExt, StorageRead, StorageWrite,
};
use namada_core::types::address::{Address, InternalAddress};
use namada_core::types::key::{
    common, tm_consensus_key_raw_hash, PublicKeyTmRawHash,
};
pub use namada_core::types::storage::{Epoch, Key, KeySeg};
use namada_core::types::token;
use once_cell::unsync::Lazy;
use parameters::PosParams;
use rewards::PosRewardsCalculator;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use storage::{
    bonds_for_source_prefix, bonds_prefix, consensus_keys_key,
    decimal_mult_amount, get_validator_address_from_bond, into_tm_voting_power,
    is_bond_key, is_unbond_key, is_validator_slashes_key,
    last_block_proposer_key, mult_change_to_amount, params_key, slashes_prefix,
    unbonds_for_source_prefix, unbonds_prefix, validator_address_raw_hash_key,
    validator_last_slash_key, validator_max_commission_rate_change_key,
    BondDetails, BondsAndUnbondsDetail, BondsAndUnbondsDetails,
    ReverseOrdTokenAmount, RewardsAccumulator, SlashedAmount, UnbondDetails,
    UnbondRecord, ValidatorUnbondRecords,
};
use thiserror::Error;
use types::{
    decimal_mult_i128, BelowCapacityValidatorSet, BelowCapacityValidatorSets,
    BondId, Bonds, CommissionRates, ConsensusValidator, ConsensusValidatorSet,
    ConsensusValidatorSets, EpochedSlashes, GenesisValidator, Position,
    RewardsProducts, Slash, SlashType, Slashes, TotalDeltas, Unbonds,
    ValidatorAddresses, ValidatorConsensusKeys, ValidatorDeltas,
    ValidatorPositionAddresses, ValidatorSetPositions, ValidatorSetUpdate,
    ValidatorState, ValidatorStates, VoteInfo, WeightedValidator,
};

/// Address of the PoS account implemented as a native VP
pub const ADDRESS: Address = Address::Internal(InternalAddress::PoS);

/// Address of the PoS slash pool account
pub const SLASH_POOL_ADDRESS: Address =
    Address::Internal(InternalAddress::PosSlashPool);

/// Address of the staking token (i.e. the native token)
pub fn staking_token_address(storage: &impl StorageRead) -> Address {
    storage
        .get_native_token()
        .expect("Must be able to read native token address")
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum GenesisError {
    #[error("Voting power overflow: {0}")]
    VotingPowerOverflow(TryFromIntError),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum InflationError {
    #[error("Error")]
    Error,
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum BecomeValidatorError {
    #[error("The given address {0} is already a validator")]
    AlreadyValidator(Address),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum BondError {
    #[error("The given address {0} is not a validator address")]
    NotAValidator(Address),
    #[error(
        "The given source address {0} is a validator address. Validators may \
         not delegate."
    )]
    SourceMustNotBeAValidator(Address),
    #[error("The given validator address {0} is inactive")]
    InactiveValidator(Address),
    #[error("Voting power overflow: {0}")]
    VotingPowerOverflow(TryFromIntError),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum UnbondError {
    #[error("No bond could be found")]
    NoBondFound,
    #[error(
        "Trying to withdraw more tokens ({0}) than the amount bonded ({0})"
    )]
    UnbondAmountGreaterThanBond(token::Amount, token::Amount),
    #[error("No bonds found for the validator {0}")]
    ValidatorHasNoBonds(Address),
    #[error("Voting power not found for the validator {0}")]
    ValidatorHasNoVotingPower(Address),
    #[error("Voting power overflow: {0}")]
    VotingPowerOverflow(TryFromIntError),
    #[error("Trying to unbond from a frozen validator: {0}")]
    ValidatorIsFrozen(Address),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum WithdrawError {
    #[error("No unbond could be found for {0}")]
    NoUnbondFound(BondId),
    #[error("No unbond may be withdrawn yet for {0}")]
    NoWithdrawableUnbond(BondId),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum SlashError {
    #[error("The validator {0} has no total deltas value")]
    ValidatorHasNoTotalDeltas(Address),
    #[error("The validator {0} has no voting power")]
    ValidatorHasNoVotingPower(Address),
    #[error("Unexpected slash token change")]
    InvalidSlashChange(i128),
    #[error("Voting power overflow: {0}")]
    VotingPowerOverflow(TryFromIntError),
    #[error("Unexpected negative stake {0} for validator {1}")]
    NegativeStake(i128, Address),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum CommissionRateChangeError {
    #[error("Unexpected negative commission rate {0} for validator {1}")]
    NegativeRate(Decimal, Address),
    #[error("Rate change of {0} is too large for validator {1}")]
    RateChangeTooLarge(Decimal, Address),
    #[error(
        "There is no maximum rate change written in storage for validator {0}"
    )]
    NoMaxSetInStorage(Address),
    #[error("Cannot write to storage for validator {0}")]
    CannotWrite(Address),
    #[error("Cannot read storage for validator {0}")]
    CannotRead(Address),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum UnjailValidatorError {
    #[error("The given address {0} is not a validator address")]
    NotAValidator(Address),
    #[error("The given address {0} is not jailed in epoch {1}")]
    NotJailed(Address, Epoch),
    #[error(
        "The given address {0} is not eligible for unnjailing until epoch \
         {1}: current epoch is {2}"
    )]
    NotEligible(Address, Epoch, Epoch),
}

impl From<BecomeValidatorError> for storage_api::Error {
    fn from(err: BecomeValidatorError) -> Self {
        Self::new(err)
    }
}

impl From<BondError> for storage_api::Error {
    fn from(err: BondError) -> Self {
        Self::new(err)
    }
}

impl From<UnbondError> for storage_api::Error {
    fn from(err: UnbondError) -> Self {
        Self::new(err)
    }
}

impl From<WithdrawError> for storage_api::Error {
    fn from(err: WithdrawError) -> Self {
        Self::new(err)
    }
}

impl From<CommissionRateChangeError> for storage_api::Error {
    fn from(err: CommissionRateChangeError) -> Self {
        Self::new(err)
    }
}

impl From<InflationError> for storage_api::Error {
    fn from(err: InflationError) -> Self {
        Self::new(err)
    }
}

impl From<UnjailValidatorError> for storage_api::Error {
    fn from(err: UnjailValidatorError) -> Self {
        Self::new(err)
    }
}

/// Get the storage handle to the epoched consensus validator set
pub fn consensus_validator_set_handle() -> ConsensusValidatorSets {
    let key = storage::consensus_validator_set_key();
    ConsensusValidatorSets::open(key)
}

/// Get the storage handle to the epoched below-capacity validator set
pub fn below_capacity_validator_set_handle() -> BelowCapacityValidatorSets {
    let key = storage::below_capacity_validator_set_key();
    BelowCapacityValidatorSets::open(key)
}

/// Get the storage handle to a PoS validator's consensus key (used for
/// signing block votes).
pub fn validator_consensus_key_handle(
    validator: &Address,
) -> ValidatorConsensusKeys {
    let key = storage::validator_consensus_key_key(validator);
    ValidatorConsensusKeys::open(key)
}

/// Get the storage handle to a PoS validator's state
pub fn validator_state_handle(validator: &Address) -> ValidatorStates {
    let key = storage::validator_state_key(validator);
    ValidatorStates::open(key)
}

/// Get the storage handle to a PoS validator's deltas
pub fn validator_deltas_handle(validator: &Address) -> ValidatorDeltas {
    let key = storage::validator_deltas_key(validator);
    ValidatorDeltas::open(key)
}

/// Get the storage handle to the total deltas
pub fn total_deltas_handle() -> TotalDeltas {
    let key = storage::total_deltas_key();
    TotalDeltas::open(key)
}

/// Get the storage handle to the set of all validators
pub fn validator_addresses_handle() -> ValidatorAddresses {
    let key = storage::validator_addresses_key();
    ValidatorAddresses::open(key)
}

/// Get the storage handle to a PoS validator's commission rate
pub fn validator_commission_rate_handle(
    validator: &Address,
) -> CommissionRates {
    let key = storage::validator_commission_rate_key(validator);
    CommissionRates::open(key)
}

/// Get the storage handle to a bond
/// TODO: remove `get_remaining` and the unused storage (maybe just call it
/// `storage::bond_key`)
pub fn bond_handle(source: &Address, validator: &Address) -> Bonds {
    let bond_id = BondId {
        source: source.clone(),
        validator: validator.clone(),
    };
    let key = storage::bond_key(&bond_id);
    Bonds::open(key)
}

/// Get the storage handle to an unbond
pub fn unbond_handle(source: &Address, validator: &Address) -> Unbonds {
    let bond_id = BondId {
        source: source.clone(),
        validator: validator.clone(),
    };
    let key = storage::unbond_key(&bond_id);
    Unbonds::open(key)
}

/// Get the storage handle to a validator's total-unbonded map
pub fn unbond_records_handle(validator: &Address) -> ValidatorUnbondRecords {
    let key = storage::validator_total_unbonded_key(validator);
    ValidatorUnbondRecords::open(key)
}

/// Get the storage handle to a PoS validator's deltas
pub fn validator_set_positions_handle() -> ValidatorSetPositions {
    let key = storage::validator_set_positions_key();
    ValidatorSetPositions::open(key)
}

/// Get the storage handle to a PoS validator's slashes
pub fn validator_slashes_handle(validator: &Address) -> Slashes {
    let key = storage::validator_slashes_key(validator);
    Slashes::open(key)
}

/// Get the storage handle to list of all slashes to be processed and ultimately
/// placed in the `validator_slashes_handle`
pub fn enqueued_slashes_handle() -> EpochedSlashes {
    let key = storage::enqueued_slashes_key();
    EpochedSlashes::open(key)
}

/// Get the storage handle to the rewards accumulator for the consensus
/// validators in a given epoch
pub fn rewards_accumulator_handle() -> RewardsAccumulator {
    let key = storage::consensus_validator_rewards_accumulator_key();
    RewardsAccumulator::open(key)
}

/// Get the storage handle to a validator's self rewards products
pub fn validator_rewards_products_handle(
    validator: &Address,
) -> RewardsProducts {
    let key = storage::validator_self_rewards_product_key(validator);
    RewardsProducts::open(key)
}

/// Get the storage handle to the delegator rewards products associated with a
/// particular validator
pub fn delegator_rewards_products_handle(
    validator: &Address,
) -> RewardsProducts {
    let key = storage::validator_delegation_rewards_product_key(validator);
    RewardsProducts::open(key)
}

/// Init genesis
pub fn init_genesis<S>(
    storage: &mut S,
    params: &PosParams,
    validators: impl Iterator<Item = GenesisValidator> + Clone,
    current_epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    tracing::debug!("Initializing PoS genesis");
    write_pos_params(storage, params.clone())?;

    let mut total_bonded = token::Amount::default();
    consensus_validator_set_handle().init(storage, current_epoch)?;
    below_capacity_validator_set_handle().init(storage, current_epoch)?;
    validator_set_positions_handle().init(storage, current_epoch)?;
    validator_addresses_handle().init(storage, current_epoch)?;

    for GenesisValidator {
        address,
        tokens,
        consensus_key,
        commission_rate,
        max_commission_rate_change,
    } in validators
    {
        // This will fail if the key is already being used - the uniqueness must
        // be enforced in the genesis configuration to prevent it
        try_insert_consensus_key(storage, &consensus_key)?;

        total_bonded += tokens;

        // Insert the validator into a validator set and write its epoched
        // validator data
        insert_validator_into_validator_set(
            storage,
            params,
            &address,
            tokens,
            current_epoch,
            0,
        )?;

        validator_addresses_handle()
            .at(&current_epoch)
            .insert(storage, address.clone())?;

        // Write other validator data to storage
        write_validator_address_raw_hash(storage, &address, &consensus_key)?;
        write_validator_max_commission_rate_change(
            storage,
            &address,
            max_commission_rate_change,
        )?;
        validator_consensus_key_handle(&address).init_at_genesis(
            storage,
            consensus_key,
            current_epoch,
        )?;
        let delta = token::Change::from(tokens);
        validator_deltas_handle(&address).init_at_genesis(
            storage,
            delta,
            current_epoch,
        )?;
        bond_handle(&address, &address).init_at_genesis(
            storage,
            delta,
            current_epoch,
        )?;
        validator_commission_rate_handle(&address).init_at_genesis(
            storage,
            commission_rate,
            current_epoch,
        )?;
    }

    // Write total deltas to storage
    total_deltas_handle().init_at_genesis(
        storage,
        token::Change::from(total_bonded),
        current_epoch,
    )?;

    // Credit bonded token amount to the PoS account
    let staking_token = staking_token_address(storage);
    credit_tokens(storage, &staking_token, &ADDRESS, total_bonded)?;
    // Copy the genesis validator set into the pipeline epoch as well
    for epoch in (current_epoch.next()).iter_range(params.pipeline_len) {
        copy_validator_sets_and_positions(
            storage,
            current_epoch,
            epoch,
            &consensus_validator_set_handle(),
            &below_capacity_validator_set_handle(),
        )?;
    }

    tracing::debug!("FINISHED GENESIS");

    Ok(())
}

/// Read PoS parameters
pub fn read_pos_params<S>(storage: &S) -> storage_api::Result<PosParams>
where
    S: StorageRead,
{
    storage
        .read(&params_key())
        .transpose()
        .expect("PosParams should always exist in storage after genesis")
}

/// Write PoS parameters
pub fn write_pos_params<S>(
    storage: &mut S,
    params: PosParams,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let key = params_key();
    storage.write(&key, params)
}

/// Get the validator address given the raw hash of the Tendermint consensus key
pub fn find_validator_by_raw_hash<S>(
    storage: &S,
    raw_hash: impl AsRef<str>,
) -> storage_api::Result<Option<Address>>
where
    S: StorageRead,
{
    let key = validator_address_raw_hash_key(raw_hash);
    storage.read(&key)
}

/// Write PoS validator's address raw hash.
pub fn write_validator_address_raw_hash<S>(
    storage: &mut S,
    validator: &Address,
    consensus_key: &common::PublicKey,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let raw_hash = tm_consensus_key_raw_hash(consensus_key);
    storage.write(&validator_address_raw_hash_key(raw_hash), validator)
}

/// Read PoS validator's max commission rate change.
pub fn read_validator_max_commission_rate_change<S>(
    storage: &S,
    validator: &Address,
) -> storage_api::Result<Option<Decimal>>
where
    S: StorageRead,
{
    let key = validator_max_commission_rate_change_key(validator);
    storage.read(&key)
}

/// Write PoS validator's max commission rate change.
pub fn write_validator_max_commission_rate_change<S>(
    storage: &mut S,
    validator: &Address,
    change: Decimal,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let key = validator_max_commission_rate_change_key(validator);
    storage.write(&key, change)
}

/// Read the most recent slash epoch for the given epoch
pub fn read_validator_last_slash_epoch<S>(
    storage: &S,
    validator: &Address,
) -> storage_api::Result<Option<Epoch>>
where
    S: StorageRead,
{
    let key = validator_last_slash_key(validator);
    storage.read(&key)
}

/// Write the most recent slash epoch for the given epoch
pub fn write_validator_last_slash_epoch<S>(
    storage: &mut S,
    validator: &Address,
    epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let key = validator_last_slash_key(validator);
    storage.write(&key, epoch)
}

/// Read last block proposer address.
pub fn read_last_block_proposer_address<S>(
    storage: &S,
) -> storage_api::Result<Option<Address>>
where
    S: StorageRead,
{
    let key = last_block_proposer_key();
    storage.read(&key)
}

/// Write last block proposer address.
pub fn write_last_block_proposer_address<S>(
    storage: &mut S,
    address: Address,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let key = last_block_proposer_key();
    storage.write(&key, address)
}

/// Read PoS validator's delta value.
pub fn read_validator_delta_value<S>(
    storage: &S,
    params: &PosParams,
    validator: &Address,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<Option<token::Change>>
where
    S: StorageRead,
{
    let handle = validator_deltas_handle(validator);
    handle.get_delta_val(storage, epoch, params)
}

/// Read PoS validator's stake (sum of deltas).
/// Returns `None` when the given address is not a validator address. For a
/// validator with `0` stake, this returns `Ok(token::Amount::default())`.
pub fn read_validator_stake<S>(
    storage: &S,
    params: &PosParams,
    validator: &Address,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<Option<token::Amount>>
where
    S: StorageRead,
{
    tracing::debug!("Read validator stake at epoch {}", epoch);
    let handle = validator_deltas_handle(validator);
    let amount = handle
        .get_sum(storage, epoch, params)?
        .map(token::Amount::from_change);
    Ok(amount)
}

/// Add or remove PoS validator's stake delta value
pub fn update_validator_deltas<S>(
    storage: &mut S,
    params: &PosParams,
    validator: &Address,
    delta: token::Change,
    current_epoch: namada_core::types::storage::Epoch,
    offset: u64,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let handle = validator_deltas_handle(validator);
    let val = handle
        .get_delta_val(storage, current_epoch + offset, params)?
        .unwrap_or_default();
    handle.set(storage, val + delta, current_epoch, offset)
}

/// Read PoS total stake (sum of deltas).
pub fn read_total_stake<S>(
    storage: &S,
    params: &PosParams,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<token::Amount>
where
    S: StorageRead,
{
    let handle = total_deltas_handle();
    let amnt = handle
        .get_sum(storage, epoch, params)?
        .map(token::Amount::from_change)
        .unwrap_or_default();
    Ok(amnt)
}

/// Read all addresses from consensus validator set.
pub fn read_consensus_validator_set_addresses<S>(
    storage: &S,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<HashSet<Address>>
where
    S: StorageRead,
{
    consensus_validator_set_handle()
        .at(&epoch)
        .iter(storage)?
        .map(|res| res.map(|(_sub_key, address)| address))
        .collect()
}

/// Read all addresses from below-capacity validator set.
pub fn read_below_capacity_validator_set_addresses<S>(
    storage: &S,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<HashSet<Address>>
where
    S: StorageRead,
{
    below_capacity_validator_set_handle()
        .at(&epoch)
        .iter(storage)?
        .map(|res| res.map(|(_sub_key, address)| address))
        .collect()
}

/// Read all addresses from consensus validator set with their stake.
pub fn read_consensus_validator_set_addresses_with_stake<S>(
    storage: &S,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<HashSet<WeightedValidator>>
where
    S: StorageRead,
{
    consensus_validator_set_handle()
        .at(&epoch)
        .iter(storage)?
        .map(|res| {
            res.map(
                |(
                    NestedSubKey::Data {
                        key: bonded_stake,
                        nested_sub_key: _,
                    },
                    address,
                )| {
                    WeightedValidator {
                        address,
                        bonded_stake,
                    }
                },
            )
        })
        .collect()
}

/// Count the number of consensus validators
pub fn get_num_consensus_validators<S>(
    storage: &S,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<u64>
where
    S: StorageRead,
{
    Ok(consensus_validator_set_handle()
        .at(&epoch)
        .iter(storage)?
        .count() as u64)
}

/// Read all addresses from below-capacity validator set with their stake.
pub fn read_below_capacity_validator_set_addresses_with_stake<S>(
    storage: &S,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<HashSet<WeightedValidator>>
where
    S: StorageRead,
{
    below_capacity_validator_set_handle()
        .at(&epoch)
        .iter(storage)?
        .map(|res| {
            res.map(
                |(
                    NestedSubKey::Data {
                        key: ReverseOrdTokenAmount(bonded_stake),
                        nested_sub_key: _,
                    },
                    address,
                )| {
                    WeightedValidator {
                        address,
                        bonded_stake,
                    }
                },
            )
        })
        .collect()
}

/// Read all validator addresses.
/// TODO: expand this to include the jailed validators as well, as it currently
/// only does consensus and bc
pub fn read_all_validator_addresses<S>(
    storage: &S,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<HashSet<Address>>
where
    S: StorageRead,
{
    validator_addresses_handle()
        .at(&epoch)
        .iter(storage)?
        .collect()
}

/// Update PoS total deltas.
/// Note: for EpochedDelta, write the value to change storage by
pub fn update_total_deltas<S>(
    storage: &mut S,
    params: &PosParams,
    delta: token::Change,
    current_epoch: namada_core::types::storage::Epoch,
    offset: u64,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let handle = total_deltas_handle();
    let val = handle
        .get_delta_val(storage, current_epoch + offset, params)?
        .unwrap_or_default();
    handle.set(storage, val + delta, current_epoch, offset)
}

/// Check if the provided address is a validator address
pub fn is_validator<S>(
    storage: &S,
    address: &Address,
    params: &PosParams,
    epoch: namada_core::types::storage::Epoch,
) -> storage_api::Result<bool>
where
    S: StorageRead + StorageWrite,
{
    let state = validator_state_handle(address).get(storage, epoch, params)?;
    Ok(state.is_some())
}

/// Check if the provided address is a delegator address, optionally at a
/// particular epoch
pub fn is_delegator<S>(
    storage: &S,
    address: &Address,
    epoch: Option<namada_core::types::storage::Epoch>,
) -> storage_api::Result<bool>
where
    S: StorageRead + StorageWrite,
{
    let prefix = bonds_for_source_prefix(address);
    match epoch {
        Some(epoch) => {
            let iter = storage_api::iter_prefix_bytes(storage, &prefix)?;
            for res in iter {
                let (key, _) = res?;
                if let Some((bond_id, bond_epoch)) = is_bond_key(&key) {
                    if bond_id.source != bond_id.validator
                        && bond_epoch <= epoch
                    {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        None => Ok(storage_api::iter_prefix_bytes(storage, &prefix)?
            .next()
            .is_some()),
    }
}

/// Self-bond tokens to a validator when `source` is `None` or equal to
/// the `validator` address, or delegate tokens from the `source` to the
/// `validator`.
pub fn bond_tokens<S>(
    storage: &mut S,
    source: Option<&Address>,
    validator: &Address,
    amount: token::Amount,
    current_epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let amount = amount.change();
    tracing::debug!("Bonding token amount {amount} at epoch {current_epoch}");
    let params = read_pos_params(storage)?;
    let pipeline_epoch = current_epoch + params.pipeline_len;
    if let Some(source) = source {
        if source != validator
            && is_validator(storage, source, &params, pipeline_epoch)?
        {
            return Err(
                BondError::SourceMustNotBeAValidator(source.clone()).into()
            );
        }
    }
    let validator_state_handle = validator_state_handle(validator);
    let state = validator_state_handle.get(storage, pipeline_epoch, &params)?;
    if state.is_none() {
        return Err(BondError::NotAValidator(validator.clone()).into());
    }

    let source = source.unwrap_or(validator);
    let bond_handle = bond_handle(source, validator);

    // Check that validator is not inactive at anywhere between the current
    // epoch and pipeline offset
    for epoch in current_epoch.iter_range(params.pipeline_len) {
        if let Some(ValidatorState::Inactive) =
            validator_state_handle.get(storage, epoch, &params)?
        {
            return Err(BondError::InactiveValidator(validator.clone()).into());
        }
    }

    // Initialize or update the bond at the pipeline offset
    let offset = params.pipeline_len;
    let cur_remain = bond_handle
        .get_delta_val(storage, current_epoch + offset, &params)?
        .unwrap_or_default();
    tracing::debug!(
        "Bond remain at offset epoch {}: {}",
        current_epoch + offset,
        cur_remain
    );
    bond_handle.set(storage, cur_remain + amount, current_epoch, offset)?;

    // Update the validator set
    // We allow bonding if the validator is jailed, however if jailed, there
    // must be no changes to the validator set. Check at the pipeline epoch.
    let is_jailed_at_pipeline = matches!(
        validator_state_handle
            .get(storage, pipeline_epoch, &params)?
            .unwrap(),
        ValidatorState::Jailed
    );
    if !is_jailed_at_pipeline {
        update_validator_set(
            storage,
            &params,
            validator,
            amount,
            current_epoch,
        )?;
    }

    // Update the validator and total deltas
    update_validator_deltas(
        storage,
        &params,
        validator,
        amount,
        current_epoch,
        offset,
    )?;

    update_total_deltas(storage, &params, amount, current_epoch, offset)?;

    // Transfer the bonded tokens from the source to PoS
    let staking_token = staking_token_address(storage);
    transfer_tokens(
        storage,
        &staking_token,
        token::Amount::from_change(amount),
        source,
        &ADDRESS,
    )?;

    Ok(())
}

/// Insert the new validator into the right validator set (depending on its
/// stake)
fn insert_validator_into_validator_set<S>(
    storage: &mut S,
    params: &PosParams,
    address: &Address,
    stake: token::Amount,
    current_epoch: Epoch,
    offset: u64,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let target_epoch = current_epoch + offset;
    let consensus_set = &consensus_validator_set_handle().at(&target_epoch);
    let below_cap_set =
        &below_capacity_validator_set_handle().at(&target_epoch);

    let num_consensus_validators =
        get_num_consensus_validators(storage, target_epoch)?;
    if num_consensus_validators < params.max_validator_slots {
        insert_validator_into_set(
            &consensus_set.at(&stake),
            storage,
            &target_epoch,
            address,
        )?;
        validator_state_handle(address).set(
            storage,
            ValidatorState::Consensus,
            current_epoch,
            offset,
        )?;
    } else {
        // Check to see if the current genesis validator should replace one
        // already in the consensus set
        let min_consensus_amount =
            get_min_consensus_validator_amount(consensus_set, storage)?;
        if stake > min_consensus_amount {
            // Swap this genesis validator in and demote the last min consensus
            // validator
            let min_consensus_handle = consensus_set.at(&min_consensus_amount);
            // Remove last min consensus validator
            let last_min_consensus_position =
                find_last_position(&min_consensus_handle, storage)?.expect(
                    "There must be always be at least 1 consensus validator",
                );
            let removed = min_consensus_handle
                .remove(storage, &last_min_consensus_position)?
                .expect(
                    "There must be always be at least 1 consensus validator",
                );
            // Insert last min consensus validator into the below-capacity set
            insert_validator_into_set(
                &below_cap_set.at(&min_consensus_amount.into()),
                storage,
                &target_epoch,
                &removed,
            )?;
            validator_state_handle(&removed).set(
                storage,
                ValidatorState::BelowCapacity,
                current_epoch,
                offset,
            )?;
            // Insert the current genesis validator into the consensus set
            insert_validator_into_set(
                &consensus_set.at(&stake),
                storage,
                &target_epoch,
                address,
            )?;
            // Update and set the validator states
            validator_state_handle(address).set(
                storage,
                ValidatorState::Consensus,
                current_epoch,
                offset,
            )?;
        } else {
            // Insert the current genesis validator into the below-capacity set
            insert_validator_into_set(
                &below_cap_set.at(&stake.into()),
                storage,
                &target_epoch,
                address,
            )?;
            validator_state_handle(address).set(
                storage,
                ValidatorState::BelowCapacity,
                current_epoch,
                offset,
            )?;
        }
    }
    Ok(())
}

/// Update validator set when a validator receives a new bond and when
/// its bond is unbonded (self-bond or delegation).
fn update_validator_set<S>(
    storage: &mut S,
    params: &PosParams,
    validator: &Address,
    token_change: token::Change,
    current_epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    if token_change == 0_i128 {
        return Ok(());
    }
    let epoch = current_epoch + params.pipeline_len;
    tracing::debug!(
        "Update epoch for validator set: {epoch}, validator: {validator}"
    );
    let consensus_validator_set = consensus_validator_set_handle();
    let below_capacity_validator_set = below_capacity_validator_set_handle();

    // Validator sets at the pipeline offset. If these are empty, then we need
    // to copy over the most recent filled validator set into this epoch first
    let consensus_val_handle = consensus_validator_set.at(&epoch);
    let below_capacity_val_handle = below_capacity_validator_set.at(&epoch);

    let tokens_pre = read_validator_stake(storage, params, validator, epoch)?
        .unwrap_or_default();

    // tracing::debug!("VALIDATOR STAKE BEFORE UPDATE: {}", tokens_pre);

    let tokens_post = tokens_pre.change() + token_change;
    // TODO: handle overflow or negative vals perhaps with TryFrom
    let tokens_post = token::Amount::from_change(tokens_post);

    // TODO: The position is only set when the validator is in consensus or
    // below_capacity set (not in below_threshold set)
    let position =
        read_validator_set_position(storage, validator, epoch, params)?
            .ok_or_err_msg(
                "Validator must have a stored validator set position",
            )?;
    let consensus_vals_pre = consensus_val_handle.at(&tokens_pre);

    let in_consensus = if consensus_vals_pre.contains(storage, &position)? {
        let val_address = consensus_vals_pre.get(storage, &position)?;
        debug_assert!(val_address.is_some());
        val_address == Some(validator.clone())
    } else {
        false
    };

    if in_consensus {
        // It's initially consensus
        tracing::debug!("Target validator is consensus");

        consensus_vals_pre.remove(storage, &position)?;

        let max_below_capacity_validator_amount =
            get_max_below_capacity_validator_amount(
                &below_capacity_val_handle,
                storage,
            )?
            .unwrap_or_default();

        if tokens_post < max_below_capacity_validator_amount {
            tracing::debug!("Need to swap validators");
            // Place the validator into the below-capacity set and promote the
            // lowest position max below-capacity validator.

            // Remove the max below-capacity validator first
            let below_capacity_vals_max = below_capacity_val_handle
                .at(&max_below_capacity_validator_amount.into());
            let lowest_position =
                find_first_position(&below_capacity_vals_max, storage)?
                    .unwrap();
            let removed_max_below_capacity = below_capacity_vals_max
                .remove(storage, &lowest_position)?
                .expect("Must have been removed");

            // Insert the previous max below-capacity validator into the
            // consensus set
            insert_validator_into_set(
                &consensus_val_handle.at(&max_below_capacity_validator_amount),
                storage,
                &epoch,
                &removed_max_below_capacity,
            )?;
            validator_state_handle(&removed_max_below_capacity).set(
                storage,
                ValidatorState::Consensus,
                current_epoch,
                params.pipeline_len,
            )?;

            // Insert the current validator into the below-capacity set
            insert_validator_into_set(
                &below_capacity_val_handle.at(&tokens_post.into()),
                storage,
                &epoch,
                validator,
            )?;
            validator_state_handle(validator).set(
                storage,
                ValidatorState::BelowCapacity,
                current_epoch,
                params.pipeline_len,
            )?;
        } else {
            tracing::debug!("Validator remains in consensus set");
            // The current validator should remain in the consensus set - place
            // it into a new position
            insert_validator_into_set(
                &consensus_val_handle.at(&tokens_post),
                storage,
                &epoch,
                validator,
            )?;
        }
    } else {
        // TODO: handle the new third set - below threshold

        // It's initially below-capacity
        let below_capacity_vals_pre =
            below_capacity_val_handle.at(&tokens_pre.into());
        let removed = below_capacity_vals_pre.remove(storage, &position)?;
        debug_assert!(removed.is_some());
        debug_assert_eq!(&removed.unwrap(), validator);

        let min_consensus_validator_amount =
            get_min_consensus_validator_amount(&consensus_val_handle, storage)?;

        if tokens_post > min_consensus_validator_amount {
            // Place the validator into the consensus set and demote the last
            // position min consensus validator to the below-capacity set

            // Remove the min consensus validator first
            let consensus_vals_min =
                consensus_val_handle.at(&min_consensus_validator_amount);
            let last_position_of_min_consensus_vals =
                find_last_position(&consensus_vals_min, storage)?.expect(
                    "There must be always be at least 1 consensus validator",
                );
            let removed_min_consensus = consensus_vals_min
                .remove(storage, &last_position_of_min_consensus_vals)?
                .expect(
                    "There must be always be at least 1 consensus validator",
                );

            // Insert the min consensus validator into the below-capacity set
            insert_validator_into_set(
                &below_capacity_val_handle
                    .at(&min_consensus_validator_amount.into()),
                storage,
                &epoch,
                &removed_min_consensus,
            )?;
            validator_state_handle(&removed_min_consensus).set(
                storage,
                ValidatorState::BelowCapacity,
                current_epoch,
                params.pipeline_len,
            )?;

            // Insert the current validator into the consensus set
            insert_validator_into_set(
                &consensus_val_handle.at(&tokens_post),
                storage,
                &epoch,
                validator,
            )?;
            validator_state_handle(validator).set(
                storage,
                ValidatorState::Consensus,
                current_epoch,
                params.pipeline_len,
            )?;
        } else {
            // The current validator should remain in the below-capacity set
            insert_validator_into_set(
                &below_capacity_val_handle.at(&tokens_post.into()),
                storage,
                &epoch,
                validator,
            )?;
            validator_state_handle(validator).set(
                storage,
                ValidatorState::BelowCapacity,
                current_epoch,
                params.pipeline_len,
            )?;
        }
    }
    Ok(())
}

/// Validator sets and positions copying into a future epoch
pub fn copy_validator_sets_and_positions<S>(
    storage: &mut S,
    current_epoch: Epoch,
    target_epoch: Epoch,
    consensus_validator_set: &ConsensusValidatorSets,
    below_capacity_validator_set: &BelowCapacityValidatorSets,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let prev_epoch = target_epoch.prev();

    let (consensus, below_capacity) = (
        consensus_validator_set.at(&prev_epoch),
        below_capacity_validator_set.at(&prev_epoch),
    );
    debug_assert!(!consensus.is_empty(storage)?);

    // Need to copy into memory here to avoid borrowing a ref
    // simultaneously as immutable and mutable
    let mut consensus_in_mem: HashMap<(token::Amount, Position), Address> =
        HashMap::new();
    let mut below_cap_in_mem: HashMap<
        (ReverseOrdTokenAmount, Position),
        Address,
    > = HashMap::new();

    for val in consensus.iter(storage)? {
        let (
            NestedSubKey::Data {
                key: stake,
                nested_sub_key: SubKey::Data(position),
            },
            address,
        ) = val?;
        consensus_in_mem.insert((stake, position), address);
    }
    for val in below_capacity.iter(storage)? {
        let (
            NestedSubKey::Data {
                key: stake,
                nested_sub_key: SubKey::Data(position),
            },
            address,
        ) = val?;
        below_cap_in_mem.insert((stake, position), address);
    }

    tracing::debug!("{consensus_in_mem:?}");

    for ((val_stake, val_position), val_address) in consensus_in_mem.into_iter()
    {
        consensus_validator_set
            .at(&target_epoch)
            .at(&val_stake)
            .insert(storage, val_position, val_address)?;
    }
    tracing::debug!("New validator set should be inserted:");
    tracing::debug!(
        "{:?}",
        read_consensus_validator_set_addresses(storage, target_epoch)?
    );

    for ((val_stake, val_position), val_address) in below_cap_in_mem.into_iter()
    {
        below_capacity_validator_set
            .at(&target_epoch)
            .at(&val_stake)
            .insert(storage, val_position, val_address)?;
    }

    // Copy validator positions
    let mut positions = HashMap::<Address, Position>::default();
    let positions_handle = validator_set_positions_handle().at(&prev_epoch);
    for result in positions_handle.iter(storage)? {
        let (validator, position) = result?;
        positions.insert(validator, position);
    }
    let new_positions_handle =
        validator_set_positions_handle().at(&target_epoch);
    for (validator, position) in positions {
        let prev = new_positions_handle.insert(storage, validator, position)?;
        debug_assert!(prev.is_none());
    }
    validator_set_positions_handle().set_last_update(storage, current_epoch)?;

    // Copy set of all validator addresses
    let mut all_validators = HashSet::<Address>::default();
    let all_validators_handle = validator_addresses_handle().at(&prev_epoch);
    for result in all_validators_handle.iter(storage)? {
        let validator = result?;
        all_validators.insert(validator);
    }
    let new_all_validators_handle =
        validator_addresses_handle().at(&target_epoch);
    for validator in all_validators {
        let was_in = new_all_validators_handle.insert(storage, validator)?;
        debug_assert!(!was_in);
    }

    Ok(())
}

/// Read the position of the validator in the subset of validators that have the
/// same bonded stake. This information is held in its own epoched structure in
/// addition to being inside the validator sets.
fn read_validator_set_position<S>(
    storage: &S,
    validator: &Address,
    epoch: Epoch,
    params: &PosParams,
) -> storage_api::Result<Option<Position>>
where
    S: StorageRead,
{
    let handle = validator_set_positions_handle();
    handle.get_position(storage, &epoch, validator, params)
}

/// Find the first (lowest) position in a validator set if it is not empty
fn find_first_position<S>(
    handle: &ValidatorPositionAddresses,
    storage: &S,
) -> storage_api::Result<Option<Position>>
where
    S: StorageRead,
{
    let lowest_position = handle
        .iter(storage)?
        .next()
        .transpose()?
        .map(|(position, _addr)| position);
    Ok(lowest_position)
}

/// Find the last (greatest) position in a validator set if it is not empty
fn find_last_position<S>(
    handle: &ValidatorPositionAddresses,
    storage: &S,
) -> storage_api::Result<Option<Position>>
where
    S: StorageRead,
{
    let position = handle
        .iter(storage)?
        .last()
        .transpose()?
        .map(|(position, _addr)| position);
    Ok(position)
}

/// Find next position in a validator set or 0 if empty
fn find_next_position<S>(
    handle: &ValidatorPositionAddresses,
    storage: &S,
) -> storage_api::Result<Position>
where
    S: StorageRead,
{
    let position_iter = handle.iter(storage)?;
    let next = position_iter
        .last()
        .transpose()?
        .map(|(position, _address)| position.next())
        .unwrap_or_default();
    Ok(next)
}

fn get_min_consensus_validator_amount<S>(
    handle: &ConsensusValidatorSet,
    storage: &S,
) -> storage_api::Result<token::Amount>
where
    S: StorageRead,
{
    Ok(handle
        .iter(storage)?
        .next()
        .transpose()?
        .map(|(subkey, _address)| match subkey {
            NestedSubKey::Data {
                key,
                nested_sub_key: _,
            } => key,
        })
        .unwrap_or_default())
}

/// Returns `Ok(None)` when the below capacity set is empty.
fn get_max_below_capacity_validator_amount<S>(
    handle: &BelowCapacityValidatorSet,
    storage: &S,
) -> storage_api::Result<Option<token::Amount>>
where
    S: StorageRead,
{
    Ok(handle
        .iter(storage)?
        .next()
        .transpose()?
        .map(|(subkey, _address)| match subkey {
            NestedSubKey::Data {
                key,
                nested_sub_key: _,
            } => token::Amount::from(key),
        }))
}

fn insert_validator_into_set<S>(
    handle: &ValidatorPositionAddresses,
    storage: &mut S,
    epoch: &Epoch,
    address: &Address,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let next_position = find_next_position(handle, storage)?;
    tracing::debug!(
        "Inserting validator {} into position {:?} at epoch {}",
        address.clone(),
        next_position.clone(),
        epoch.clone()
    );
    handle.insert(storage, next_position, address.clone())?;
    validator_set_positions_handle().at(epoch).insert(
        storage,
        address.clone(),
        next_position,
    )?;
    Ok(())
}

/// Unbond tokens that are bonded between a validator and a source (self or
/// delegator)
pub fn unbond_tokens<S>(
    storage: &mut S,
    source: Option<&Address>,
    validator: &Address,
    amount: token::Amount,
    current_epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let amount = amount.change();
    tracing::debug!("Unbonding token amount {amount} at epoch {current_epoch}");
    let params = read_pos_params(storage)?;
    let pipeline_epoch = current_epoch + params.pipeline_len;
    tracing::debug!(
        "Current validator stake at pipeline: {}",
        read_validator_stake(storage, &params, validator, pipeline_epoch)?
            .unwrap_or_default()
    );

    // Make sure source is not some other validator
    if let Some(source) = source {
        if source != validator
            && is_validator(storage, source, &params, pipeline_epoch)?
        {
            return Err(
                BondError::SourceMustNotBeAValidator(source.clone()).into()
            );
        }
    }
    // Make sure the target is actually a validator
    if !is_validator(storage, validator, &params, pipeline_epoch)? {
        return Err(BondError::NotAValidator(validator.clone()).into());
    }
    // Make sure the validator is not currently frozen
    if is_validator_frozen(storage, validator, current_epoch, &params)? {
        return Err(UnbondError::ValidatorIsFrozen(validator.clone()).into());
    }

    // Should be able to unbond inactive validators

    // Check that validator is not inactive at anywhere between the current
    // epoch and pipeline offset
    // let validator_state_handle = validator_state_handle(validator);
    // for epoch in current_epoch.iter_range(params.pipeline_len) {
    //     if let Some(ValidatorState::Inactive) =
    //         validator_state_handle.get(storage, epoch, &params)?
    //     {
    //         return
    // Err(BondError::InactiveValidator(validator.clone()).into());     }
    // }

    let source = source.unwrap_or(validator);
    let bonds_handle = bond_handle(source, validator);

    // Make sure there are enough tokens left in the bond at the pipeline offset
    let remaining_at_pipeline = bonds_handle
        .get_sum(storage, pipeline_epoch, &params)?
        .unwrap_or_default();
    if amount > remaining_at_pipeline {
        return Err(UnbondError::UnbondAmountGreaterThanBond(
            token::Amount::from_change(amount),
            token::Amount::from_change(remaining_at_pipeline),
        )
        .into());
    }

    let unbonds = unbond_handle(source, validator);
    // TODO: think if this should be +1 or not!!!
    let withdrawable_epoch =
        current_epoch + params.pipeline_len + params.unbonding_len;

    let mut remaining = token::Amount::from_change(amount);
    let mut amount_after_slashing = token::Change::default();

    // Iterate thru bonds, find non-zero delta entries starting from
    // future-most, then decrement those values. For every val that
    // gets decremented down to 0, need a unique unbond object.
    // Read all matched bonds into memory to do reverse iteration
    #[allow(clippy::needless_collect)]
    let bonds: Vec<Result<_, _>> =
        bonds_handle.get_data_handler().iter(storage)?.collect();

    println!("Bonds before decrementing:");
    for ep in Epoch::default().iter_range(current_epoch.0 + 3) {
        let delta = bonds_handle
            .get_delta_val(storage, ep, &params)?
            .unwrap_or_default();
        if delta != 0 {
            println!("bond ∆ at epoch {}: {}", ep, delta);
        }
    }

    let mut bond_iter = bonds.into_iter().rev();

    // Map: { bond start epoch, (new bond value, unbond value) }
    let mut new_bond_values_map =
        HashMap::<Epoch, (token::Amount, token::Amount)>::new();

    while remaining > token::Amount::default() {
        println!("remaining = {}", remaining);
        let bond = bond_iter.next().transpose()?;
        if bond.is_none() {
            continue;
        }
        let (bond_epoch, bond_amnt) = bond.unwrap();
        println!("Bond (epoch, amnt) = ({}, {})", bond_epoch, bond_amnt);
        let bond_amount = token::Amount::from_change(bond_amnt);

        let to_unbond = cmp::min(bond_amount, remaining);
        let new_bond_amount = bond_amount - to_unbond;
        new_bond_values_map.insert(bond_epoch, (new_bond_amount, to_unbond));
        println!("to_unbond (init) = {}", to_unbond);

        let mut slashes_for_this_bond = Vec::<Slash>::new();
        for slash in validator_slashes_handle(validator).iter(storage)? {
            let slash = slash?;
            if bond_epoch <= slash.epoch {
                slashes_for_this_bond.push(slash);
            }
        }

        amount_after_slashing +=
            get_slashed_amount(&params, to_unbond, &mut slashes_for_this_bond)?;
        println!("Cur amnt after slashing = {}", &amount_after_slashing);

        let record = UnbondRecord {
            amount: to_unbond,
            start: bond_epoch,
        };
        unbond_records_handle(validator)
            .at(&pipeline_epoch)
            .push(storage, record)?;

        remaining -= to_unbond;
    }
    drop(bond_iter);

    // Write the in-memory bond and unbond values back to storage
    for (bond_epoch, (new_bond_amount, unbond_amount)) in
        new_bond_values_map.into_iter()
    {
        bonds_handle.set(storage, new_bond_amount.into(), bond_epoch, 0)?;
        update_unbond(
            &unbonds,
            storage,
            &withdrawable_epoch,
            &bond_epoch,
            unbond_amount,
        )?;
    }

    println!("Bonds after decrementing:");
    for ep in Epoch::default().iter_range(current_epoch.0 + 3) {
        let delta = bonds_handle
            .get_delta_val(storage, ep, &params)?
            .unwrap_or_default();
        if delta != 0 {
            println!("bond ∆ at epoch {}: {}", ep, delta);
        }
    }
    let stake_at_pipeline =
        read_validator_stake(storage, &params, validator, pipeline_epoch)?
            .unwrap_or_default()
            .change();
    let token_change = cmp::min(amount_after_slashing, stake_at_pipeline);

    // Update the validator set at the pipeline offset. Since unbonding from a
    // jailed validator who is no longer frozen is allowed, only update the
    // validator set if the validator is not jailed
    let is_jailed_at_pipeline = matches!(
        validator_state_handle(validator)
            .get(storage, pipeline_epoch, &params)?
            .unwrap(),
        ValidatorState::Jailed
    );
    if !is_jailed_at_pipeline {
        tracing::debug!("Updating validator set for unbonding");
        update_validator_set(
            storage,
            &params,
            validator,
            -token_change,
            current_epoch,
        )?;
    }

    // Update the validator and total deltas at the pipeline offset
    update_validator_deltas(
        storage,
        &params,
        validator,
        -token_change,
        current_epoch,
        params.pipeline_len,
    )?;
    update_total_deltas(
        storage,
        &params,
        -token_change,
        current_epoch,
        params.pipeline_len,
    )?;

    Ok(())
}

// Compute a token amount after slashing, given the initial amount and a set of
// slashes. It is assumed that the input `slashes` are those commited while the
// `amount` was contributing to voting power.
fn get_slashed_amount(
    params: &PosParams,
    amount: token::Amount,
    slashes: &mut Vec<Slash>,
) -> storage_api::Result<token::Change> {
    println!("\nFN `get_slashed_amount`");

    let mut updated_amount = amount;
    let mut computed_amounts = Vec::<SlashedAmount>::new();

    slashes.sort_by_key(|s| s.epoch);
    println!("Num slashes: {}", slashes.len());
    for slash in slashes {
        println!("Slash epoch: {}, rate: {}", slash.epoch, slash.rate);
        let infraction_epoch = slash.epoch;
        let mut computed_to_remove = BTreeSet::<Reverse<usize>>::new();
        for (ix, slashed_amount) in computed_amounts.iter().enumerate() {
            // Update amount with slashes that happened more than unbonding_len
            // epochs before this current slash
            // TODO: understand this better (from Informal)
            // TODO: do bounds of this need to be changed with a +/- 1??
            if slashed_amount.epoch + params.unbonding_len < infraction_epoch {
                updated_amount = updated_amount
                    .checked_sub(slashed_amount.amount)
                    .unwrap_or_default();
                computed_to_remove.insert(Reverse(ix));
            }
        }
        // Invariant: `computed_to_remove` must be in reverse ord to avoid
        // left-shift of the `computed_amounts` after call to `remove`
        // invalidating the rest of the indices.
        for item in computed_to_remove {
            computed_amounts.remove(item.0);
        }
        computed_amounts.push(SlashedAmount {
            amount: decimal_mult_amount(slash.rate, updated_amount),
            epoch: infraction_epoch,
        });
    }
    println!("Finished loop over slashes in `get_slashed_amount`");
    println!("Updated amount: {:?}", &updated_amount);
    println!("Computed amounts: {:?}", &computed_amounts);

    let total_computed_amounts = computed_amounts
        .into_iter()
        .map(|slashed| slashed.amount)
        .sum();

    let final_amount = updated_amount
        .checked_sub(total_computed_amounts)
        .unwrap_or_default();

    Ok(final_amount.change())
}

fn update_unbond<S>(
    handle: &Unbonds,
    storage: &mut S,
    withdraw_epoch: &Epoch,
    start_epoch: &Epoch,
    amount: token::Amount,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let current = handle
        .at(withdraw_epoch)
        .get(storage, start_epoch)?
        .unwrap_or_default();
    handle.at(withdraw_epoch).insert(
        storage,
        *start_epoch,
        current + amount,
    )?;
    Ok(())
}

/// Initialize data for a new validator.
pub fn become_validator<S>(
    storage: &mut S,
    params: &PosParams,
    address: &Address,
    consensus_key: &common::PublicKey,
    current_epoch: Epoch,
    commission_rate: Decimal,
    max_commission_rate_change: Decimal,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    // This will fail if the key is already being used
    try_insert_consensus_key(storage, consensus_key)?;

    let pipeline_epoch = current_epoch + params.pipeline_len;
    validator_addresses_handle()
        .at(&pipeline_epoch)
        .insert(storage, address.clone())?;

    // Non-epoched validator data
    write_validator_address_raw_hash(storage, address, consensus_key)?;
    write_validator_max_commission_rate_change(
        storage,
        address,
        max_commission_rate_change,
    )?;

    // Epoched validator data
    validator_consensus_key_handle(address).set(
        storage,
        consensus_key.clone(),
        current_epoch,
        params.pipeline_len,
    )?;
    validator_commission_rate_handle(address).set(
        storage,
        commission_rate,
        current_epoch,
        params.pipeline_len,
    )?;
    validator_deltas_handle(address).set(
        storage,
        token::Change::default(),
        current_epoch,
        params.pipeline_len,
    )?;

    let stake = token::Amount::default();

    insert_validator_into_validator_set(
        storage,
        params,
        address,
        stake,
        current_epoch,
        params.pipeline_len,
    )?;
    Ok(())
}

/// Withdraw tokens from those that have been unbonded from proof-of-stake
pub fn withdraw_tokens<S>(
    storage: &mut S,
    source: Option<&Address>,
    validator: &Address,
    current_epoch: Epoch,
) -> storage_api::Result<token::Amount>
where
    S: StorageRead + StorageWrite,
{
    println!("Withdrawing tokens in epoch {current_epoch}");
    let params = read_pos_params(storage)?;
    let source = source.unwrap_or(validator);

    let unbond_handle = unbond_handle(source, validator);
    if unbond_handle.is_empty(storage)? {
        return Err(WithdrawError::NoUnbondFound(BondId {
            source: source.clone(),
            validator: validator.clone(),
        })
        .into());
    }

    let mut withdrawable_amount = token::Amount::default();
    // (withdraw_epoch, start_epoch)
    let mut unbonds_to_remove: Vec<(Epoch, Epoch)> = Vec::new();

    for unbond in unbond_handle.iter(storage)? {
        // println!("\nUNBOND ITER\n");
        let (
            NestedSubKey::Data {
                key: withdraw_epoch,
                nested_sub_key: SubKey::Data(start_epoch),
            },
            amount,
        ) = unbond?;

        tracing::debug!(
            "Unbond delta ({start_epoch}..{withdraw_epoch}), amount {amount}",
        );

        // TODO: adding slash rates in same epoch, applying cumulatively in dif
        // epochs
        if withdraw_epoch > current_epoch {
            tracing::debug!("Not yet withdrawable");
            continue;
        }
        let mut slashes_for_this_unbond = Vec::<Slash>::new();
        for slash in validator_slashes_handle(validator).iter(storage)? {
            let slash = slash?;
            if start_epoch <= slash.epoch
                && slash.epoch < withdraw_epoch - params.unbonding_len
            {
                slashes_for_this_unbond.push(slash);
            }
        }
        let amount_after_slashing =
            get_slashed_amount(&params, amount, &mut slashes_for_this_unbond)?;

        withdrawable_amount +=
            token::Amount::from_change(amount_after_slashing);
        unbonds_to_remove.push((withdraw_epoch, start_epoch));
    }
    tracing::debug!("Withdrawing total {withdrawable_amount}");

    // Remove the unbond data from storage
    for (withdraw_epoch, start_epoch) in unbonds_to_remove {
        tracing::debug!("Remove ({start_epoch}..{withdraw_epoch}) from unbond");
        unbond_handle
            .at(&withdraw_epoch)
            .remove(storage, &start_epoch)?;
        // TODO: check if the `end_epoch` layer is now empty and remove it if
        // so, may need to implement remove/delete for nested map
    }

    // Transfer the tokens from the PoS address back to the source
    let staking_token = staking_token_address(storage);
    transfer_tokens(
        storage,
        &staking_token,
        withdrawable_amount,
        &ADDRESS,
        source,
    )?;

    Ok(withdrawable_amount)
}

/// Change the commission rate of a validator
pub fn change_validator_commission_rate<S>(
    storage: &mut S,
    validator: &Address,
    new_rate: Decimal,
    current_epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    if new_rate < Decimal::ZERO {
        return Err(CommissionRateChangeError::NegativeRate(
            new_rate,
            validator.clone(),
        )
        .into());
    }

    let max_change =
        read_validator_max_commission_rate_change(storage, validator)?;
    if max_change.is_none() {
        return Err(CommissionRateChangeError::NoMaxSetInStorage(
            validator.clone(),
        )
        .into());
    }

    let params = read_pos_params(storage)?;
    let commission_handle = validator_commission_rate_handle(validator);
    let pipeline_epoch = current_epoch + params.pipeline_len;

    let rate_at_pipeline = commission_handle
        .get(storage, pipeline_epoch, &params)?
        .expect("Could not find a rate in given epoch");
    if new_rate == rate_at_pipeline {
        return Ok(());
    }
    let rate_before_pipeline = commission_handle
        .get(storage, pipeline_epoch.prev(), &params)?
        .expect("Could not find a rate in given epoch");
    let change_from_prev = new_rate - rate_before_pipeline;
    if change_from_prev.abs() > max_change.unwrap() {
        return Err(CommissionRateChangeError::RateChangeTooLarge(
            change_from_prev,
            validator.clone(),
        )
        .into());
    }

    commission_handle.set(storage, new_rate, current_epoch, params.pipeline_len)
}

/// Transfer tokens between accounts
/// TODO: may want to move this into core crate
pub fn transfer_tokens<S>(
    storage: &mut S,
    token: &Address,
    amount: token::Amount,
    src: &Address,
    dest: &Address,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let src_key = token::balance_key(token, src);
    let dest_key = token::balance_key(token, dest);
    if let Some(mut src_balance) = storage.read::<token::Amount>(&src_key)? {
        // let mut src_balance: token::Amount =
        //     decode(src_balance).unwrap_or_default();
        if src_balance < amount {
            tracing::error!(
                "PoS system transfer error, the source doesn't have \
                 sufficient balance. It has {}, but {} is required",
                src_balance,
                amount
            );
        }
        src_balance.spend(&amount);
        let mut dest_balance = storage
            .read::<token::Amount>(&dest_key)?
            .unwrap_or_default();

        // let dest_balance = storage.read_bytes(&dest_key).unwrap_or_default();
        // let mut dest_balance: token::Amount = dest_balance
        //     .and_then(|b| decode(b).ok())
        //     .unwrap_or_default();
        dest_balance.receive(&amount);
        storage
            .write(&src_key, src_balance)
            .expect("Unable to write token balance for PoS system");
        storage
            .write(&dest_key, dest_balance)
            .expect("Unable to write token balance for PoS system");
    } else {
        tracing::error!("PoS system transfer error, the source has no balance");
    }
    Ok(())
}

/// Check if the given consensus key is already being used to ensure uniqueness.
///
/// If it's not being used, it will be inserted into the set that's being used
/// for this. If it's already used, this will return an Error.
pub fn try_insert_consensus_key<S>(
    storage: &mut S,
    consensus_key: &common::PublicKey,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let key = consensus_keys_key();
    LazySet::open(key).try_insert(storage, consensus_key.clone())
}

/// Check if the given consensus key is already being used to ensure uniqueness.
pub fn is_consensus_key_used<S>(
    storage: &S,
    consensus_key: &common::PublicKey,
) -> storage_api::Result<bool>
where
    S: StorageRead,
{
    let key = consensus_keys_key();
    let handle = LazySet::open(key);
    handle.contains(storage, consensus_key)
}

/// Get the total bond amount, including slashes, for a given bond ID and epoch
pub fn bond_amount<S>(
    storage: &S,
    params: &PosParams,
    bond_id: &BondId,
    epoch: Epoch,
) -> storage_api::Result<(token::Amount, token::Amount)>
where
    S: StorageRead,
{
    // TODO: review this logic carefully, do cubic slashing, apply rewards
    let slashes = find_validator_slashes(storage, &bond_id.validator)?;
    let bonds =
        bond_handle(&bond_id.source, &bond_id.validator).get_data_handler();
    let mut total = token::Amount::default();
    let mut total_active = token::Amount::default();
    for next in bonds.iter(storage)? {
        let (bond_epoch, delta) = next?;
        // if bond_epoch > epoch {
        //     break;
        // }

        // TODO: do we need to consider the adjusted amounts of previous bonds
        // and their slashes when iterating?
        for slash in &slashes {
            let Slash {
                epoch: slash_epoch,
                block_height: _,
                r#type: slash_type,
                rate: _,
            } = slash;
            if *slash_epoch < bond_epoch {
                continue;
            }
            // TODO: consider edge cases with the cubic slashing window
            let cubic_rate = get_final_cubic_slash_rate(
                storage,
                params,
                *slash_epoch,
                *slash_type,
            )?;
            let current_slashed = decimal_mult_i128(cubic_rate, delta);
            let delta = token::Amount::from_change(delta - current_slashed);
            total += delta;
            if bond_epoch <= epoch {
                total_active += delta;
            }
        }
    }
    Ok((total, total_active))
}

/// Communicate imminent validator set updates to Tendermint. This function is
/// called two blocks before the start of a new epoch because Tendermint
/// validator updates become active two blocks after the updates are submitted.
pub fn validator_set_update_tendermint<S, T>(
    storage: &S,
    params: &PosParams,
    current_epoch: Epoch,
    f: impl FnMut(ValidatorSetUpdate) -> T,
) -> storage_api::Result<Vec<T>>
where
    S: StorageRead,
{
    // Because this is called 2 blocks before a start on an epoch, we're gonna
    // give Tendermint updates for the next epoch
    let next_epoch: Epoch = current_epoch.next();

    let cur_consensus_validators =
        consensus_validator_set_handle().at(&next_epoch);
    let prev_consensus_validators =
        consensus_validator_set_handle().at(&current_epoch);

    let consensus_validators = cur_consensus_validators
        .iter(storage)?
        .filter_map(|validator| {
            let (
                NestedSubKey::Data {
                    key: cur_stake,
                    nested_sub_key: _,
                },
                address,
            ) = validator.unwrap();

            tracing::debug!(
                "Consensus validator address {address}, stake {cur_stake}"
            );

            // Check if the validator was consensus in the previous epoch with
            // the same stake
            // Look up previous state and prev and current voting powers
            if !prev_consensus_validators.is_empty(storage).unwrap() {
                let prev_state = validator_state_handle(&address)
                    .get(storage, current_epoch, params)
                    .unwrap();
                let prev_tm_voting_power = Lazy::new(|| {
                    let prev_validator_stake =
                        validator_deltas_handle(&address)
                            .get_sum(storage, current_epoch, params)
                            .unwrap()
                            .map(token::Amount::from_change)
                            .unwrap_or_default();
                    into_tm_voting_power(
                        params.tm_votes_per_token,
                        prev_validator_stake,
                    )
                });
                let cur_tm_voting_power = Lazy::new(|| {
                    into_tm_voting_power(params.tm_votes_per_token, cur_stake)
                });

                // If it was in `Consensus` before and voting power has not
                // changed, skip the update
                if matches!(prev_state, Some(ValidatorState::Consensus))
                    && *prev_tm_voting_power == *cur_tm_voting_power
                {
                    tracing::debug!(
                        "skipping validator update, {address} is in consensus \
                         set but voting power hasn't changed"
                    );
                    return None;
                }

                // If both previous and current voting powers are 0, skip
                // update
                if *prev_tm_voting_power == 0 && *cur_tm_voting_power == 0 {
                    tracing::info!(
                        "skipping validator update, {address} is in consensus \
                         set but without voting power"
                    );
                    return None;
                }
            }
            let consensus_key = validator_consensus_key_handle(&address)
                .get(storage, next_epoch, params)
                .unwrap()
                .unwrap();
            tracing::debug!(
                "{address} consensus key {}",
                consensus_key.tm_raw_hash()
            );
            Some(ValidatorSetUpdate::Consensus(ConsensusValidator {
                consensus_key,
                bonded_stake: cur_stake.into(),
            }))
        });
    let cur_below_capacity_validators =
        below_capacity_validator_set_handle().at(&next_epoch);
    let prev_below_capacity_vals =
        below_capacity_validator_set_handle().at(&current_epoch);

    let below_capacity_validators = cur_below_capacity_validators
        .iter(storage)
        .unwrap()
        .filter_map(|validator| {
            let (
                NestedSubKey::Data {
                    key: cur_stake,
                    nested_sub_key: _,
                },
                address,
            ) = validator.unwrap();
            let cur_stake = token::Amount::from(cur_stake);

            tracing::debug!(
                "Below-capacity validator address {address}, stake {cur_stake}"
            );

            let prev_validator_stake = validator_deltas_handle(&address)
                .get_sum(storage, current_epoch, params)
                .unwrap()
                .map(token::Amount::from_change)
                .unwrap_or_default();
            let prev_tm_voting_power = into_tm_voting_power(
                params.tm_votes_per_token,
                prev_validator_stake,
            );

            // If the validator previously had no voting power, it wasn't in
            // tendermint set and we have to skip it.
            if prev_tm_voting_power == 0 {
                tracing::debug!(
                    "skipping validator update {address}, it's inactive and \
                     previously had no voting power"
                );
                return None;
            }

            if !prev_below_capacity_vals.is_empty(storage).unwrap() {
                // Look up the previous state
                let prev_state = validator_state_handle(&address)
                    .get(storage, current_epoch, params)
                    .unwrap();
                // If the `prev_state.is_none()`, it's a new validator that
                // is `BelowCapacity`, so no update is needed. If it
                // previously was `BelowCapacity` there's no update needed
                // either.
                if !matches!(prev_state, Some(ValidatorState::Consensus)) {
                    tracing::debug!(
                        "skipping validator update, {address} is not and \
                         wasn't previously in consensus set"
                    );
                    return None;
                }
            }

            let consensus_key = validator_consensus_key_handle(&address)
                .get(storage, next_epoch, params)
                .unwrap()
                .unwrap();
            tracing::debug!(
                "{address} consensus key {}",
                consensus_key.tm_raw_hash()
            );
            Some(ValidatorSetUpdate::Deactivated(consensus_key))
        });
    Ok(consensus_validators
        .chain(below_capacity_validators)
        .map(f)
        .collect())
}

/// Find all validators to which a given bond `owner` (or source) has a
/// delegation
pub fn find_delegation_validators<S>(
    storage: &S,
    owner: &Address,
) -> storage_api::Result<HashSet<Address>>
where
    S: StorageRead,
{
    let bonds_prefix = bonds_for_source_prefix(owner);
    let mut delegations: HashSet<Address> = HashSet::new();

    for iter_result in storage_api::iter_prefix_bytes(storage, &bonds_prefix)? {
        let (key, _bond_bytes) = iter_result?;
        let validator_address = get_validator_address_from_bond(&key)
            .ok_or_else(|| {
                storage_api::Error::new_const(
                    "Delegation key should contain validator address.",
                )
            })?;
        delegations.insert(validator_address);
    }
    Ok(delegations)
}

/// Find all validators to which a given bond `owner` (or source) has a
/// delegation with the amount
pub fn find_delegations<S>(
    storage: &S,
    owner: &Address,
    epoch: &Epoch,
) -> storage_api::Result<HashMap<Address, token::Amount>>
where
    S: StorageRead,
{
    let bonds_prefix = bonds_for_source_prefix(owner);
    let params = read_pos_params(storage)?;
    let mut delegations: HashMap<Address, token::Amount> = HashMap::new();

    for iter_result in storage_api::iter_prefix_bytes(storage, &bonds_prefix)? {
        let (key, _bond_bytes) = iter_result?;
        let validator_address = get_validator_address_from_bond(&key)
            .ok_or_else(|| {
                storage_api::Error::new_const(
                    "Delegation key should contain validator address.",
                )
            })?;
        let amount = bond_handle(owner, &validator_address)
            .get_sum(storage, *epoch, &params)?
            .unwrap_or_default();
        delegations
            .insert(validator_address, token::Amount::from_change(amount));
    }
    Ok(delegations)
}

/// Find PoS slashes applied to a validator, if any
pub fn find_validator_slashes<S>(
    storage: &S,
    validator: &Address,
) -> storage_api::Result<Vec<Slash>>
where
    S: StorageRead,
{
    validator_slashes_handle(validator).iter(storage)?.collect()
}

/// Find raw bond deltas for the given source and validator address.
pub fn find_bonds<S>(
    storage: &S,
    source: &Address,
    validator: &Address,
) -> storage_api::Result<BTreeMap<Epoch, token::Change>>
where
    S: StorageRead,
{
    bond_handle(source, validator)
        .get_data_handler()
        .iter(storage)?
        .collect()
}

/// Find raw unbond deltas for the given source and validator address.
pub fn find_unbonds<S>(
    storage: &S,
    source: &Address,
    validator: &Address,
) -> storage_api::Result<BTreeMap<(Epoch, Epoch), token::Amount>>
where
    S: StorageRead,
{
    unbond_handle(source, validator)
        .iter(storage)?
        .map(|next_result| {
            let (
                NestedSubKey::Data {
                    key: withdraw_epoch,
                    nested_sub_key: SubKey::Data(start_epoch),
                },
                amount,
            ) = next_result?;
            Ok(((start_epoch, withdraw_epoch), amount))
        })
        .collect()
}

/// Collect the details of all bonds and unbonds that match the source and
/// validator arguments. If either source or validator is `None`, then grab the
/// information for all sources or validators, respectively.
pub fn bonds_and_unbonds<S>(
    storage: &S,
    source: Option<Address>,
    validator: Option<Address>,
) -> storage_api::Result<BondsAndUnbondsDetails>
where
    S: StorageRead,
{
    let params = read_pos_params(storage)?;

    match (source.clone(), validator.clone()) {
        (Some(source), Some(validator)) => {
            find_bonds_and_unbonds_details(storage, &params, source, validator)
        }
        _ => {
            get_multiple_bonds_and_unbonds(storage, &params, source, validator)
        }
    }
}

/// Find all slashes and the associated validators in the PoS system
pub fn find_all_slashes<S>(
    storage: &S,
) -> storage_api::Result<HashMap<Address, Vec<Slash>>>
where
    S: StorageRead,
{
    let mut slashes: HashMap<Address, Vec<Slash>> = HashMap::new();
    let slashes_iter = storage_api::iter_prefix_bytes(
        storage,
        &slashes_prefix(),
    )?
    .filter_map(|result| {
        if let Ok((key, val_bytes)) = result {
            if let Some(validator) = is_validator_slashes_key(&key) {
                let slash: Slash =
                    BorshDeserialize::try_from_slice(&val_bytes).ok()?;
                return Some((validator, slash));
            }
        }
        None
    });

    slashes_iter.for_each(|(address, slash)| match slashes.get(&address) {
        Some(vec) => {
            let mut vec = vec.clone();
            vec.push(slash);
            slashes.insert(address, vec);
        }
        None => {
            slashes.insert(address, vec![slash]);
        }
    });
    Ok(slashes)
}

fn get_multiple_bonds_and_unbonds<S>(
    storage: &S,
    params: &PosParams,
    source: Option<Address>,
    validator: Option<Address>,
) -> storage_api::Result<BondsAndUnbondsDetails>
where
    S: StorageRead,
{
    debug_assert!(
        source.is_none() || validator.is_none(),
        "Use `find_bonds_and_unbonds_details` when full bond ID is known"
    );
    let mut slashes_cache = HashMap::<Address, Vec<Slash>>::new();
    // Applied slashes grouped by validator address
    let mut applied_slashes = HashMap::<Address, HashSet<Slash>>::new();

    // TODO: if validator is `Some`, look-up all its bond owners (including
    // self-bond, if any) first

    let prefix = match source.as_ref() {
        Some(source) => bonds_for_source_prefix(source),
        None => bonds_prefix(),
    };
    // We have to iterate raw bytes, cause the epoched data `last_update` field
    // gets matched here too
    let mut raw_bonds = storage_api::iter_prefix_bytes(storage, &prefix)?
        .filter_map(|result| {
            if let Ok((key, val_bytes)) = result {
                if let Some((bond_id, start)) = is_bond_key(&key) {
                    if source.is_some()
                        && source.as_ref().unwrap() != &bond_id.source
                    {
                        return None;
                    }
                    if validator.is_some()
                        && validator.as_ref().unwrap() != &bond_id.validator
                    {
                        return None;
                    }
                    let change: token::Change =
                        BorshDeserialize::try_from_slice(&val_bytes).ok()?;
                    if change == 0 {
                        return None;
                    }
                    return Some((bond_id, start, change));
                }
            }
            None
        });

    let prefix = match source.as_ref() {
        Some(source) => unbonds_for_source_prefix(source),
        None => unbonds_prefix(),
    };
    let mut raw_unbonds = storage_api::iter_prefix_bytes(storage, &prefix)?
        .filter_map(|result| {
            if let Ok((key, val_bytes)) = result {
                if let Some((bond_id, start, withdraw)) = is_unbond_key(&key) {
                    if source.is_some()
                        && source.as_ref().unwrap() != &bond_id.source
                    {
                        return None;
                    }
                    if validator.is_some()
                        && validator.as_ref().unwrap() != &bond_id.validator
                    {
                        return None;
                    }
                    match (source.clone(), validator.clone()) {
                        (None, Some(validator)) => {
                            if bond_id.validator != validator {
                                return None;
                            }
                        }
                        (Some(owner), None) => {
                            if owner != bond_id.source {
                                return None;
                            }
                        }
                        _ => {}
                    }
                    let amount: token::Amount =
                        BorshDeserialize::try_from_slice(&val_bytes).ok()?;
                    return Some((bond_id, start, withdraw, amount));
                }
            }
            None
        });

    let mut bonds_and_unbonds =
        HashMap::<BondId, (Vec<BondDetails>, Vec<UnbondDetails>)>::new();

    raw_bonds.try_for_each(|(bond_id, start, change)| {
        if !slashes_cache.contains_key(&bond_id.validator) {
            let slashes = find_validator_slashes(storage, &bond_id.validator)?;
            slashes_cache.insert(bond_id.validator.clone(), slashes);
        }
        let slashes = slashes_cache
            .get(&bond_id.validator)
            .expect("We must have inserted it if it's not cached already");
        let validator = bond_id.validator.clone();
        let (bonds, _unbonds) = bonds_and_unbonds.entry(bond_id).or_default();
        bonds.push(make_bond_details(
            storage,
            params,
            &validator,
            change,
            start,
            slashes,
            &mut applied_slashes,
        ));
        Ok::<_, storage_api::Error>(())
    })?;

    raw_unbonds.try_for_each(|(bond_id, start, withdraw, amount)| {
        if !slashes_cache.contains_key(&bond_id.validator) {
            let slashes = find_validator_slashes(storage, &bond_id.validator)?;
            slashes_cache.insert(bond_id.validator.clone(), slashes);
        }
        let slashes = slashes_cache
            .get(&bond_id.validator)
            .expect("We must have inserted it if it's not cached already");
        let validator = bond_id.validator.clone();
        let (_bonds, unbonds) = bonds_and_unbonds.entry(bond_id).or_default();
        unbonds.push(make_unbond_details(
            storage,
            params,
            &validator,
            amount,
            (start, withdraw),
            slashes,
            &mut applied_slashes,
        ));
        Ok::<_, storage_api::Error>(())
    })?;

    Ok(bonds_and_unbonds
        .into_iter()
        .map(|(bond_id, (bonds, unbonds))| {
            let details = BondsAndUnbondsDetail {
                bonds,
                unbonds,
                slashes: applied_slashes
                    .get(&bond_id.validator)
                    .cloned()
                    .unwrap_or_default(),
            };
            (bond_id, details)
        })
        .collect())
}

fn find_bonds_and_unbonds_details<S>(
    storage: &S,
    params: &PosParams,
    source: Address,
    validator: Address,
) -> storage_api::Result<BondsAndUnbondsDetails>
where
    S: StorageRead,
{
    let slashes = find_validator_slashes(storage, &validator)?;
    let mut applied_slashes = HashMap::<Address, HashSet<Slash>>::new();

    let bonds = find_bonds(storage, &source, &validator)?
        .into_iter()
        .filter(|(_start, change)| *change > token::Change::default())
        .map(|(start, change)| {
            make_bond_details(
                storage,
                params,
                &validator,
                change,
                start,
                &slashes,
                &mut applied_slashes,
            )
        })
        .collect();

    let unbonds = find_unbonds(storage, &source, &validator)?
        .into_iter()
        .map(|(epoch_range, change)| {
            make_unbond_details(
                storage,
                params,
                &validator,
                change,
                epoch_range,
                &slashes,
                &mut applied_slashes,
            )
        })
        .collect();

    let details = BondsAndUnbondsDetail {
        bonds,
        unbonds,
        slashes: applied_slashes.get(&validator).cloned().unwrap_or_default(),
    };
    let bond_id = BondId { source, validator };
    Ok(HashMap::from_iter([(bond_id, details)]))
}

// TODO: update for cubic slashing
fn make_bond_details<S>(
    storage: &S,
    params: &PosParams,
    validator: &Address,
    change: token::Change,
    start: Epoch,
    slashes: &[Slash],
    applied_slashes: &mut HashMap<Address, HashSet<Slash>>,
) -> BondDetails
where
    S: StorageRead,
{
    let amount = token::Amount::from_change(change);
    let slashed_amount =
        slashes
            .iter()
            .fold(None, |acc: Option<token::Amount>, slash| {
                if slash.epoch >= start {
                    let slash_rate = get_final_cubic_slash_rate(
                        storage,
                        params,
                        slash.epoch,
                        slash.r#type,
                    )
                    .unwrap();
                    let validator_slashes =
                        applied_slashes.entry(validator.clone()).or_default();
                    if !validator_slashes.contains(slash) {
                        validator_slashes.insert(slash.clone());
                    }
                    return Some(
                        acc.unwrap_or_default()
                            + mult_change_to_amount(slash_rate, change),
                    );
                }
                None
            });
    BondDetails {
        start,
        amount,
        slashed_amount,
    }
}

// TODO: update for cubic slashing
fn make_unbond_details<S>(
    storage: &S,
    params: &PosParams,
    validator: &Address,
    amount: token::Amount,
    (start, withdraw): (Epoch, Epoch),
    slashes: &[Slash],
    applied_slashes: &mut HashMap<Address, HashSet<Slash>>,
) -> UnbondDetails
where
    S: StorageRead,
{
    let slashed_amount =
        slashes
            .iter()
            .fold(None, |acc: Option<token::Amount>, slash| {
                if slash.epoch >= start
                    && slash.epoch
                        < withdraw
                            .checked_sub(Epoch(params.unbonding_len))
                            .unwrap_or_default()
                {
                    let slash_rate = get_final_cubic_slash_rate(
                        storage,
                        params,
                        slash.epoch,
                        slash.r#type,
                    )
                    .unwrap();
                    let validator_slashes =
                        applied_slashes.entry(validator.clone()).or_default();
                    if !validator_slashes.contains(slash) {
                        validator_slashes.insert(slash.clone());
                    }
                    return Some(
                        acc.unwrap_or_default()
                            + decimal_mult_amount(slash_rate, amount),
                    );
                }
                None
            });
    UnbondDetails {
        start,
        withdraw,
        amount,
        slashed_amount,
    }
}

/// Tally a running sum of the fraction of rewards owed to each validator in
/// the consensus set. This is used to keep track of the rewards due to each
/// consensus validator over the lifetime of an epoch.
pub fn log_block_rewards<S>(
    storage: &mut S,
    epoch: impl Into<Epoch>,
    proposer_address: &Address,
    votes: Vec<VoteInfo>,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    // The votes correspond to the last committed block (n-1 if we are
    // finalizing block n)

    let epoch: Epoch = epoch.into();
    let params = read_pos_params(storage)?;
    let consensus_validators = consensus_validator_set_handle().at(&epoch);

    // Get total stake of the consensus validator set
    let mut total_consensus_stake = 0_u64;
    for validator in consensus_validators.iter(storage)? {
        let (
            NestedSubKey::Data {
                key: amount,
                nested_sub_key: _,
            },
            _address,
        ) = validator?;
        total_consensus_stake += u64::from(amount);
    }

    // Get set of signing validator addresses and the combined stake of
    // these signers
    let mut signer_set: HashSet<Address> = HashSet::new();
    let mut total_signing_stake: u64 = 0;
    for VoteInfo {
        validator_address,
        validator_vp,
    } in votes
    {
        if validator_vp == 0 {
            continue;
        }
        // Ensure that the validator is not currently jailed or other
        let state = validator_state_handle(&validator_address)
            .get(storage, epoch, &params)?;
        if state != Some(ValidatorState::Consensus) {
            continue;
        }

        // Ensure TM stake updates properly with a debug_assert
        if cfg!(debug_assertions) {
            let stake_from_deltas = read_validator_stake(
                storage,
                &params,
                &validator_address,
                epoch,
            )?
            .unwrap_or_default();
            debug_assert_eq!(
                stake_from_deltas,
                token::Amount::from(validator_vp)
            );
        }

        signer_set.insert(validator_address);
        total_signing_stake += validator_vp;
    }

    // Get the block rewards coefficients (proposing, signing/voting,
    // consensus set status)
    let consensus_stake: Decimal = total_consensus_stake.into();
    let signing_stake: Decimal = total_signing_stake.into();
    let rewards_calculator = PosRewardsCalculator {
        proposer_reward: params.block_proposer_reward,
        signer_reward: params.block_vote_reward,
        signing_stake: total_signing_stake,
        total_stake: total_consensus_stake,
    };
    let coeffs = match rewards_calculator.get_reward_coeffs() {
        Ok(coeffs) => coeffs,
        Err(_) => return Err(InflationError::Error.into()),
    };

    // println!(
    //     "TOTAL SIGNING STAKE (LOGGING BLOCK REWARDS) = {}",
    //     signing_stake
    // );

    // Compute the fractional block rewards for each consensus validator and
    // update the reward accumulators
    let mut values: HashMap<Address, Decimal> = HashMap::new();
    for validator in consensus_validators.iter(storage)? {
        let (
            NestedSubKey::Data {
                key: stake,
                nested_sub_key: _,
            },
            address,
        ) = validator?;

        // TODO:
        // When below-threshold validator set is added, this shouldn't be needed
        // anymore since some minimal stake will be required to be in at least
        // the consensus set
        if stake == token::Amount::default() {
            continue;
        }

        let mut rewards_frac = Decimal::default();
        let stake: Decimal = u64::from(stake).into();
        // println!(
        //     "NAMADA VALIDATOR STAKE (LOGGING BLOCK REWARDS) OF EPOCH {} =
        // {}",     epoch, stake
        // );

        // Proposer reward
        if address == *proposer_address {
            rewards_frac += coeffs.proposer_coeff;
        }
        // Signer reward
        if signer_set.contains(&address) {
            let signing_frac = stake / signing_stake;
            rewards_frac += coeffs.signer_coeff * signing_frac;
        }
        // Consensus validator reward
        rewards_frac += coeffs.active_val_coeff * (stake / consensus_stake);

        // Update the rewards accumulator
        let prev = rewards_accumulator_handle()
            .get(storage, &address)?
            .unwrap_or_default();
        values.insert(address, prev + rewards_frac);
    }
    for (address, value) in values.into_iter() {
        rewards_accumulator_handle().insert(storage, address, value)?;
    }

    Ok(())
}

/// Calculate the cubic slashing rate using all slashes within a window around
/// the given infraction epoch
pub fn compute_cubic_slash_rate<S>(
    storage: &S,
    params: &PosParams,
    infraction_epoch: Epoch,
) -> storage_api::Result<Decimal>
where
    S: StorageRead,
{
    println!("COMPUTING CUBIC SLASH RATE");
    let mut sum_vp_fraction = Decimal::ZERO;
    let start_epoch = infraction_epoch
        .sub_or_default(Epoch(params.cubic_slashing_window_length));
    let end_epoch = infraction_epoch + params.cubic_slashing_window_length;

    for epoch in Epoch::iter_bounds_inclusive(start_epoch, end_epoch) {
        let total_stake =
            Decimal::from(read_total_stake(storage, params, epoch)?);

        let processing_epoch = epoch + params.unbonding_len + 1_u64;
        let slashes = enqueued_slashes_handle().at(&processing_epoch);
        let infracting_stake = slashes
            .iter(storage)?
            .map(|res| {
                let (
                    NestedSubKey::Data {
                        key: validator,
                        nested_sub_key: _,
                    },
                    _slash,
                ) = res?;

                let validator_stake =
                    read_validator_stake(storage, params, &validator, epoch)?
                        .unwrap_or_default();

                Ok(Decimal::from(validator_stake))
                // TODO: does something more complex need to be done
                // here in the event some of these slashes correspond to
                // the same validator?
            })
            .sum::<storage_api::Result<Decimal>>()?;
        sum_vp_fraction += infracting_stake / total_stake;
    }

    // TODO: make sure `sum_vp_fraction` does not exceed 1/3 or handle with care
    // another way
    Ok(dec!(9) * sum_vp_fraction * sum_vp_fraction)
}

/// Get final cubic slashing rate that is bound from below by some minimum value
/// and capped at 100%
pub fn get_final_cubic_slash_rate<S>(
    storage: &S,
    params: &PosParams,
    infraction_epoch: Epoch,
    current_slash_type: SlashType,
) -> storage_api::Result<Decimal>
where
    S: StorageRead,
{
    let cubic_rate =
        compute_cubic_slash_rate(storage, params, infraction_epoch)?;
    // Need some truncation right now to max the rate at 100%
    let rate = cubic_rate
        .clamp(current_slash_type.get_slash_rate(params), Decimal::ONE);
    Ok(rate)
}

/// Record a slash for a misbehavior that has been received from Tendermint and
/// then jail the validator, removing it from the validator set. The slash rate
/// will be computed at a later epoch.
pub fn slash<S>(
    storage: &mut S,
    params: &PosParams,
    current_epoch: Epoch,
    evidence_epoch: Epoch,
    evidence_block_height: impl Into<u64>,
    slash_type: SlashType,
    validator: &Address,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    println!("SLASHING ON NEW EVIDENCE FROM {}", validator);
    println!(
        "Current state = {:?}",
        validator_state_handle(validator)
            .get(storage, current_epoch, params)
            .unwrap()
            .unwrap()
    );

    // Debugging
    // println!("PRE Validator Set");
    // for offset in 0..=params.pipeline_len {
    //     println!("Epoch {}", current_epoch.0 + offset);
    //     for wv in read_consensus_validator_set_addresses_with_stake(
    //         storage,
    //         current_epoch + offset,
    //     )? {
    //         println!(
    //             "Consensus val {}, stake {}, state {:?}",
    //             &wv.address,
    //             u64::from(wv.bonded_stake),
    //             validator_state_handle(&wv.address)
    //                 .get(storage, current_epoch + offset, params)
    //                 .unwrap()
    //         );
    //     }
    //     for wv in read_below_capacity_validator_set_addresses_with_stake(
    //         storage,
    //         current_epoch + offset,
    //     )? {
    //         println!(
    //             "Below-cap val {}, stake {}, state {:?}",
    //             &wv.address,
    //             u64::from(wv.bonded_stake),
    //             validator_state_handle(&wv.address).get(
    //                 storage,
    //                 current_epoch + offset,
    //                 params
    //             )
    //         );
    //     }
    // }

    let evidence_block_height: u64 = evidence_block_height.into();
    let slash = Slash {
        epoch: evidence_epoch,
        block_height: evidence_block_height,
        r#type: slash_type,
        rate: Decimal::ZERO, // Let the rate be 0 initially before processing
    };
    // Need `+1` because we process at the beginning of a new epoch
    let processing_epoch = evidence_epoch + params.unbonding_len + 1_u64;
    let pipeline_epoch = current_epoch + params.pipeline_len;

    // Add the slash to the list of enqueued slashes to be processed at a later
    // epoch
    enqueued_slashes_handle()
        .get_data_handler()
        .at(&processing_epoch)
        .at(validator)
        .push(storage, slash)?;

    // Update the most recent slash (infraction) epoch for the validator
    let last_slash_epoch = read_validator_last_slash_epoch(storage, validator)?;
    if last_slash_epoch.is_none()
        || evidence_epoch.0 > last_slash_epoch.unwrap_or_default().0
    {
        write_validator_last_slash_epoch(storage, validator, evidence_epoch)?;
    }

    // Remove the validator from the set starting at the next epoch and up thru
    // the pipeline epoch.
    for epoch in
        Epoch::iter_bounds_inclusive(current_epoch.next(), pipeline_epoch)
    {
        let prev_state = validator_state_handle(validator)
            .get(storage, epoch, params)?
            .expect("Expected to find a valid validator.");
        match prev_state {
            ValidatorState::Consensus => {
                println!("CONSENSUS");
                let amount_pre = validator_deltas_handle(validator)
                    .get_sum(storage, epoch, params)?
                    .unwrap_or_default();
                let val_position = validator_set_positions_handle()
                    .at(&epoch)
                    .get(storage, validator)?
                    .expect("Could not find validator's position in storage.");
                let _ = consensus_validator_set_handle()
                    .at(&epoch)
                    .at(&token::Amount::from_change(amount_pre))
                    .remove(storage, &val_position)?;

                // For the pipeline epoch only:
                // promote the next max inactive validator to the active
                // validator set at the pipeline offset
                if epoch == pipeline_epoch {
                    let below_capacity_handle =
                        below_capacity_validator_set_handle().at(&epoch);
                    let max_below_capacity_amount =
                        get_max_below_capacity_validator_amount(
                            &below_capacity_handle,
                            storage,
                        )?;
                    if let Some(max_below_capacity_amount) =
                        max_below_capacity_amount
                    {
                        let position_to_promote = find_first_position(
                            &below_capacity_handle
                                .at(&max_below_capacity_amount.into()),
                            storage,
                        )?
                        .expect("Should return a position.");
                        let max_bc_validator = below_capacity_handle
                            .at(&max_below_capacity_amount.into())
                            .remove(storage, &position_to_promote)?
                            .expect(
                                "Should have returned a removed validator.",
                            );
                        insert_validator_into_set(
                            &consensus_validator_set_handle()
                                .at(&epoch)
                                .at(&max_below_capacity_amount),
                            storage,
                            &epoch,
                            &max_bc_validator,
                        )?;
                        validator_state_handle(&max_bc_validator).set(
                            storage,
                            ValidatorState::Consensus,
                            current_epoch,
                            params.pipeline_len,
                        )?;
                    }
                }
            }
            ValidatorState::BelowCapacity => {
                println!("BELOW CAP");
                let amount_pre = validator_deltas_handle(validator)
                    .get_sum(storage, epoch, params)?
                    .unwrap_or_default();
                let val_position = validator_set_positions_handle()
                    .at(&epoch)
                    .get(storage, validator)?
                    .expect("Could not find validator's position in storage.");
                let _ = below_capacity_validator_set_handle()
                    .at(&epoch)
                    .at(&token::Amount::from_change(amount_pre).into())
                    .remove(storage, &val_position)?;
            }
            ValidatorState::Inactive => {
                println!("INACTIVE");
                panic!(
                    "SHouldn't be here - haven't implemented inactive vals yet"
                )
            }
            ValidatorState::Jailed => {
                println!("Validator already jailed");
                // return Ok(());
            }
        }
    }

    println!(
        "\nWRITING VALIDATOR {} STATE AS JAILED STARTING IN EPOCH {}\n",
        validator,
        current_epoch.next()
    );
    // Set the validator state as `Jailed` thru the pipeline epoch
    for offset in 1..=params.pipeline_len {
        validator_state_handle(validator).set(
            storage,
            ValidatorState::Jailed,
            current_epoch,
            offset,
        )?;
    }

    // Debugging
    // println!("POST Validator Set");

    // for offset in 0..=params.pipeline_len {
    //     println!("Epoch {}", current_epoch.0 + offset);
    //     for wv in read_consensus_validator_set_addresses_with_stake(
    //         storage,
    //         current_epoch + offset,
    //     )? {
    //         println!(
    //             "Consensus val {}, stake {}, state {:?}",
    //             &wv.address,
    //             u64::from(wv.bonded_stake),
    //             validator_state_handle(&wv.address).get(
    //                 storage,
    //                 current_epoch + offset,
    //                 params
    //             )
    //         );
    //     }
    //     for wv in read_below_capacity_validator_set_addresses_with_stake(
    //         storage,
    //         current_epoch + offset,
    //     )? {
    //         println!(
    //             "Below-cap val {}, stake {}, state {:?}",
    //             &wv.address,
    //             u64::from(wv.bonded_stake),
    //             validator_state_handle(&wv.address).get(
    //                 storage,
    //                 current_epoch + offset,
    //                 params
    //             )
    //         );
    //     }
    // }

    // No other actions are performed here until the epoch in which the slash is
    // processed.

    Ok(())
}

/// Process slashes that have been queued up after discovery. Calculate the
/// cubic slashing rate, store the finalized slashes, update the deltas, then
/// transfer slashed tokens from PoS to the Slash Pool. This function is called
/// at the beginning of the epoch that is `unbonding_length + 1` epochs after
/// the infraction epoch.
pub fn process_slashes<S>(
    storage: &mut S,
    current_epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let params = read_pos_params(storage)?;

    if current_epoch.0 < params.unbonding_len + 1 {
        return Ok(());
    }
    let infraction_epoch = current_epoch - params.unbonding_len - 1;

    // Slashes to be processed in the current epoch
    let enqueued_slashes = enqueued_slashes_handle().at(&current_epoch);
    if enqueued_slashes.is_empty(storage)? {
        println!("No slashes found");
        return Ok(());
    }
    println!("Found slashes");

    // Compute the cubic slash rate
    let cubic_slash_rate =
        compute_cubic_slash_rate(storage, &params, infraction_epoch)?;

    // Collect the enqueued slashes and update their rates
    let mut validators_and_slashes: HashMap<Address, Vec<Slash>> =
        HashMap::new();
    for enqueued_slash in enqueued_slashes.iter(storage)? {
        let (
            NestedSubKey::Data {
                key: validator,
                nested_sub_key: _,
            },
            enqueued_slash,
        ) = enqueued_slash?;
        debug_assert_eq!(enqueued_slash.epoch, infraction_epoch);

        let slash_rate = cmp::min(
            Decimal::ONE,
            cmp::max(
                enqueued_slash.r#type.get_slash_rate(&params),
                cubic_slash_rate,
            ),
        );
        let updated_slash = Slash {
            epoch: enqueued_slash.epoch,
            block_height: enqueued_slash.block_height,
            r#type: enqueued_slash.r#type,
            rate: slash_rate,
        };
        println!(
            "Processing slash for val {} committed in epoch {}, with rate {}",
            &validator, enqueued_slash.epoch, slash_rate
        );

        let cur_slashes = validators_and_slashes.entry(validator).or_default();
        cur_slashes.push(updated_slash);
    }

    // let mut total_slashed = token::Change::default();
    let mut deltas_for_update: HashMap<Address, Vec<(u64, token::Change)>> =
        HashMap::new();

    // Store the final processed slashes to their corresponding validators, then
    // update the deltas
    for (validator, enqueued_slashes) in validators_and_slashes.into_iter() {
        let validator_stake_at_infraction = read_validator_stake(
            storage,
            &params,
            &validator,
            infraction_epoch,
        )?
        .unwrap_or_default();

        println!(
            "Val {} stake at infraction epoch {} = {}",
            &validator,
            infraction_epoch,
            u64::from(validator_stake_at_infraction)
        );

        for enqueued_slash in &enqueued_slashes {
            // Add this slash to the list of validator's slashes in storage
            validator_slashes_handle(&validator)
                .push(storage, enqueued_slash.clone())?;

            // Find the total unbonded amount, accounting for slashes, from the
            // slash epoch up to the last epoch first
            let mut total_unbonded = token::Amount::default();
            for epoch in Epoch::iter_bounds_inclusive(
                infraction_epoch.next(),
                current_epoch.prev(),
            ) {
                let unbonds = unbond_records_handle(&validator).at(&epoch);
                for unbond in unbonds.iter(storage)? {
                    let unbond = unbond?;
                    println!(
                        "UnbondRecord: amount = {}, start_epoch {}",
                        &u64::from(unbond.amount),
                        &unbond.start
                    );
                    if unbond.start > infraction_epoch {
                        continue;
                    }
                    let mut prev_slashes = Vec::<Slash>::new();
                    for val_slash in
                        validator_slashes_handle(&validator).iter(storage)?
                    {
                        let val_slash = val_slash?;
                        println!(
                            "Past slash at epoch {} with rate {}",
                            val_slash.epoch, val_slash.rate
                        );
                        if unbond.start <= val_slash.epoch
                            && val_slash.epoch + params.unbonding_len
                                < infraction_epoch
                        // TODO: this `<` should maybe be a `<=`
                        {
                            println!("Collecting this slash");
                            prev_slashes.push(val_slash);
                        }
                    }

                    total_unbonded +=
                        token::Amount::from_change(get_slashed_amount(
                            &params,
                            unbond.amount,
                            &mut prev_slashes,
                        )?);

                    println!(
                        "Total unbonded (epoch {}) w slashing = {}",
                        epoch, total_unbonded
                    );
                }
            }
            println!("\nComputing adjusted amounts now");

            // Compute the adjusted validator deltas and slashed amounts
            let mut last_slash = token::Change::default();
            // TODO: make sure these offsets are consistent with Informal spec!
            for offset in 0..params.pipeline_len {
                println!(
                    "Epoch {}\nLast slash = {}",
                    current_epoch + offset,
                    last_slash
                );
                let unbonds = unbond_records_handle(&validator)
                    .at(&(current_epoch + offset));

                for unbond in unbonds.iter(storage)? {
                    let unbond = unbond?;
                    println!(
                        "UnbondRecord: amount = {}, start_epoch {}",
                        &unbond.amount, &unbond.start
                    );
                    if unbond.start > infraction_epoch {
                        continue;
                    }

                    let mut prev_slashes = Vec::<Slash>::new();
                    for val_slash in
                        validator_slashes_handle(&validator).iter(storage)?
                    {
                        let val_slash = val_slash?;
                        println!(
                            "Past slash at epoch {} with rate {}",
                            val_slash.epoch, val_slash.rate
                        );
                        if unbond.start <= val_slash.epoch
                            && val_slash.epoch + params.unbonding_len
                                < infraction_epoch
                        // TODO: this `<` should maybe be a `<=`
                        {
                            println!("Collecting this slash");
                            prev_slashes.push(val_slash);
                        }
                    }

                    total_unbonded +=
                        token::Amount::from_change(get_slashed_amount(
                            &params,
                            unbond.amount,
                            &mut prev_slashes,
                        )?);
                    println!(
                        "Total unbonded (offset {}) w slashing = {}",
                        offset, total_unbonded
                    );
                }

                let this_slash = decimal_mult_amount(
                    enqueued_slash.rate,
                    validator_stake_at_infraction
                        .checked_sub(total_unbonded)
                        .unwrap_or_default(),
                )
                .change();
                println!("This slash = {}", this_slash);

                let diff_slashed_amount = last_slash - this_slash;
                println!("Diff slashed amount = {}", diff_slashed_amount);

                deltas_for_update
                    .entry(validator.clone())
                    .or_default()
                    .push((offset, diff_slashed_amount));

                // total_slashed -= diff_slashed_amount;
                last_slash = this_slash;
                // total_unbonded = token::Amount::default();
            }
        }
    }
    println!("\nUpdating deltas");
    // Update the deltas in storage
    let mut total_slashed = token::Change::default();
    for (validator, info) in deltas_for_update {
        println!("Val {}", &validator);
        let mut total_slashed_val = token::Change::default();
        for (offset, delta) in info {
            // Check to make sure the delta from slashing does not send the
            // total stake at any point negative
            let validator_stake_at_offset = read_validator_stake(
                storage,
                &params,
                &validator,
                current_epoch + offset,
            )?
            .unwrap_or_default()
            .change();

            let validator_stake_at_infraction = read_validator_stake(
                storage,
                &params,
                &validator,
                infraction_epoch,
            )?
            .unwrap_or_default()
            .change();

            println!(
                "Val stake at offset {} = {}",
                offset, validator_stake_at_offset
            );
            let mut change = if validator_stake_at_offset + delta
                < token::Change::default()
            {
                -validator_stake_at_offset
            } else {
                delta
            };
            if total_slashed_val - change > validator_stake_at_infraction {
                println!(
                    "Catching from slashing more than validator stake at \
                     infraction"
                );
                change = total_slashed_val - validator_stake_at_infraction;
                total_slashed_val -= change;
                debug_assert_eq!(
                    total_slashed_val,
                    validator_stake_at_infraction
                );
                debug_assert!(
                    validator_stake_at_offset + change
                        >= token::Change::default()
                );
            } else {
                total_slashed_val -= change;
            };
            println!("change = {}", change);

            update_validator_deltas(
                storage,
                &params,
                &validator,
                change,
                current_epoch,
                offset,
            )?;
            update_total_deltas(
                storage,
                &params,
                change,
                current_epoch,
                offset,
            )?;
        }
        total_slashed += total_slashed_val
    }
    println!("Total slashed = {}", total_slashed);

    // Transfer all slashed tokens from PoS account to Slash Pool address
    let staking_token = staking_token_address(storage);
    transfer_tokens(
        storage,
        &staking_token,
        token::Amount::from_change(total_slashed),
        &ADDRESS,
        &SLASH_POOL_ADDRESS,
    )?;

    Ok(())
}

/// Unjail a validator that is currently jailed
pub fn unjail_validator<S>(
    storage: &mut S,
    validator: &Address,
    current_epoch: Epoch,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let params = read_pos_params(storage)?;

    // Check that the validator is jailed up to the pipeline epoch
    for epoch in current_epoch.iter_range(params.pipeline_len + 1) {
        let state = validator_state_handle(validator).get(
            storage,
            current_epoch,
            &params,
        )?;
        if let Some(state) = state {
            if state != ValidatorState::Jailed {
                return Err(UnjailValidatorError::NotJailed(
                    validator.clone(),
                    epoch,
                )
                .into());
            }
        } else {
            return Err(
                UnjailValidatorError::NotAValidator(validator.clone()).into()
            );
        }
    }

    // Check that the unjailing tx can be submitted given the current epoch
    // and the most recent infraction epoch
    let last_slash_epoch = read_validator_last_slash_epoch(storage, validator)?
        .unwrap_or_default();
    let eligible_epoch = last_slash_epoch + params.unbonding_len; // TODO: check this is the correct epoch to submit this tx
    if current_epoch < eligible_epoch {
        return Err(UnjailValidatorError::NotEligible(
            validator.clone(),
            eligible_epoch,
            current_epoch,
        )
        .into());
    }
    // TODO: any other checks that are needed? (deltas, etc)?

    // Re-insert the validator into the validator set and update its state
    let pipeline_epoch = current_epoch + params.pipeline_len;
    let stake =
        read_validator_stake(storage, &params, validator, pipeline_epoch)?
            .unwrap_or_default();
    dbg!(&stake);

    insert_validator_into_validator_set(
        storage,
        &params,
        validator,
        stake,
        current_epoch,
        params.pipeline_len,
    )?;
    Ok(())
}

fn is_validator_frozen<S>(
    storage: &mut S,
    validator: &Address,
    current_epoch: Epoch,
    params: &PosParams,
) -> storage_api::Result<bool>
where
    S: StorageRead,
{
    let last_infraction_epoch =
        read_validator_last_slash_epoch(storage, validator)?;
    if let Some(last_epoch) = last_infraction_epoch {
        let is_frozen =
            current_epoch < last_epoch + params.unbonding_len + 1_u64;
        Ok(is_frozen)
    } else {
        Ok(false)
    }
}
