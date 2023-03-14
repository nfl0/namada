//! PoS system tests

use std::cmp::min;
use std::ops::Range;

use namada_core::ledger::storage::testing::TestWlStorage;
use namada_core::ledger::storage_api::collections::lazy_map;
use namada_core::ledger::storage_api::token::credit_tokens;
use namada_core::ledger::storage_api::StorageRead;
use namada_core::types::address::testing::{
    address_from_simple_seed, arb_established_address,
};
use namada_core::types::address::{Address, EstablishedAddressGen};
use namada_core::types::key::common::SecretKey;
use namada_core::types::key::testing::{
    arb_common_keypair, common_sk_from_simple_seed,
};
use namada_core::types::storage::Epoch;
use namada_core::types::{address, key, token};
use proptest::prelude::*;
use proptest::test_runner::Config;
use rust_decimal::Decimal;
// Use `RUST_LOG=info` (or another tracing level) and `--nocapture` to see
// `tracing` logs from tests
use test_log::test;

use crate::parameters::testing::arb_pos_params;
use crate::parameters::PosParams;
use crate::types::{
    BondDetails, BondId, BondsAndUnbondsDetails, GenesisValidator, Position,
    ReverseOrdTokenAmount, Slash, SlashType, ValidatorState, WeightedValidator,
};
use crate::{
    become_validator, below_capacity_validator_set_handle, bond_handle,
    bond_tokens, bonds_and_unbonds, consensus_validator_set_handle,
    copy_validator_sets_and_positions, find_validator_by_raw_hash,
    init_genesis, insert_validator_into_validator_set,
    read_below_capacity_validator_set_addresses_with_stake,
    read_consensus_validator_set_addresses_with_stake,
    read_num_consensus_validators, read_total_stake,
    read_validator_delta_value, read_validator_stake, slashes_handle,
    staking_token_address, total_deltas_handle, unbond_handle, unbond_tokens,
    update_validator_deltas, update_validator_set, validator_state_handle,
    withdraw_tokens, write_validator_address_raw_hash,
};

proptest! {
    // Generate arb valid input for `test_init_genesis_aux`
    #![proptest_config(Config {
        cases: 1,
        .. Config::default()
    })]
    #[test]
    fn test_init_genesis(

    pos_params in arb_pos_params(Some(5)),
    start_epoch in (0_u64..1000).prop_map(Epoch),
    genesis_validators in arb_genesis_validators(1..10),

    ) {
        test_init_genesis_aux(pos_params, start_epoch, genesis_validators)
    }
}

proptest! {
    // Generate arb valid input for `test_bonds_aux`
    #![proptest_config(Config {
        cases: 1,
        .. Config::default()
    })]
    #[test]
    fn test_bonds(

    pos_params in arb_pos_params(Some(5)),
    genesis_validators in arb_genesis_validators(1..3),

    ) {
        test_bonds_aux(pos_params, genesis_validators)
    }
}

proptest! {
    // Generate arb valid input for `test_become_validator_aux`
    #![proptest_config(Config {
        cases: 1,
        .. Config::default()
    })]
    #[test]
    fn test_become_validator(

    pos_params in arb_pos_params(Some(5)),
    new_validator in arb_established_address().prop_map(Address::Established),
    new_validator_consensus_key in arb_common_keypair(),
    genesis_validators in arb_genesis_validators(1..3),

    ) {
        test_become_validator_aux(pos_params, new_validator,
            new_validator_consensus_key, genesis_validators)
    }
}

/// Test genesis initialization
fn test_init_genesis_aux(
    params: PosParams,
    start_epoch: Epoch,
    mut validators: Vec<GenesisValidator>,
) {
    println!(
        "Test inputs: {params:?}, {start_epoch}, genesis validators: \
         {validators:#?}"
    );
    let mut s = TestWlStorage::default();
    s.storage.block.epoch = start_epoch;

    init_genesis(&mut s, &params, validators.clone().into_iter(), start_epoch)
        .unwrap();

    let mut bond_details = bonds_and_unbonds(&s, None, None).unwrap();
    assert!(bond_details.iter().all(|(_id, details)| {
        details.unbonds.is_empty() && details.slashes.is_empty()
    }));

    validators.sort_by(|a, b| a.tokens.cmp(&b.tokens));
    for (i, validator) in validators.into_iter().rev().enumerate() {
        println!("Validator {validator:?}");

        let addr = &validator.address;
        let self_bonds = bond_details
            .remove(&BondId {
                source: addr.clone(),
                validator: addr.clone(),
            })
            .unwrap();
        assert_eq!(self_bonds.bonds.len(), 1);
        assert_eq!(
            self_bonds.bonds[0],
            BondDetails {
                start: start_epoch,
                amount: validator.tokens,
                slashed_amount: None,
            }
        );

        let state = validator_state_handle(&validator.address)
            .get(&s, start_epoch, &params)
            .unwrap();
        if (i as u64) < params.max_validator_slots {
            // should be in consensus set
            let handle = consensus_validator_set_handle().at(&start_epoch);
            assert!(handle.at(&validator.tokens).iter(&s).unwrap().any(
                |result| {
                    let (_pos, addr) = result.unwrap();
                    addr == validator.address
                }
            ));
            assert_eq!(state, Some(ValidatorState::Consensus));
        } else {
            // TODO: one more set once we have `below_threshold`

            // should be in below-capacity set
            let handle = below_capacity_validator_set_handle().at(&start_epoch);
            assert!(handle.at(&validator.tokens.into()).iter(&s).unwrap().any(
                |result| {
                    let (_pos, addr) = result.unwrap();
                    addr == validator.address
                }
            ));
            assert_eq!(state, Some(ValidatorState::BelowCapacity));
        }
    }
}

/// Test bonding
/// NOTE: copy validator sets each time we advance the epoch
fn test_bonds_aux(params: PosParams, validators: Vec<GenesisValidator>) {
    // This can be useful for debugging:
    // params.pipeline_len = 2;
    // params.unbonding_len = 4;
    println!("\nTest inputs: {params:?}, genesis validators: {validators:#?}");
    let mut s = TestWlStorage::default();

    // Genesis
    let start_epoch = s.storage.block.epoch;
    let mut current_epoch = s.storage.block.epoch;
    init_genesis(
        &mut s,
        &params,
        validators.clone().into_iter(),
        current_epoch,
    )
    .unwrap();
    s.commit_genesis().unwrap();

    // Advance to epoch 1
    current_epoch = advance_epoch(&mut s, &params);
    let self_bond_epoch = current_epoch;

    let validator = validators.first().unwrap();

    // Read some data before submitting bond
    let pipeline_epoch = current_epoch + params.pipeline_len;
    let pos_balance_pre = s
        .read::<token::Amount>(&token::balance_key(
            &staking_token_address(),
            &super::ADDRESS,
        ))
        .unwrap()
        .unwrap_or_default();
    let total_stake_before =
        read_total_stake(&s, &params, pipeline_epoch).unwrap();

    // Self-bond
    let amount_self_bond = token::Amount::from(100_500_000);
    credit_tokens(
        &mut s,
        &staking_token_address(),
        &validator.address,
        amount_self_bond,
    )
    .unwrap();
    bond_tokens(
        &mut s,
        None,
        &validator.address,
        amount_self_bond,
        current_epoch,
    )
    .unwrap();

    // Check the bond delta
    let self_bond = bond_handle(&validator.address, &validator.address);
    let delta = self_bond
        .get_delta_val(&s, pipeline_epoch, &params)
        .unwrap();
    assert_eq!(delta, Some(amount_self_bond.change()));

    // Check the validator in the validator set
    let set =
        read_consensus_validator_set_addresses_with_stake(&s, pipeline_epoch)
            .unwrap();
    assert!(set.into_iter().any(
        |WeightedValidator {
             bonded_stake,
             address,
         }| {
            address == validator.address
                && bonded_stake == validator.tokens + amount_self_bond
        }
    ));

    let val_deltas = read_validator_delta_value(
        &s,
        &params,
        &validator.address,
        pipeline_epoch,
    )
    .unwrap();
    assert_eq!(val_deltas, Some(amount_self_bond.change()));

    let total_deltas_handle = total_deltas_handle();
    assert_eq!(
        current_epoch,
        total_deltas_handle.get_last_update(&s).unwrap().unwrap()
    );
    let total_stake_after =
        read_total_stake(&s, &params, pipeline_epoch).unwrap();
    assert_eq!(total_stake_before + amount_self_bond, total_stake_after);

    // Check bond details after self-bond
    let self_bond_id = BondId {
        source: validator.address.clone(),
        validator: validator.address.clone(),
    };
    let check_bond_details = |ix, bond_details: BondsAndUnbondsDetails| {
        println!("Check index {ix}");
        assert_eq!(bond_details.len(), 1);
        let details = bond_details.get(&self_bond_id).unwrap();
        assert_eq!(
            details.bonds.len(),
            2,
            "Contains genesis and newly added self-bond"
        );
        dbg!(&details.bonds);
        assert_eq!(
            details.bonds[0],
            BondDetails {
                start: start_epoch,
                amount: validator.tokens,
                slashed_amount: None
            },
        );
        assert_eq!(
            details.bonds[1],
            BondDetails {
                start: pipeline_epoch,
                amount: amount_self_bond,
                slashed_amount: None
            },
        );
    };
    // Try to call it with different combinations of owner/validator args
    check_bond_details(0, bonds_and_unbonds(&s, None, None).unwrap());
    check_bond_details(
        1,
        bonds_and_unbonds(&s, Some(validator.address.clone()), None).unwrap(),
    );
    check_bond_details(
        2,
        bonds_and_unbonds(&s, None, Some(validator.address.clone())).unwrap(),
    );
    check_bond_details(
        3,
        bonds_and_unbonds(
            &s,
            Some(validator.address.clone()),
            Some(validator.address.clone()),
        )
        .unwrap(),
    );

    // Get a non-validating account with tokens
    let delegator = address::testing::gen_implicit_address();
    let amount_del = token::Amount::from(201_000_000);
    credit_tokens(&mut s, &staking_token_address(), &delegator, amount_del)
        .unwrap();
    let balance_key = token::balance_key(&staking_token_address(), &delegator);
    let balance = s
        .read::<token::Amount>(&balance_key)
        .unwrap()
        .unwrap_or_default();
    assert_eq!(balance, amount_del);

    // Advance to epoch 3
    advance_epoch(&mut s, &params);
    current_epoch = advance_epoch(&mut s, &params);
    let pipeline_epoch = current_epoch + params.pipeline_len;

    // Delegation
    let delegation_epoch = current_epoch;
    bond_tokens(
        &mut s,
        Some(&delegator),
        &validator.address,
        amount_del,
        current_epoch,
    )
    .unwrap();
    let val_stake_pre = read_validator_stake(
        &s,
        &params,
        &validator.address,
        pipeline_epoch - 1,
    )
    .unwrap()
    .unwrap_or_default();
    let val_stake_post =
        read_validator_stake(&s, &params, &validator.address, pipeline_epoch)
            .unwrap()
            .unwrap_or_default();
    assert_eq!(validator.tokens + amount_self_bond, val_stake_pre);
    assert_eq!(
        validator.tokens + amount_self_bond + amount_del,
        val_stake_post
    );
    let delegation = bond_handle(&delegator, &validator.address);
    assert_eq!(
        delegation
            .get_sum(&s, pipeline_epoch - 1, &params)
            .unwrap()
            .unwrap_or_default(),
        token::Change::default()
    );
    assert_eq!(
        delegation
            .get_sum(&s, pipeline_epoch, &params)
            .unwrap()
            .unwrap_or_default(),
        amount_del.change()
    );

    // Check delegation bonds details after delegation
    let delegation_bond_id = BondId {
        source: delegator.clone(),
        validator: validator.address.clone(),
    };
    let check_bond_details = |ix, bond_details: BondsAndUnbondsDetails| {
        println!("Check index {ix}");
        assert_eq!(bond_details.len(), 1);
        let details = bond_details.get(&delegation_bond_id).unwrap();
        assert_eq!(details.bonds.len(), 1,);
        dbg!(&details.bonds);
        assert_eq!(
            details.bonds[0],
            BondDetails {
                start: pipeline_epoch,
                amount: amount_del,
                slashed_amount: None
            },
        );
    };
    // Try to call it with different combinations of owner/validator args
    check_bond_details(
        0,
        bonds_and_unbonds(&s, Some(delegator.clone()), None).unwrap(),
    );
    check_bond_details(
        1,
        bonds_and_unbonds(
            &s,
            Some(delegator.clone()),
            Some(validator.address.clone()),
        )
        .unwrap(),
    );

    // Check all bond details (self-bonds and delegation)
    let check_bond_details = |ix, bond_details: BondsAndUnbondsDetails| {
        println!("Check index {ix}");
        assert_eq!(bond_details.len(), 2);
        let self_bond_details = bond_details.get(&self_bond_id).unwrap();
        let delegation_details = bond_details.get(&delegation_bond_id).unwrap();
        assert_eq!(
            self_bond_details.bonds.len(),
            2,
            "Contains genesis and newly added self-bond"
        );
        assert_eq!(
            self_bond_details.bonds[0],
            BondDetails {
                start: start_epoch,
                amount: validator.tokens,
                slashed_amount: None
            },
        );
        assert_eq!(self_bond_details.bonds[1].amount, amount_self_bond);
        assert_eq!(
            delegation_details.bonds[0],
            BondDetails {
                start: pipeline_epoch,
                amount: amount_del,
                slashed_amount: None
            },
        );
    };
    // Try to call it with different combinations of owner/validator args
    check_bond_details(0, bonds_and_unbonds(&s, None, None).unwrap());
    check_bond_details(
        1,
        bonds_and_unbonds(&s, None, Some(validator.address.clone())).unwrap(),
    );

    // Advance to epoch 5
    for _ in 0..2 {
        current_epoch = advance_epoch(&mut s, &params);
    }
    let pipeline_epoch = current_epoch + params.pipeline_len;

    // Unbond the self-bond
    unbond_tokens(&mut s, None, &validator.address, amount_del, current_epoch)
        .unwrap();

    let val_stake_pre = read_validator_stake(
        &s,
        &params,
        &validator.address,
        pipeline_epoch - 1,
    )
    .unwrap();
    let val_stake_post =
        read_validator_stake(&s, &params, &validator.address, pipeline_epoch)
            .unwrap();
    let val_delta = read_validator_delta_value(
        &s,
        &params,
        &validator.address,
        pipeline_epoch,
    )
    .unwrap();
    let unbond = unbond_handle(&validator.address, &validator.address);

    assert_eq!(val_delta, Some(-amount_del.change()));
    assert_eq!(
        unbond
            .at(&(pipeline_epoch + params.unbonding_len))
            .get(&s, &(self_bond_epoch + params.pipeline_len))
            .unwrap(),
        Some(amount_self_bond)
    );
    assert_eq!(
        unbond
            .at(&(pipeline_epoch + params.unbonding_len))
            .get(&s, &Epoch::default())
            .unwrap(),
        Some(amount_del - amount_self_bond)
    );
    assert_eq!(
        val_stake_pre,
        Some(validator.tokens + amount_self_bond + amount_del)
    );
    assert_eq!(val_stake_post, Some(validator.tokens + amount_self_bond));

    // Unbond delegation
    unbond_tokens(
        &mut s,
        Some(&delegator),
        &validator.address,
        amount_self_bond,
        current_epoch,
    )
    .unwrap();

    let val_stake_pre = read_validator_stake(
        &s,
        &params,
        &validator.address,
        pipeline_epoch - 1,
    )
    .unwrap();
    let val_stake_post =
        read_validator_stake(&s, &params, &validator.address, pipeline_epoch)
            .unwrap();
    let val_delta = read_validator_delta_value(
        &s,
        &params,
        &validator.address,
        pipeline_epoch,
    )
    .unwrap();
    let unbond = unbond_handle(&delegator, &validator.address);

    assert_eq!(val_delta, Some(-(amount_self_bond + amount_del).change()));
    assert_eq!(
        unbond
            .at(&(pipeline_epoch + params.unbonding_len))
            .get(&s, &(delegation_epoch + params.pipeline_len))
            .unwrap(),
        Some(amount_self_bond)
    );
    assert_eq!(
        val_stake_pre,
        Some(validator.tokens + amount_self_bond + amount_del)
    );
    assert_eq!(val_stake_post, Some(validator.tokens));

    let withdrawable_offset = params.unbonding_len + params.pipeline_len;

    // Advance to withdrawable epoch
    for _ in 0..withdrawable_offset {
        current_epoch = advance_epoch(&mut s, &params);
    }

    dbg!(current_epoch);

    let pos_balance = s
        .read::<token::Amount>(&token::balance_key(
            &staking_token_address(),
            &super::ADDRESS,
        ))
        .unwrap();

    assert_eq!(
        Some(pos_balance_pre + amount_self_bond + amount_del),
        pos_balance
    );

    // Withdraw the self-unbond
    withdraw_tokens(&mut s, None, &validator.address, current_epoch).unwrap();
    let unbond = unbond_handle(&validator.address, &validator.address);
    let unbond_iter = unbond.iter(&s).unwrap().next();
    assert!(unbond_iter.is_none());

    let pos_balance = s
        .read::<token::Amount>(&token::balance_key(
            &staking_token_address(),
            &super::ADDRESS,
        ))
        .unwrap();
    assert_eq!(Some(pos_balance_pre + amount_self_bond), pos_balance);

    // Withdraw the delegation unbond
    withdraw_tokens(
        &mut s,
        Some(&delegator),
        &validator.address,
        current_epoch,
    )
    .unwrap();
    let unbond = unbond_handle(&delegator, &validator.address);
    let unbond_iter = unbond.iter(&s).unwrap().next();
    assert!(unbond_iter.is_none());

    let pos_balance = s
        .read::<token::Amount>(&token::balance_key(
            &staking_token_address(),
            &super::ADDRESS,
        ))
        .unwrap();
    assert_eq!(Some(pos_balance_pre), pos_balance);
}

/// Test validator initialization.
fn test_become_validator_aux(
    params: PosParams,
    new_validator: Address,
    new_validator_consensus_key: SecretKey,
    validators: Vec<GenesisValidator>,
) {
    println!(
        "Test inputs: {params:?}, new validator: {new_validator}, genesis \
         validators: {validators:#?}"
    );

    let mut s = TestWlStorage::default();

    // Genesis
    let mut current_epoch = dbg!(s.storage.block.epoch);
    init_genesis(
        &mut s,
        &params,
        validators.clone().into_iter(),
        current_epoch,
    )
    .unwrap();
    s.commit_genesis().unwrap();

    // Advance to epoch 1
    current_epoch = advance_epoch(&mut s, &params);

    let num_consensus_before = read_num_consensus_validators(&s).unwrap();
    assert_eq!(
        min(validators.len() as u64, params.max_validator_slots),
        num_consensus_before
    );

    // Initialize the validator account
    let consensus_key = new_validator_consensus_key.to_public();
    become_validator(
        &mut s,
        &params,
        &new_validator,
        &consensus_key,
        current_epoch,
        Decimal::new(5, 2),
        Decimal::new(5, 2),
    )
    .unwrap();

    let num_consensus_after = read_num_consensus_validators(&s).unwrap();
    assert_eq!(
        if validators.len() as u64 >= params.max_validator_slots {
            num_consensus_before
        } else {
            num_consensus_before + 1
        },
        num_consensus_after
    );

    // Advance to epoch 2
    current_epoch = advance_epoch(&mut s, &params);

    // Self-bond to the new validator
    let amount = token::Amount::from(100_500_000);
    credit_tokens(&mut s, &staking_token_address(), &new_validator, amount)
        .unwrap();
    bond_tokens(&mut s, None, &new_validator, amount, current_epoch).unwrap();

    // Check the bond delta
    let bond_handle = bond_handle(&new_validator, &new_validator);
    let pipeline_epoch = current_epoch + params.pipeline_len;
    let delta = bond_handle
        .get_delta_val(&s, pipeline_epoch, &params)
        .unwrap();
    assert_eq!(delta, Some(amount.change()));

    // Check the validator in the validator set -
    // If the consensus validator slots are full and all the genesis validators
    // have stake GTE the new validator's self-bond amount, the validator should
    // be added to the below-capacity set, or the consensus otherwise
    if params.max_validator_slots <= validators.len() as u64
        && validators
            .iter()
            .all(|validator| validator.tokens >= amount)
    {
        let set = read_below_capacity_validator_set_addresses_with_stake(
            &s,
            pipeline_epoch,
        )
        .unwrap();
        assert!(set.into_iter().any(
            |WeightedValidator {
                 bonded_stake,
                 address,
             }| {
                address == new_validator && bonded_stake == amount
            }
        ));
    } else {
        let set = read_consensus_validator_set_addresses_with_stake(
            &s,
            pipeline_epoch,
        )
        .unwrap();
        assert!(set.into_iter().any(
            |WeightedValidator {
                 bonded_stake,
                 address,
             }| {
                address == new_validator && bonded_stake == amount
            }
        ));
    }

    // Advance to epoch 3
    current_epoch = advance_epoch(&mut s, &params);

    // Unbond the self-bond
    unbond_tokens(&mut s, None, &new_validator, amount, current_epoch).unwrap();

    let withdrawable_offset = params.unbonding_len + params.pipeline_len;

    // Advance to withdrawable epoch
    for _ in 0..withdrawable_offset {
        current_epoch = advance_epoch(&mut s, &params);
    }

    // Withdraw the self-bond
    withdraw_tokens(&mut s, None, &new_validator, current_epoch).unwrap();
}

#[test]
fn scrap_slashes() {
    let mut storage = TestWlStorage::default();
    let address = address::testing::established_address_1();
    let slash = Slash {
        epoch: Epoch::default(),
        block_height: 1,
        r#type: SlashType::DuplicateVote,
    };
    let handle = slashes_handle().at(&Epoch::default());
    dbg!(handle.is_empty(&storage).unwrap());

    handle.at(&address).push(&mut storage, slash).unwrap();
    dbg!(handle.is_empty(&storage).unwrap());

    let all_slashes: Vec<_> = slashes_handle()
        .at(&Epoch::default())
        .iter(&storage)
        .unwrap()
        .collect();
    dbg!(&all_slashes); // Showing a bunch of Errs in storage
}

#[test]
fn test_validator_raw_hash() {
    let mut storage = TestWlStorage::default();
    let address = address::testing::established_address_1();
    let consensus_sk = key::testing::keypair_1();
    let consensus_pk = consensus_sk.to_public();
    let expected_raw_hash = key::tm_consensus_key_raw_hash(&consensus_pk);

    assert!(
        find_validator_by_raw_hash(&storage, &expected_raw_hash)
            .unwrap()
            .is_none()
    );
    write_validator_address_raw_hash(&mut storage, &address, &consensus_pk)
        .unwrap();
    let found =
        find_validator_by_raw_hash(&storage, &expected_raw_hash).unwrap();
    assert_eq!(found, Some(address));
}

#[test]
fn test_validator_sets() {
    let mut s = TestWlStorage::default();
    // Only 3 consensus validator slots
    let params = PosParams {
        max_validator_slots: 3,
        ..Default::default()
    };
    let seed = "seed";
    let mut address_gen = EstablishedAddressGen::new(seed);
    let mut gen_validator = || address_gen.generate_address(seed);

    // Start with two genesis validators with 1 NAM stake
    let epoch = Epoch::default();
    let pipeline_epoch = epoch + params.pipeline_len;
    let pk1 = key::testing::keypair_1().to_public();
    let pk2 = key::testing::keypair_2().to_public();
    let (val1, stake1) = (gen_validator(), token::Amount::whole(1));
    let (val2, stake2) = (gen_validator(), token::Amount::whole(1));
    let (val3, stake3) = (gen_validator(), token::Amount::whole(10));
    let (val4, stake4) = (gen_validator(), token::Amount::whole(100));
    let (val5, stake5) = (gen_validator(), token::Amount::whole(1));
    let (val6, stake6) = (gen_validator(), token::Amount::whole(1));
    println!("val1: {val1}, {stake1}");
    println!("val2: {val2}, {stake2}");
    println!("val3: {val3}, {stake3}");
    println!("val4: {val4}, {stake4}");
    println!("val5: {val5}, {stake5}");
    println!("val6: {val6}, {stake6}");

    init_genesis(
        &mut s,
        &params,
        [
            GenesisValidator {
                address: val1.clone(),
                tokens: stake1,
                consensus_key: pk1,
                commission_rate: Decimal::new(1, 1),
                max_commission_rate_change: Decimal::new(1, 1),
            },
            GenesisValidator {
                address: val2.clone(),
                tokens: stake2,
                consensus_key: pk2,
                commission_rate: Decimal::new(1, 1),
                max_commission_rate_change: Decimal::new(1, 1),
            },
        ]
        .into_iter(),
        epoch,
    )
    .unwrap();

    // Insert another validator with the greater stake 10 NAM
    insert_validator_into_validator_set(
        &mut s,
        &params,
        &val3,
        stake3,
        epoch,
        params.pipeline_len,
    )
    .unwrap();
    // Update deltas as they are needed for validator set updates
    update_validator_deltas(&mut s, &params, &val3, stake3.change(), epoch)
        .unwrap();

    let consensus_vals: Vec<_> = consensus_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(consensus_vals.len(), 3);
    assert!(matches!(
        &consensus_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val1 && stake == &stake1 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val2 && stake == &stake2 && *position == Position(1)
    ));
    assert!(matches!(
        &consensus_vals[2],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val3 && stake == &stake3 && *position == Position(0)
    ));

    // Insert another validator with a greater stake still 1000 NAM. It should
    // replace 2nd consensus validator with stake 1, which should become
    // below-capacity
    insert_validator_into_validator_set(
        &mut s,
        &params,
        &val4,
        stake4,
        epoch,
        params.pipeline_len,
    )
    .unwrap();
    update_validator_deltas(&mut s, &params, &val4, stake4.change(), epoch)
        .unwrap();

    let consensus_vals: Vec<_> = consensus_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(consensus_vals.len(), 3);
    assert!(matches!(
        &consensus_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val1 && stake == &stake1 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val3 && stake == &stake3 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[2],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val4 && stake == &stake4 && *position == Position(0)
    ));

    let below_capacity_vals: Vec<_> = below_capacity_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(below_capacity_vals.len(), 1);
    assert!(matches!(
        &below_capacity_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val2 && stake == &stake2 && *position == Position(0)
    ));

    // Insert another validator with a stake 1 NAM. It should be added to the
    // below-capacity set
    insert_validator_into_validator_set(
        &mut s,
        &params,
        &val5,
        stake5,
        epoch,
        params.pipeline_len,
    )
    .unwrap();
    update_validator_deltas(&mut s, &params, &val5, stake5.change(), epoch)
        .unwrap();

    let below_capacity_vals: Vec<_> = below_capacity_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(below_capacity_vals.len(), 2);
    assert!(matches!(
        &below_capacity_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val2 && stake == &stake2 && *position == Position(0)
    ));
    assert!(matches!(
        &below_capacity_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val5 && stake == &stake5 && *position == Position(1)
    ));

    // Unbond some stake from val1, it should be be swapped with the greatest
    // below-capacity validator val2 into the below-capacity set
    let unbond = token::Amount::from(500_000);
    let stake1 = stake1 - unbond;
    println!("val1 {val1} new stake {stake1}");
    // Because `update_validator_set` and `update_validator_deltas` are
    // effective from pipeline offset, we use pipeline epoch for the rest of the
    // checks
    update_validator_set(&mut s, &params, &val1, -unbond.change(), epoch)
        .unwrap();
    update_validator_deltas(&mut s, &params, &val1, -unbond.change(), epoch)
        .unwrap();

    let consensus_vals: Vec<_> = consensus_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(consensus_vals.len(), 3);
    assert!(matches!(
        &consensus_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val2 && stake == &stake2 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val3 && stake == &stake3 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[2],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val4 && stake == &stake4 && *position == Position(0)
    ));

    let below_capacity_vals: Vec<_> = below_capacity_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(below_capacity_vals.len(), 2);
    assert!(matches!(
        &below_capacity_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val5 && stake == &stake5 && *position == Position(1)
    ));
    assert!(matches!(
        &below_capacity_vals[1],
        (
            lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
            },
            address
        )
        if address == &val1 && stake == &stake1 && *position == Position(0)
    ));

    // Insert another validator with stake 1 - it should be added after val1
    insert_validator_into_validator_set(
        &mut s,
        &params,
        &val6,
        stake6,
        epoch,
        params.pipeline_len,
    )
    .unwrap();
    update_validator_deltas(&mut s, &params, &val6, stake6.change(), epoch)
        .unwrap();

    let consensus_vals: Vec<_> = consensus_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(consensus_vals.len(), 3);
    assert!(matches!(
        &consensus_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val2 && stake == &stake2 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val3 && stake == &stake3 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[2],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val4 && stake == &stake4 && *position == Position(0)
    ));

    let below_capacity_vals: Vec<_> = below_capacity_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(below_capacity_vals.len(), 3);
    assert!(matches!(
        &below_capacity_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val5 && stake == &stake5 && *position == Position(1)
    ));
    assert!(matches!(
        &below_capacity_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val6 && stake == &stake6 && *position == Position(2)
    ));
    assert!(matches!(
        &below_capacity_vals[2],
        (
            lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
            },
            address
        )
        if address == &val1 && stake == &stake1 && *position == Position(0)
    ));

    // Bond some stake to val5, it should be be swapped with the lowest
    // consensus validator val2 into the consensus set
    let bond = token::Amount::from(500_000);
    let stake5 = stake5 + bond;
    println!("val5 {val5} new stake {stake5}");
    update_validator_set(&mut s, &params, &val5, bond.change(), epoch).unwrap();
    update_validator_deltas(&mut s, &params, &val5, bond.change(), epoch)
        .unwrap();

    let consensus_vals: Vec<_> = consensus_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(consensus_vals.len(), 3);
    assert!(matches!(
        &consensus_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val5 && stake == &stake5 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val3 && stake == &stake3 && *position == Position(0)
    ));
    assert!(matches!(
        &consensus_vals[2],
        (lazy_map::NestedSubKey::Data {
                key: stake,
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val4 && stake == &stake4 && *position == Position(0)
    ));

    let below_capacity_vals: Vec<_> = below_capacity_validator_set_handle()
        .at(&pipeline_epoch)
        .iter(&s)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(below_capacity_vals.len(), 3);
    assert!(matches!(
        &below_capacity_vals[0],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val6 && stake == &stake6 && *position == Position(2)
    ));
    assert!(matches!(
        &below_capacity_vals[1],
        (lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
        }, address)
        if address == &val2 && stake == &stake2 && *position == Position(3)
    ));
    assert!(matches!(
        &below_capacity_vals[2],
        (
            lazy_map::NestedSubKey::Data {
                key: ReverseOrdTokenAmount(stake),
                nested_sub_key: lazy_map::SubKey::Data(position),
            },
            address
        )
        if address == &val1 && stake == &stake1 && *position == Position(0)
    ));
}

/// Advance to the next epoch. Returns the new epoch.
fn advance_epoch(s: &mut TestWlStorage, params: &PosParams) -> Epoch {
    s.storage.block.epoch = s.storage.block.epoch.next();
    let current_epoch = s.storage.block.epoch;
    copy_validator_sets_and_positions(
        s,
        current_epoch,
        current_epoch + params.pipeline_len,
        &consensus_validator_set_handle(),
        &below_capacity_validator_set_handle(),
    )
    .unwrap();
    current_epoch
}

fn arb_genesis_validators(
    size: Range<usize>,
) -> impl Strategy<Value = Vec<GenesisValidator>> {
    let tokens: Vec<_> = (0..size.end)
        .map(|_| (1..=1_000_000_000_000_u64).prop_map(token::Amount::from))
        .collect();
    (size, tokens).prop_map(|(size, token_amounts)| {
        // use unique seeds to generate validators' address and consensus key
        let seeds = (0_u64..).take(size);
        seeds
            .zip(token_amounts)
            .map(|(seed, tokens)| {
                let address = address_from_simple_seed(seed);
                let consensus_sk = common_sk_from_simple_seed(seed);
                let consensus_key = consensus_sk.to_public();

                let commission_rate = Decimal::new(5, 2);
                let max_commission_rate_change = Decimal::new(1, 3);
                GenesisValidator {
                    address,
                    tokens,
                    consensus_key,
                    commission_rate,
                    max_commission_rate_change,
                }
            })
            .collect()
    })
}
