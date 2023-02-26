//! A tx for a PoS bond that stakes tokens via a self-bond or delegation.

use namada_tx_prelude::*;

#[transaction]
fn apply_tx(ctx: &mut Ctx, tx_data: Vec<u8>) -> TxResult {
    let signed = SignedTxData::try_from_slice(&tx_data[..])
        .wrap_err("failed to decode SignedTxData")
        .unwrap();
    let data = signed.data.ok_or_err_msg("Missing data").unwrap();
    let bond = transaction::pos::Bond::try_from_slice(&data[..])
        .wrap_err("failed to decode Bond")
        .unwrap();

    ctx.bond_tokens(bond.source.as_ref(), &bond.validator, bond.amount)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use namada::ledger::pos::{GenesisValidator, PosParams, PosVP};
    use namada::proof_of_stake::{
        bond_handle, read_consensus_validator_set_addresses_with_stake,
        read_total_stake, read_validator_stake,
    };
    use namada::proto::Tx;
    use namada::types::storage::Epoch;
    use namada_tests::log::test;
    use namada_tests::native_vp::pos::init_pos;
    use namada_tests::native_vp::TestNativeVpEnv;
    use namada_tests::tx::*;
    use namada_tx_prelude::address::testing::{
        arb_established_address, arb_non_internal_address,
    };
    use namada_tx_prelude::address::InternalAddress;
    use namada_tx_prelude::key::testing::arb_common_keypair;
    use namada_tx_prelude::key::RefTo;
    use namada_tx_prelude::proof_of_stake::parameters::testing::arb_pos_params;
    use namada_tx_prelude::token;
    use namada_vp_prelude::key::{common, ed25519, testing, SecretKey};
    use namada_vp_prelude::proof_of_stake::WeightedValidator;
    use proptest::prelude::*;
    use rust_decimal;

    use super::*;

    proptest! {
        /// In this test, we setup the ledger and PoS system with an arbitrary
        /// initial stake with 1 genesis validator and arbitrary PoS parameters. We then
        /// generate an arbitrary bond that we'd like to apply.
        ///
        /// After we apply the bond, we check that all the storage values
        /// in the PoS system have been updated as expected, and then we check
        /// that this transaction is accepted by the PoS validity predicate.
        #[test]
        fn test_tx_bond(
            // (initial_stake, bond) in arb_initial_stake_and_bond(),
            // // A key to sign the transaction
            // key in arb_common_keypair(),
            pos_params in arb_pos_params(None)) {
            // test_tx_bond_aux(initial_stake, bond, key, pos_params).unwrap()
            test_tx_bond_DEBUG(pos_params).unwrap()
        }
    }

    fn test_tx_bond_aux(
        initial_stake: token::Amount,
        bond: transaction::pos::Bond,
        key: key::common::SecretKey,
        pos_params: PosParams,
    ) -> TxResult {
        dbg!(&initial_stake, &bond);
        let is_delegation =
            matches!(&bond.source, Some(source) if *source != bond.validator);
        let consensus_key = key::testing::keypair_1().ref_to();
        let commission_rate = rust_decimal::Decimal::new(5, 2);
        let max_commission_rate_change = rust_decimal::Decimal::new(1, 2);

        let genesis_validators = [GenesisValidator {
            address: bond.validator.clone(),
            tokens: initial_stake,
            consensus_key,
            commission_rate,
            max_commission_rate_change,
        }];

        init_pos(&genesis_validators[..], &pos_params, Epoch(0));

        let native_token = tx_host_env::with(|tx_env| {
            if let Some(source) = &bond.source {
                tx_env.spawn_accounts([source]);
            }

            // Ensure that the bond's source has enough tokens for the bond
            let target = bond.source.as_ref().unwrap_or(&bond.validator);
            let native_token = tx_env.wl_storage.storage.native_token.clone();
            tx_env.credit_tokens(target, &native_token, None, bond.amount);
            native_token
        });

        let tx_code = vec![];
        let tx_data = bond.try_to_vec().unwrap();
        let tx = Tx::new(tx_code, Some(tx_data));
        let signed_tx = tx.sign(&key);
        let tx_data = signed_tx.data.unwrap();

        // Ensure that the initial stake of the sole validator is equal to the
        // PoS account balance
        let pos_balance_key = token::balance_key(
            &native_token,
            &Address::Internal(InternalAddress::PoS),
        );
        let pos_balance_pre: token::Amount = ctx()
            .read(&pos_balance_key)
            .unwrap()
            .expect("PoS must have balance");
        assert_eq!(pos_balance_pre, initial_stake);

        // Read some data before the tx is executed
        let mut epoched_total_stake_pre: Vec<token::Amount> = Vec::new();
        let mut epoched_validator_stake_pre: Vec<token::Amount> = Vec::new();
        let mut epoched_validator_set_pre: Vec<HashSet<WeightedValidator>> =
            Vec::new();

        for epoch in 0..=pos_params.unbonding_len {
            epoched_total_stake_pre.push(read_total_stake(
                ctx(),
                &pos_params,
                Epoch(epoch),
            )?);
            epoched_validator_stake_pre.push(
                read_validator_stake(
                    ctx(),
                    &pos_params,
                    &bond.validator,
                    Epoch(epoch),
                )?
                .unwrap(),
            );
            epoched_validator_set_pre.push(
                read_consensus_validator_set_addresses_with_stake(
                    ctx(),
                    Epoch(epoch),
                )?,
            );
        }

        apply_tx(ctx(), tx_data)?;

        // Read the data after the tx is executed.
        let mut epoched_total_stake_post: Vec<token::Amount> = Vec::new();
        let mut epoched_validator_stake_post: Vec<token::Amount> = Vec::new();
        let mut epoched_validator_set_post: Vec<HashSet<WeightedValidator>> =
            Vec::new();

        println!("\nFILLING POST STATE\n");

        for epoch in 0..=pos_params.unbonding_len {
            epoched_total_stake_post.push(read_total_stake(
                ctx(),
                &pos_params,
                Epoch(epoch),
            )?);
            epoched_validator_stake_post.push(
                read_validator_stake(
                    ctx(),
                    &pos_params,
                    &bond.validator,
                    Epoch(epoch),
                )?
                .unwrap(),
            );
            epoched_validator_set_post.push(
                read_consensus_validator_set_addresses_with_stake(
                    ctx(),
                    Epoch(epoch),
                )?,
            );
        }

        // The following storage keys should be updated:

        //     - `#{PoS}/validator/#{validator}/deltas`
        //     - `#{PoS}/total_deltas`
        //     - `#{PoS}/validator_set`

        // Check that the validator set and deltas are unchanged before pipeline
        // length and that they are updated between the pipeline and
        // unbonding lengths
        if bond.amount == token::Amount::from(0) {
            // None of the optional storage fields should have been updated
            assert_eq!(epoched_validator_set_pre, epoched_validator_set_post);
            assert_eq!(
                epoched_validator_stake_pre,
                epoched_validator_stake_post
            );
            assert_eq!(epoched_total_stake_pre, epoched_total_stake_post);
        } else {
            for epoch in 0..pos_params.pipeline_len as usize {
                assert_eq!(
                    epoched_validator_stake_post[epoch], initial_stake,
                    "The validator deltas before the pipeline offset must not \
                     change - checking in epoch: {epoch}"
                );
                assert_eq!(
                    epoched_total_stake_post[epoch], initial_stake,
                    "The total deltas before the pipeline offset must not \
                     change - checking in epoch: {epoch}"
                );
                assert_eq!(
                    epoched_validator_set_pre[epoch],
                    epoched_validator_set_post[epoch],
                    "Validator set before pipeline offset must not change - \
                     checking epoch {epoch}"
                );
            }
            for epoch in (pos_params.pipeline_len as usize)
                ..=pos_params.unbonding_len as usize
            {
                let expected_stake =
                    i128::from(initial_stake) + i128::from(bond.amount);
                assert_eq!(
                    epoched_validator_stake_post[epoch],
                    token::Amount::from_change(expected_stake),
                    "The total deltas at and after the pipeline offset epoch \
                     must be incremented by the bonded amount - checking in \
                     epoch: {epoch}"
                );
                assert_eq!(
                    epoched_total_stake_post[epoch],
                    token::Amount::from_change(expected_stake),
                    "The total deltas at and after the pipeline offset epoch \
                     must be incremented by the bonded amount - checking in \
                     epoch: {epoch}"
                );
                if epoch == pos_params.pipeline_len as usize {
                    assert_ne!(
                        epoched_validator_set_pre[epoch],
                        epoched_validator_set_post[epoch],
                        "Validator set at and after pipeline offset must have \
                         changed - checking epoch {epoch}"
                    );
                }
            }
        }

        //     - `#{staking_token}/balance/#{PoS}`
        // Check that PoS balance is updated
        let pos_balance_post: token::Amount =
            ctx().read(&pos_balance_key)?.unwrap();
        assert_eq!(pos_balance_pre + bond.amount, pos_balance_post);

        //     - `#{PoS}/bond/#{owner}/#{validator}`
        let bond_src = bond
            .source
            .clone()
            .unwrap_or_else(|| bond.validator.clone());

        let bonds_post = bond_handle(&bond_src, &bond.validator);
        // let bonds_post = ctx().read_bond(&bond_id)?.unwrap();

        for epoch in 0..pos_params.unbonding_len {
            dbg!(
                epoch,
                bonds_post.get_delta_val(ctx(), Epoch(epoch), &pos_params)?
            );
        }

        if is_delegation {
            // A delegation is applied at pipeline offset
            // Check that bond is empty before pipeline offset
            for epoch in 0..pos_params.pipeline_len {
                let bond =
                    bonds_post.get_sum(ctx(), Epoch(epoch), &pos_params)?;
                assert!(
                    bond.is_none(),
                    "Delegation before pipeline offset should be empty - \
                     checking epoch {epoch}, got {bond:#?}"
                );
            }
            // Check that bond is updated after the pipeline length
            for epoch in pos_params.pipeline_len..=pos_params.unbonding_len {
                let expected_bond_amount = bond.amount.change();
                let bond =
                    bonds_post.get_sum(ctx(), Epoch(epoch), &pos_params)?;
                assert_eq!(
                    bond,
                    Some(expected_bond_amount),
                    "Delegation at and after pipeline offset should be equal \
                     to the bonded amount - checking epoch {epoch}"
                );
            }
        } else {
            // This is a self-bond
            // Check that a bond already exists from genesis with initial stake
            // for the validator
            for epoch in 0..pos_params.pipeline_len {
                let expected_bond_amount = initial_stake.change();
                let bond = bonds_post
                    .get_sum(ctx(), Epoch(epoch), &pos_params)
                    .expect("Genesis validator should already have self-bond");
                assert_eq!(
                    bond,
                    Some(expected_bond_amount),
                    "Self-bond before pipeline offset should be equal to the \
                     genesis initial stake - checking epoch {epoch}"
                );
            }
            // Check that the bond is updated after the pipeline length
            for epoch in pos_params.pipeline_len..=pos_params.unbonding_len {
                let expected_bond_amount = initial_stake + bond.amount;
                let bond =
                    bonds_post.get_sum(ctx(), Epoch(epoch), &pos_params)?;
                assert_eq!(
                    bond,
                    Some(expected_bond_amount.change()),
                    "Self-bond at and after pipeline offset should contain \
                     genesis stake and the bonded amount - checking epoch \
                     {epoch}"
                );
            }
        }

        // Use the tx_env to run PoS VP
        let tx_env = tx_host_env::take();
        let vp_env = TestNativeVpEnv::from_tx_env(tx_env, address::POS);
        let result = vp_env.validate_tx(PosVP::new);
        let result =
            result.expect("Validation of valid changes must not fail!");
        assert!(
            result,
            "PoS Validity predicate must accept this transaction"
        );
        Ok(())
    }

    fn test_tx_bond_DEBUG(pos_params: PosParams) -> TxResult {
        println!("DEBUGGING W SOME CUSTOM SHIT");
        let validator = address::testing::established_address_1();
        let consensus_key = key::testing::keypair_1().ref_to();
        let commission_rate = rust_decimal::Decimal::new(5, 2);
        let max_commission_rate_change = rust_decimal::Decimal::new(1, 2);
        let initial_stake = token::Amount::from(10000000000_u64);

        let delegator1 = address::testing::established_address_2();
        let delegator2 = address::testing::established_address_3();

        let genesis_validators = [GenesisValidator {
            address: validator.clone(),
            tokens: initial_stake,
            consensus_key,
            commission_rate,
            max_commission_rate_change,
        }];

        init_pos(&genesis_validators[..], &pos_params, Epoch(0));

        let self_bond = transaction::pos::Bond {
            validator: validator.clone(),
            amount: token::Amount::from(200000000_u64),
            source: None,
        };
        let key_self = testing::gen_keypair::<ed25519::SigScheme>()
            .try_to_sk::<common::SecretKey>()
            .unwrap();
        let delegation1 = transaction::pos::Bond {
            validator: validator.clone(),
            amount: token::Amount::from(300000000_u64),
            source: Some(delegator1.clone()),
        };
        let key_del1 = testing::keypair_1();
        let delegation2 = transaction::pos::Bond {
            validator: validator.clone(),
            amount: token::Amount::from(250000000_u64),
            source: Some(delegator2.clone()),
        };
        let key_del2 = testing::keypair_2();

        let native_token = tx_host_env::with(|tx_env| {
            tx_env.spawn_accounts([&delegator1, &delegator2]);
            let native_token = tx_env.wl_storage.storage.native_token.clone();

            tx_env.credit_tokens(
                &validator,
                &native_token,
                None,
                token::Amount::from(500000000_u64),
            );
            tx_env.credit_tokens(
                &delegator1,
                &native_token,
                None,
                token::Amount::from(500000000_u64),
            );
            tx_env.credit_tokens(
                &delegator2,
                &native_token,
                None,
                token::Amount::from(500000000_u64),
            );
            native_token
        });

        let tx_code = vec![];

        let tx_data_self = self_bond.try_to_vec().unwrap();
        let tx_data_del1 = delegation1.try_to_vec().unwrap();
        let tx_data_del2 = delegation2.try_to_vec().unwrap();

        let tx_self = Tx::new(tx_code.clone(), Some(tx_data_self));
        let tx_del1 = Tx::new(tx_code.clone(), Some(tx_data_del1));
        let tx_del2 = Tx::new(tx_code.clone(), Some(tx_data_del2));

        let signed_tx_self = tx_self.clone().sign(&key_self);
        let signed_tx_del1 = tx_del1.clone().sign(&key_del1);
        let signed_tx_del2 = tx_del2.clone().sign(&key_del2);

        let tx_data_self = signed_tx_self.data.unwrap();
        let tx_data_del1 = signed_tx_del1.data.unwrap();
        let tx_data_del2 = signed_tx_del2.data.unwrap();

        println!("CONSENSUS VALIDATOR SET INITIAL");
        for epoch in 0..=pos_params.unbonding_len {
            dbg!(read_consensus_validator_set_addresses_with_stake(
                ctx(),
                Epoch(epoch),
            )?,);
        }

        // Ensure that the initial stake of the sole validator is equal to the
        // PoS account balance
        let pos_balance_key = token::balance_key(
            &native_token,
            &Address::Internal(InternalAddress::PoS),
        );
        let pos_balance_pre: token::Amount = ctx()
            .read(&pos_balance_key)
            .unwrap()
            .expect("PoS must have balance");
        assert_eq!(pos_balance_pre, initial_stake);

        apply_tx(ctx(), tx_data_self)?;

        println!("\nCONSENSUS VALIDATOR SET AFTER SELF BOND\n");
        for epoch in 0..=pos_params.unbonding_len {
            dbg!(read_consensus_validator_set_addresses_with_stake(
                ctx(),
                Epoch(epoch),
            )?,);
        }

        apply_tx(ctx(), tx_data_del1)?;

        println!("\nCONSENSUS VALIDATOR SET AFTER DELEGATION 1\n");
        for epoch in 0..=pos_params.unbonding_len {
            dbg!(read_consensus_validator_set_addresses_with_stake(
                ctx(),
                Epoch(epoch),
            )?,);
        }

        apply_tx(ctx(), tx_data_del2)?;

        println!("\nCONSENSUS VALIDATOR SET AFTER DELEGATION 2\n");
        for epoch in 0..=pos_params.unbonding_len {
            dbg!(read_consensus_validator_set_addresses_with_stake(
                ctx(),
                Epoch(epoch),
            )?,);
        }

        // Use the tx_env to run PoS VP
        let tx_env = tx_host_env::take();
        let vp_env = TestNativeVpEnv::from_tx_env(tx_env, address::POS);
        let result = vp_env.validate_tx(PosVP::new);
        let result =
            result.expect("Validation of valid changes must not fail!");
        assert!(
            result,
            "PoS Validity predicate must accept this transaction"
        );

        assert!(false);

        Ok(())
    }

    prop_compose! {
        /// Generates an initial validator stake and a bond, while making sure
        /// that the `initial_stake + bond.amount <= u64::MAX` to avoid
        /// overflow.
        fn arb_initial_stake_and_bond()
            // Generate initial stake
            (initial_stake in token::testing::arb_amount_ceiled((i64::MAX/8) as u64))
            // Use the initial stake to limit the bond amount
            (bond in arb_bond(((i64::MAX/8) as u64) - u64::from(initial_stake)),
            // Use the generated initial stake too
            initial_stake in Just(initial_stake),
        ) -> (token::Amount, transaction::pos::Bond) {
            (initial_stake, bond)
        }
    }

    fn arb_bond(
        max_amount: u64,
    ) -> impl Strategy<Value = transaction::pos::Bond> {
        (
            arb_established_address(),
            prop::option::of(arb_non_internal_address()),
            token::testing::arb_amount_non_zero_ceiled(max_amount),
        )
            .prop_map(|(validator, source, amount)| {
                transaction::pos::Bond {
                    validator: Address::Established(validator),
                    amount,
                    source,
                }
            })
    }
}
