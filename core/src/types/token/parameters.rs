//! Custom parameters for each token type. These are used for
//! determining the shielded pool incentives.

use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use crate::ledger::storage::{self as ledger_storage, types};
use crate::ledger::storage_api::StorageWrite;
use crate::types::address::Address;
use crate::types::storage::{Key, KeySeg};

/// Token parameters for each kind of asset held on chain
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    BorshSerialize,
    BorshDeserialize,
    BorshSchema,
    Deserialize,
    Serialize,
)]
pub struct Parameters {
    /// Maximum reward rate
    max_reward_rate: Decimal,
    /// Shielded Pool nominal derivative gain
    kd_gain_nom: Decimal,
    /// Shielded Pool nominal proportional gain for the given token
    kp_gain_nom: Decimal,
    /// Locked ratio for the given token
    locked_ratio_target_key: Decimal,
}

/// The key for the nominal proportional gain of a shielded pool for a given
/// asset
pub const KP_SP_GAIN_KEY: &str = "proptional_gain";

/// The key for the nominal derivative gain of a shielded pool for a given asset
pub const KD_SP_GAIN_KEY: &str = "derivative_gain";

/// The key for the locked ratio target for a given asset
pub const LOCKED_RATIO_TARGET_KEY: &str = "locked_ratio_target";

/// The key for the max reward rate for a given asset
pub const MAX_REWARD_RATE: &str = "max_reward_rate";

/// Obtain the nominal proportional key for the given token
pub fn kp_sp_gain(token_addr: &Address) -> Key {
    key_of_token(token_addr, KP_SP_GAIN_KEY, "nominal proproitonal gains")
}

/// Obtain the nominal derivative key for the given token
pub fn kd_sp_gain(token_addr: &Address) -> Key {
    key_of_token(token_addr, KD_SP_GAIN_KEY, "nominal proproitonal gains")
}

/// The max reward rate key for the given token
pub fn max_reward_rate(token_addr: &Address) -> Key {
    key_of_token(token_addr, MAX_REWARD_RATE, "max reward rate")
}

/// Obtain the locked target ratio key for the given token
pub fn locked_token_ratio(token_addr: &Address) -> Key {
    key_of_token(
        token_addr,
        LOCKED_RATIO_TARGET_KEY,
        "nominal proproitonal gains",
    )
}

/// Gets the key for the given token address, error with the given
/// message to expect if the key is not in the address
pub fn key_of_token(
    token_addr: &Address,
    specific_key: &str,
    expect_message: &str,
) -> Key {
    Key::from(token_addr.to_db_key())
        .push(&specific_key.to_owned())
        .expect(expect_message)
}

impl Parameters {
    /// Initialize parameters for the token in storage during the genesis block.
    pub fn init_storage<DB, H>(
        &self,
        address: &Address,
        wl_storage: &mut ledger_storage::WlStorage<DB, H>,
    ) where
        DB: ledger_storage::DB + for<'iter> ledger_storage::DBIter<'iter>,
        H: ledger_storage::StorageHasher,
    {
        let Self {
            max_reward_rate: max_rate,
            kd_gain_nom,
            kp_gain_nom,
            locked_ratio_target_key: locked_target,
        } = self;
        wl_storage
            .write(&max_reward_rate(address), max_rate)
            .expect("max reward rate for the given asset must be initialized");
        wl_storage
            .write(&locked_token_ratio(address), locked_target)
            .expect("locked ratio must be initialized");
        wl_storage
            .write(&kp_sp_gain(address), kp_gain_nom)
            .expect("The nominal proportional gain must be initialized");
        wl_storage
            .write(&kd_sp_gain(address), kd_gain_nom)
            .expect("The nominal derivative gain must be initialized");
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            max_reward_rate: dec!(0.1),
            kp_gain_nom: dec!(0.1),
            kd_gain_nom: dec!(0.1),
            locked_ratio_target_key: dec!(0.6667),
        }
    }
}
