//! Token storage_api functions

use super::{StorageRead, StorageWrite};
use crate::ledger::storage_api;
use crate::types::address::Address;
use crate::types::token;
pub use crate::types::token::{
    balance_key, is_balance_key, is_total_supply_key, total_supply_key, Amount,
    Change,
};

/// Read the balance of a given token and owner.
pub fn read_balance<S>(
    storage: &S,
    token: &Address,
    owner: &Address,
) -> storage_api::Result<token::Amount>
where
    S: StorageRead,
{
    let key = token::balance_key(token, owner);
    let balance = storage.read::<token::Amount>(&key)?.unwrap_or_default();
    Ok(balance)
}

/// Read the total network supply of a given token.
pub fn read_total_supply<S>(
    storage: &S,
    token: &Address,
) -> storage_api::Result<token::Amount>
where
    S: StorageRead,
{
    let key = token::total_supply_key(token);
    let balance = storage.read::<token::Amount>(&key)?.unwrap_or_default();
    Ok(balance)
}

/// Transfer `token` from `src` to `dest`. Returns an `Err` if `src` has
/// insufficient balance or if the transfer the `dest` would overflow (This can
/// only happen if the total supply does't fit in `token::Amount`).
pub fn transfer<S>(
    storage: &mut S,
    token: &Address,
    src: &Address,
    dest: &Address,
    amount: token::Amount,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    if amount.is_zero() {
        return Ok(());
    }
    let src_key = token::balance_key(token, src);
    let src_balance = read_balance(storage, token, src)?;
    match src_balance.checked_sub(amount) {
        Some(new_src_balance) => {
            let dest_key = token::balance_key(token, dest);
            let dest_balance = read_balance(storage, token, dest)?;
            match dest_balance.checked_add(amount) {
                Some(new_dest_balance) => {
                    storage.write(&src_key, new_src_balance)?;
                    storage.write(&dest_key, new_dest_balance)
                }
                None => Err(storage_api::Error::new_const(
                    "The transfer would overflow destination balance",
                )),
            }
        }
        None => {
            Err(storage_api::Error::new_const("Insufficient source balance"))
        }
    }
}

/// Credit tokens to an account, to be used only by protocol. In transactions,
/// this would get rejected by the default `vp_token`.
pub fn credit_tokens<S>(
    storage: &mut S,
    token: &Address,
    dest: &Address,
    amount: token::Amount,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let balance_key = token::balance_key(token, dest);
    let cur_balance = read_balance(storage, token, dest)?;
    let new_balance = cur_balance.checked_add(amount).ok_or_else(|| {
        storage_api::Error::new_const("Token balance overflow")
    })?;

    let total_supply_key = token::total_supply_key(token);
    let cur_supply = storage
        .read::<Amount>(&total_supply_key)?
        .unwrap_or_default();
    let new_supply = cur_supply.checked_add(amount).ok_or_else(|| {
        storage_api::Error::new_const("Token total supply overflow")
    })?;

    storage.write(&balance_key, new_balance)?;
    storage.write(&total_supply_key, new_supply)
}

/// Burn the given amount of tokens from the source address. The total supply is adjusted accordingly. This function is intented to be used only in protocol.
pub fn burn_tokens<S>(
    storage: &mut S,
    token: &Address,
    source: &Address,
    amount: token::Amount,
) -> storage_api::Result<()>
where
    S: StorageRead + StorageWrite,
{
    let balance_key = token::balance_key(token, source);
    let cur_balance = read_balance(storage, token, source)?;
    let new_balance = cur_balance.checked_sub(amount).ok_or_else(|| {
        storage_api::Error::new_const("Token balance underflow")
    })?;

    let total_supply_key = token::total_supply_key(token);
    let cur_supply = storage
        .read::<Amount>(&total_supply_key)?
        .unwrap_or_default();
    let new_supply = cur_supply.checked_sub(amount).ok_or_else(|| {
        storage_api::Error::new_const("Token total supply underflow")
    })?;

    storage.write(&balance_key, new_balance)?;
    storage.write(&total_supply_key, new_supply)
}
