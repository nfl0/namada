//! Token validity predicate queries

router! {TOKEN,
}

#[cfg(any(test, feature = "async-client"))]
pub mod client_only_methods {
    use borsh::BorshDeserialize;

    use super::Token;
    use crate::ledger::queries::{Client, RPC};
    use crate::types::address::Address;
    use crate::types::token;

    impl Token {
        /// Get the balance of the given `token` belonging to the given `owner`.
        pub async fn balance<CLIENT>(
            &self,
            client: &CLIENT,
            token: &Address,
            owner: &Address,
        ) -> Result<token::Amount, <CLIENT as Client>::Error>
        where
            CLIENT: Client + Sync,
        {
            let balance_key = token::balance_key(token, owner);
            let response = RPC
                .shell()
                .storage_value(client, None, None, false, &balance_key)
                .await?;

            let balance = if response.data.is_empty() {
                token::Amount::default()
            } else {
                token::Amount::try_from_slice(&response.data)
                    .unwrap_or_default()
            };
            Ok(balance)
        }
    }
}
