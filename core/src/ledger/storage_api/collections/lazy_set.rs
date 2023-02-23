//! Lazy dynamically-sized hash set.

use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;

use super::super::Result;
use super::LazyCollection;
use crate::ledger::storage_api::validation::Data;
use crate::ledger::storage_api::{self, StorageRead, StorageWrite};
use crate::ledger::vp_env::VpEnv;
use crate::types::storage::{self};

/// Subkey corresponding to the data elements of the LazySet
pub const DATA_SUBKEY: &str = "data";

/// Using `u64` with Hasher::finish()
pub type ValueHash = u64;

/// Lazy dynamically-sized hash set.
///
/// This can be used as an alternative to `std::collections::HashSet`. In the
/// lazy set, the elements do not reside in memory but are instead read and
/// written to storage sub-keys of the storage `key` used to construct the
/// vector.
#[derive(Clone, Debug)]
pub struct LazySet<T: Hash> {
    key: storage::Key,
    phantom: PhantomData<T>,
}

/// Possible sub-keys of a [`LazySet`]
#[derive(Debug)]
pub enum SubKey {
    /// Data sub-key, further sub-keyed by the hash of the underlying value
    Data(ValueHash),
}

/// Possible sub-keys of a [`LazySet`], together with their [`validation::Data`]
/// that contains prior and posterior state.
#[derive(Debug)]
pub enum SubKeyWithData<T> {
    /// Data sub-key, further sub-keyed by the hash of the underlying value
    Data(ValueHash, Data<T>),
}

/// Possible actions that can modify a [`LazySet`]. This roughly corresponds to
/// the methods that have `StorageWrite` access.
#[derive(Clone, Debug)]
pub enum Action<T> {
    /// Insert a value `T` into a [`LazySet<T>`]
    Insert(T),
    /// Remove a value `T` from a [`LazySet<T>`]
    Remove(T),
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid storage key {0}")]
    InvalidSubKey(storage::Key),
}

// #[allow(missing_docs)]
// #[derive(Error, Debug)]
// pub enum UpdateError {
//     #[error(
//         "Invalid index into a LazyVec. Got {index}, but the length is {len}"
//     )]
//     InvalidIndex { index: Index, len: u64 },
// }

/// [`LazyVec`] validation result
pub type ValidationResult<T> = std::result::Result<T, ValidationError>;

impl<T> LazyCollection for LazySet<T>
where
    T: BorshSerialize + BorshDeserialize + 'static + Debug + Hash,
{
    type Action = Action<T>;
    type SubKey = SubKey;
    type SubKeyWithData = SubKeyWithData<T>;
    type Value = T;

    /// Create or use an existing set with the given storage `key`.
    fn open(key: storage::Key) -> Self {
        Self {
            key,
            phantom: PhantomData,
        }
    }

    /// Check if the given storage key is a valid LazyVec sub-key and if so
    /// return which one
    fn is_valid_sub_key(
        &self,
        _key: &storage::Key,
    ) -> storage_api::Result<Option<SubKey>> {
        todo!()
        // let suffix = match key.split_prefix(&self.key) {
        //     None => {
        //         // not matching prefix, irrelevant
        //         return Ok(None);
        //     }
        //     Some(None) => {
        //         // no suffix, invalid
        //         return Err(ValidationError::InvalidSubKey(key.clone()))
        //             .into_storage_result();
        //     }
        //     Some(Some(suffix)) => suffix,
        // };

        // // Match the suffix against expected sub-keys
        // match &suffix.segments[..] {
        //     [DbKeySeg::StringSeg(sub)] if sub == LEN_SUBKEY => {
        //         Ok(Some(SubKey::Len))
        //     }
        //     [DbKeySeg::StringSeg(sub_a), DbKeySeg::StringSeg(sub_b)]
        //         if sub_a == DATA_SUBKEY =>
        //     {
        //         if let Ok(index) = storage::KeySeg::parse(sub_b.clone()) {
        //             Ok(Some(SubKey::Data(index)))
        //         } else {
        //             Err(ValidationError::InvalidSubKey(key.clone()))
        //                 .into_storage_result()
        //         }
        //     }
        //     _ => Err(ValidationError::InvalidSubKey(key.clone()))
        //         .into_storage_result(),
        // }
    }

    fn read_sub_key_data<ENV>(
        _env: &ENV,
        _storage_key: &storage::Key,
        _sub_key: Self::SubKey,
    ) -> storage_api::Result<Option<Self::SubKeyWithData>>
    where
        ENV: for<'a> VpEnv<'a>,
    {
        todo!()
        // let change = match sub_key {
        //     SubKey::Len => {
        //         let data = validation::read_data(env, storage_key)?;
        //         data.map(SubKeyWithData::Len)
        //     }
        //     SubKey::Data(index) => {
        //         let data = validation::read_data(env, storage_key)?;
        //         data.map(|data| SubKeyWithData::Data(index, data))
        //     }
        // };
        // Ok(change)
    }

    /// The validation rules for a [`LazyVec`] are:
    ///   - A difference in the vector's length must correspond to the
    ///     difference in how many elements were pushed versus how many elements
    ///     were popped.
    ///   - An empty vector must be deleted from storage
    ///   - In addition, we check that indices of any changes are within an
    ///     expected range (i.e. the vectors indices should always be
    ///     monotonically increasing from zero)
    fn validate_changed_sub_keys(
        _keys: Vec<Self::SubKeyWithData>,
    ) -> storage_api::Result<Vec<Self::Action>> {
        todo!()
        // let mut actions = vec![];

        // // We need to accumulate some values for what's changed
        // let mut post_gt_pre = false;
        // let mut len_diff: u64 = 0;
        // let mut len_pre: u64 = 0;
        // let mut added = BTreeSet::<Index>::default();
        // let mut updated = BTreeSet::<Index>::default();
        // let mut deleted = BTreeSet::<Index>::default();

        // for key in keys {
        //     match key {
        //         SubKeyWithData::Len(data) => match data {
        //             Data::Add { post } => {
        //                 if post == 0 {
        //                     return Err(
        //                         ValidationError::EmptyVecShouldBeDeleted,
        //                     )
        //                     .into_storage_result();
        //                 }
        //                 post_gt_pre = true;
        //                 len_diff = post;
        //             }
        //             Data::Update { pre, post } => {
        //                 if post == 0 {
        //                     return Err(
        //                         ValidationError::EmptyVecShouldBeDeleted,
        //                     )
        //                     .into_storage_result();
        //                 }
        //                 if post > pre {
        //                     post_gt_pre = true;
        //                     len_diff = post - pre;
        //                 } else {
        //                     len_diff = pre - post;
        //                 }
        //                 len_pre = pre;
        //             }
        //             Data::Delete { pre } => {
        //                 len_diff = pre;
        //                 len_pre = pre;
        //             }
        //         },
        //         SubKeyWithData::Data(index, data) => match data {
        //             Data::Add { post } => {
        //                 actions.push(Action::Push(post));
        //                 added.insert(index);
        //             }
        //             Data::Update { pre, post } => {
        //                 actions.push(Action::Update { index, pre, post });
        //                 updated.insert(index);
        //             }
        //             Data::Delete { pre } => {
        //                 actions.push(Action::Pop(pre));
        //                 deleted.insert(index);
        //             }
        //         },
        //     }
        // }
        // let added_len: u64 = added
        //     .len()
        //     .try_into()
        //     .map_err(ValidationError::IndexOverflow)
        //     .into_storage_result()?;
        // let deleted_len: u64 = deleted
        //     .len()
        //     .try_into()
        //     .map_err(ValidationError::IndexOverflow)
        //     .into_storage_result()?;

        // if len_diff != 0
        //     && !(if post_gt_pre {
        //         deleted_len + len_diff == added_len
        //     } else {
        //         added_len + len_diff == deleted_len
        //     })
        // {
        //     return
        // Err(ValidationError::InvalidLenDiff).into_storage_result(); }

        // let mut last_added = Option::None;
        // // Iterate additions in increasing order of indices
        // for index in added {
        //     if let Some(last_added) = last_added {
        //         // Following additions should be at monotonically increasing
        //         // indices
        //         let expected = last_added + 1;
        //         if expected != index {
        //             return Err(ValidationError::UnexpectedPushIndex {
        //                 got: index,
        //                 expected,
        //             })
        //             .into_storage_result();
        //         }
        //     } else if index != len_pre {
        //         // The first addition must be at the pre length value.
        //         // If something is deleted and a new value is added
        //         // in its place, it will go through `Data::Update`
        //         // instead.
        //         return Err(ValidationError::UnexpectedPushIndex {
        //             got: index,
        //             expected: len_pre,
        //         })
        //         .into_storage_result();
        //     }
        //     last_added = Some(index);
        // }

        // let mut last_deleted = Option::None;
        // // Also iterate deletions in increasing order of indices
        // for index in deleted {
        //     if let Some(last_added) = last_deleted {
        //         // Following deletions should be at monotonically increasing
        //         // indices
        //         let expected = last_added + 1;
        //         if expected != index {
        //             return Err(ValidationError::UnexpectedPopIndex {
        //                 got: index,
        //                 expected,
        //             })
        //             .into_storage_result();
        //         }
        //     }
        //     last_deleted = Some(index);
        // }
        // if let Some(index) = last_deleted {
        //     if len_pre > 0 {
        //         let expected = len_pre - 1;
        //         if index != expected {
        //             // The last deletion must be at the pre length value
        // minus 1             return
        // Err(ValidationError::UnexpectedPopIndex {
        // got: index,                 expected: len_pre,
        //             })
        //             .into_storage_result();
        //         }
        //     }
        // }

        // // And finally iterate updates
        // for index in updated {
        //     // Update index has to be within the length bounds
        //     let max = len_pre + len_diff;
        //     if index >= max {
        //         return Err(ValidationError::UnexpectedUpdateIndex {
        //             got: index,
        //             max,
        //         })
        //         .into_storage_result();
        //     }
        // }

        // Ok(actions)
    }
}

// Generic `LazySet` methods that require no bounds on values `T`
impl<T: Hash> LazySet<T> {
    /// Returns `true` if the set contains no elements.
    pub fn is_empty<S>(&self, storage: &S) -> Result<bool>
    where
        S: StorageRead,
    {
        let mut iter =
            storage_api::iter_prefix_bytes(storage, &self.get_data_prefix())?;
        Ok(iter.next().is_none())
    }

    /// Get the prefix of set's elements storage
    fn get_data_prefix(&self) -> storage::Key {
        self.key.push(&DATA_SUBKEY.to_owned()).unwrap()
    }

    // /// Get the sub-key of vector's elements storage
    fn get_data_key(&self, hash: ValueHash) -> storage::Key {
        self.get_data_prefix().push(&hash).unwrap()
    }
}

// `LazySet` methods with borsh encoded values `T`
impl<T> LazySet<T>
where
    T: BorshSerialize + BorshDeserialize + 'static + Debug + Hash,
{
    /// Inserts a value into the set.
    ///
    /// Returns a bool indicating if the value was already in the set.
    pub fn insert<S>(&self, storage: &mut S, val: T) -> Result<()>
    where
        S: StorageWrite + StorageRead,
    {
        let hash = self.hash(&val)?;

        let data_key = self.get_data_key(hash);
        storage.write(&data_key, val)?;

        Ok(())
    }

    /// Removes a value from the set.
    pub fn remove<S>(&self, storage: &mut S, val: T) -> Result<()>
    where
        S: StorageWrite + StorageRead,
    {
        let hash = self.hash(&val)?;
        let data_key = self.get_data_key(hash);
        storage.delete(&data_key)?;

        Ok(())
    }

    /// Return `true` if the value is in the set.
    pub fn contains<S>(&self, storage: &S, val: &T) -> Result<bool>
    where
        S: StorageRead,
    {
        let hash = self.hash(val)?;
        let data_key = self.get_data_key(hash);
        Ok(storage.read_bytes(&data_key).transpose().is_some())
    }

    fn hash(&self, val: &T) -> Result<ValueHash> {
        let mut hasher = DefaultHasher::new();
        val.hash(&mut hasher);
        Ok(hasher.finish())
    }

    /// An iterator visiting all elements. The iterator element type is
    /// `Result<T>`, because iterator's call to `next` may fail with e.g. out of
    /// gas or data decoding error.
    ///
    /// Note that this function shouldn't be used in transactions and VPs code
    /// on unbounded sets to avoid gas usage increasing with the length of the
    /// set.
    pub fn iter<'iter>(
        &self,
        storage: &'iter impl StorageRead,
    ) -> Result<impl Iterator<Item = Result<T>> + 'iter> {
        let iter = storage_api::iter_prefix(storage, &self.get_data_prefix())?;
        Ok(iter.map(|key_val_res| {
            let (_key, val) = key_val_res?;
            Ok(val)
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ledger::storage::testing::TestWlStorage;

    #[test]
    fn test_lazy_set_basics() -> storage_api::Result<()> {
        let mut storage = TestWlStorage::default();

        let key = storage::Key::parse("test").unwrap();
        let lazy_set = LazySet::<u32>::open(key);

        // The map should be empty at first
        assert!(lazy_set.is_empty(&storage)?);
        assert!(!lazy_set.contains(&storage, &0)?);
        assert!(!lazy_set.contains(&storage, &1)?);
        assert!(lazy_set.iter(&storage)?.next().is_none());

        // Insert a new values and check that it's added
        let (val, val2) = (123, 456);
        lazy_set.insert(&mut storage, val)?;
        lazy_set.insert(&mut storage, val2)?;

        assert!(!lazy_set.contains(&storage, &0)?);
        assert!(lazy_set.contains(&storage, &val)?);
        assert!(lazy_set.contains(&storage, &val2)?);
        assert!(!lazy_set.is_empty(&storage)?);
        let mut set_it = lazy_set.iter(&storage)?;
        assert!(set_it.next().is_some());
        assert!(set_it.next().is_some());
        assert!(set_it.next().is_none());
        drop(set_it);

        // Remove the values and check the map contents
        lazy_set.remove(&mut storage, val)?;
        assert!(!lazy_set.is_empty(&storage)?);
        assert!(!lazy_set.contains(&storage, &0)?);
        assert!(!lazy_set.contains(&storage, &1)?);
        assert!(!lazy_set.contains(&storage, &val)?);
        assert!(lazy_set.contains(&storage, &val2)?);
        assert!(lazy_set.iter(&storage)?.next().is_some());
        lazy_set.remove(&mut storage, val2)?;
        assert!(lazy_set.is_empty(&storage)?);
        assert!(lazy_set.iter(&storage)?.next().is_none());

        Ok(())
    }
}
