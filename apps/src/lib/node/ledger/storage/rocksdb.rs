//! The persistent storage in RocksDB.
//!
//! The current storage tree is:
//! - `height`: the last committed block height
//! - `tx_queue`: txs to be decrypted in the next block
//! - `pred`: predecessor values of the top-level keys of the same name
//!   - `tx_queue`
//! - `next_epoch_min_start_height`: minimum block height from which the next
//!   epoch can start
//! - `next_epoch_min_start_time`: minimum block time from which the next epoch
//!   can start
//! - `pred`: predecessor values of the top-level keys of the same name
//!   - `next_epoch_min_start_height`
//!   - `next_epoch_min_start_time`
//! - `subspace`: accounts sub-spaces
//!   - `{address}/{dyn}`: any byte data associated with accounts
//! - `results`: block results
//! - `h`: for each block at height `h`:
//!   - `tree`: merkle tree
//!     - `root`: root hash
//!     - `store`: the tree's store
//!   - `hash`: block hash
//!   - `epoch`: block epoch
//!   - `address_gen`: established address generator
//!   - `diffs`: diffs in account subspaces' key-vals
//!     - `new/{dyn}`: value set in block height `h`
//!     - `old/{dyn}`: value from predecessor block height
//!   - `header`: block's header

use std::cmp::Ordering;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use borsh::{BorshDeserialize, BorshSerialize};
use data_encoding::HEXLOWER;
use namada::ledger::storage::types::PrefixIterator;
use namada::ledger::storage::{
    types, BlockStateRead, BlockStateWrite, DBIter, DBWriteBatch, Error,
    MerkleTreeStoresRead, Result, StoreType, DB,
};
use namada::types::internal::TxQueue;
use namada::types::storage::{
    BlockHeight, BlockResults, Epoch, Epochs, Header, Key, KeySeg,
    KEY_SEGMENT_SEPARATOR,
};
use namada::types::time::DateTimeUtc;
use rayon::prelude::*;
use rocksdb::{
    BlockBasedOptions, Direction, FlushOptions, IteratorMode, Options,
    ReadOptions, SliceTransform, WriteBatch, WriteOptions,
};

use crate::config::utils::num_of_threads;

// TODO the DB schema will probably need some kind of versioning

/// Env. var to set a number of Rayon global worker threads
const ENV_VAR_ROCKSDB_COMPACTION_THREADS: &str =
    "NAMADA_ROCKSDB_COMPACTION_THREADS";

/// RocksDB handle
#[derive(Debug)]
pub struct RocksDB(rocksdb::DB);

/// DB Handle for batch writes.
#[derive(Default)]
pub struct RocksDBWriteBatch(WriteBatch);

/// Open RocksDB for the DB
pub fn open(
    path: impl AsRef<Path>,
    cache: Option<&rocksdb::Cache>,
) -> Result<RocksDB> {
    let logical_cores = num_cpus::get();
    let compaction_threads = num_of_threads(
        ENV_VAR_ROCKSDB_COMPACTION_THREADS,
        // If not set, default to quarter of logical CPUs count
        logical_cores / 4,
    ) as i32;
    tracing::info!(
        "Using {} compactions threads for RocksDB.",
        compaction_threads
    );

    let mut cf_opts = Options::default();
    // ! recommended initial setup https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
    cf_opts.set_level_compaction_dynamic_level_bytes(true);

    // This gives `compaction_threads` number to compaction threads and 1 thread
    // for flush background jobs: https://github.com/facebook/rocksdb/blob/17ce1ca48be53ba29138f92dafc9c853d9241377/options/options.cc#L622
    cf_opts.increase_parallelism(compaction_threads);

    cf_opts.set_bytes_per_sync(1048576);
    set_max_open_files(&mut cf_opts);

    cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    cf_opts.set_compression_options(0, 0, 0, 1024 * 1024);
    // TODO the recommended default `options.compaction_pri =
    // kMinOverlappingRatio` doesn't seem to be available in Rust
    let mut table_opts = BlockBasedOptions::default();
    table_opts.set_block_size(16 * 1024);
    table_opts.set_cache_index_and_filter_blocks(true);
    table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    if let Some(cache) = cache {
        table_opts.set_block_cache(cache);
    }
    // latest format versions https://github.com/facebook/rocksdb/blob/d1c510baecc1aef758f91f786c4fbee3bc847a63/include/rocksdb/table.h#L394
    table_opts.set_format_version(5);
    cf_opts.set_block_based_table_factory(&table_opts);

    cf_opts.create_missing_column_families(true);
    cf_opts.create_if_missing(true);
    cf_opts.set_atomic_flush(true);

    cf_opts.set_comparator("key_comparator", key_comparator);
    let extractor = SliceTransform::create_fixed_prefix(20);
    cf_opts.set_prefix_extractor(extractor);
    // TODO use column families

    rocksdb::DB::open_cf_descriptors(&cf_opts, path, vec![])
        .map(RocksDB)
        .map_err(|e| Error::DBError(e.into_string()))
}

/// A custom key comparator is used to sort keys by the height. In
/// lexicographical order, the height aren't ordered. For example, "11" is
/// before "2".
fn key_comparator(a: &[u8], b: &[u8]) -> Ordering {
    let a_str = &String::from_utf8(a.to_vec()).unwrap();
    let b_str = &String::from_utf8(b.to_vec()).unwrap();

    let a_vec: Vec<&str> = a_str.split('/').collect();
    let b_vec: Vec<&str> = b_str.split('/').collect();

    let result_a_h = a_vec[0].parse::<u64>();
    let result_b_h = b_vec[0].parse::<u64>();
    match (result_a_h, result_b_h) {
        (Ok(a_h), Ok(b_h)) => {
            if a_h == b_h {
                a_vec[1..].cmp(&b_vec[1..])
            } else {
                a_h.cmp(&b_h)
            }
        }
        _ => {
            // the key doesn't include the height
            a_str.cmp(b_str)
        }
    }
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        self.flush(true).expect("flush failed");
    }
}

impl RocksDB {
    fn flush(&self, wait: bool) -> Result<()> {
        let mut flush_opts = FlushOptions::default();
        flush_opts.set_wait(wait);
        self.0
            .flush_opt(&flush_opts)
            .map_err(|e| Error::DBError(e.into_string()))
    }

    /// Persist the diff of an account subspace key-val under the height where
    /// it was changed.
    fn write_subspace_diff(
        &mut self,
        height: BlockHeight,
        key: &Key,
        old_value: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<()> {
        let key_prefix = Key::from(height.to_db_key())
            .push(&"diffs".to_owned())
            .map_err(Error::KeyError)?;

        if let Some(old_value) = old_value {
            let old_val_key = key_prefix
                .push(&"old".to_owned())
                .map_err(Error::KeyError)?
                .join(key)
                .to_string();
            self.0
                .put(old_val_key, old_value)
                .map_err(|e| Error::DBError(e.into_string()))?;
        }

        if let Some(new_value) = new_value {
            let new_val_key = key_prefix
                .push(&"new".to_owned())
                .map_err(Error::KeyError)?
                .join(key)
                .to_string();
            self.0
                .put(new_val_key, new_value)
                .map_err(|e| Error::DBError(e.into_string()))?;
        }
        Ok(())
    }

    /// Persist the diff of an account subspace key-val under the height where
    /// it was changed in a batch write.
    fn batch_write_subspace_diff(
        batch: &mut RocksDBWriteBatch,
        height: BlockHeight,
        key: &Key,
        old_value: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<()> {
        let key_prefix = Key::from(height.to_db_key())
            .push(&"diffs".to_owned())
            .map_err(Error::KeyError)?;

        if let Some(old_value) = old_value {
            let old_val_key = key_prefix
                .push(&"old".to_owned())
                .map_err(Error::KeyError)?
                .join(key)
                .to_string();
            batch.0.put(old_val_key, old_value);
        }

        if let Some(new_value) = new_value {
            let new_val_key = key_prefix
                .push(&"new".to_owned())
                .map_err(Error::KeyError)?
                .join(key)
                .to_string();
            batch.0.put(new_val_key, new_value);
        }
        Ok(())
    }

    fn exec_batch(&mut self, batch: WriteBatch) -> Result<()> {
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true);
        self.0
            .write_opt(batch, &write_opts)
            .map_err(|e| Error::DBError(e.into_string()))
    }

    /// Dump last known block
    pub fn dump_last_block(
        &self,
        out_file_path: std::path::PathBuf,
        historic: bool,
    ) {
        use std::io::Write;

        // Find the last block height
        let height: BlockHeight = types::decode(
            self.0
                .get("height")
                .expect("Unable to read DB")
                .expect("No block height found"),
        )
        .expect("Unable to decode block height");

        let full_path = out_file_path
            .with_file_name(format!(
                "{}_{height}",
                out_file_path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "dump_db".to_string())
            ))
            .with_extension("toml");

        let mut file = File::options()
            .append(true)
            .create_new(true)
            .open(&full_path)
            .expect("Cannot open the output file");

        println!("Will write to {} ...", full_path.to_string_lossy());

        let mut dump_it = |prefix: String| {
            let mut read_opts = ReadOptions::default();
            read_opts.set_total_order_seek(true);

            let mut upper_prefix = prefix.clone().into_bytes();
            if let Some(last) = upper_prefix.pop() {
                upper_prefix.push(last + 1);
            }
            read_opts.set_iterate_upper_bound(upper_prefix);

            let iter = self.0.iterator_opt(
                IteratorMode::From(prefix.as_bytes(), Direction::Forward),
                read_opts,
            );

            for (key, raw_val, _gas) in PersistentPrefixIterator(
                PrefixIterator::new(iter, String::default()),
                // Empty string to prevent prefix stripping, the prefix is
                // already in the enclosed iterator
            ) {
                let val = HEXLOWER.encode(&raw_val);
                let bytes = format!("\"{key}\" = \"{val}\"\n");
                file.write_all(bytes.as_bytes())
                    .expect("Unable to write to output file");
            }
        };

        if historic {
            // Dump the keys prepended with the selected block height (includes
            // subspace diff keys)
            dump_it(height.raw());
        }

        dump_it("subspace".to_string());

        println!("Done writing to {}", full_path.to_string_lossy());
    }

    /// Rollback to previous block. Given the inner working of tendermint
    /// rollback and of the key structure of Namada, calling rollback more than
    /// once without restarting the chain results in a single rollback.
    pub fn rollback(
        &mut self,
        tendermint_block_height: BlockHeight,
    ) -> Result<()> {
        let last_block = self.read_last_block()?.ok_or(Error::DBError(
            "Missing last block in storage".to_string(),
        ))?;
        tracing::info!(
            "Namada last block height: {}, Tendermint last block height: {}",
            last_block.height,
            tendermint_block_height
        );

        // If the block height to which tendermint rolled back matches the
        // Namada height, there's no need to rollback
        if tendermint_block_height == last_block.height {
            tracing::info!(
                "Namada height already matches the rollback Tendermint \
                 height, no need to rollback."
            );
            return Ok(());
        }

        let mut batch = WriteBatch::default();
        let previous_height =
            BlockHeight::from(u64::from(last_block.height) - 1);

        // Revert the non-height-prepended metadata storage keys which get
        // updated with every block. Because of the way we save these
        // three keys in storage we can only perform one rollback before
        // restarting the chain
        tracing::info!("Reverting non-height-prepended metadata keys");
        batch.put("height", types::encode(&previous_height));
        for metadata_key in [
            "next_epoch_min_start_height",
            "next_epoch_min_start_time",
            "tx_queue",
        ] {
            let previous_key = format!("pred/{}", metadata_key);
            let previous_value = self
                .0
                .get(previous_key.as_bytes())
                .map_err(|e| Error::DBError(e.to_string()))?
                .ok_or(Error::UnknownKey { key: previous_key })?;

            batch.put(metadata_key, previous_value);
            // NOTE: we cannot restore the "pred/" keys themselves since we
            // don't have their predecessors in storage, but there's no need to
            // since we cannot do more than one rollback anyway because of
            // Tendermint.
        }

        // Delete block results for the last block
        tracing::info!("Removing last block results");
        batch.delete(format!("results/{}", last_block.height));

        // Execute next step in parallel
        let batch = Mutex::new(batch);

        tracing::info!("Restoring previous hight subspace diffs");
        self.iter_prefix(&Key::default())
            .par_bridge()
            .try_for_each(|(key, _value, _gas)| -> Result<()> {
                // Restore previous height diff if present, otherwise delete the
                // subspace key

                // Add the prefix back since `iter_prefix` has removed it
                let prefixed_key = format!("subspace/{}", key);

                match self.read_subspace_val_with_height(
                    &Key::from(key.to_db_key()),
                    previous_height,
                    last_block.height,
                )? {
                    Some(previous_value) => {
                        batch.lock().unwrap().put(&prefixed_key, previous_value)
                    }
                    None => batch.lock().unwrap().delete(&prefixed_key),
                }

                Ok(())
            })?;

        // Delete any height-prepended key, including subspace diff keys
        let mut batch = batch.into_inner().unwrap();
        let prefix = last_block.height.to_string();
        let mut read_opts = ReadOptions::default();
        read_opts.set_total_order_seek(true);
        let mut upper_prefix = prefix.clone().into_bytes();
        if let Some(last) = upper_prefix.pop() {
            upper_prefix.push(last + 1);
        }
        read_opts.set_iterate_upper_bound(upper_prefix);

        let iter = self.0.iterator_opt(
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
            read_opts,
        );
        tracing::info!("Deleting keys prepended with the last height");
        for (key, _value, _gas) in PersistentPrefixIterator(
            // Empty prefix string to prevent stripping
            PrefixIterator::new(iter, String::default()),
        ) {
            batch.delete(key);
        }

        // Write the batch and persist changes to disk
        tracing::info!("Flushing restored state to disk");
        self.exec_batch(batch)?;
        self.flush(true)
    }
}

impl DB for RocksDB {
    type Cache = rocksdb::Cache;
    type WriteBatch = RocksDBWriteBatch;

    fn open(
        db_path: impl AsRef<std::path::Path>,
        cache: Option<&Self::Cache>,
    ) -> Self {
        open(db_path, cache).expect("cannot open the DB")
    }

    fn flush(&self, wait: bool) -> Result<()> {
        let mut flush_opts = FlushOptions::default();
        flush_opts.set_wait(wait);
        self.0
            .flush_opt(&flush_opts)
            .map_err(|e| Error::DBError(e.into_string()))
    }

    fn read_last_block(&mut self) -> Result<Option<BlockStateRead>> {
        // Block height
        let height: BlockHeight = match self
            .0
            .get("height")
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(bytes) => {
                // TODO if there's an issue decoding this height, should we try
                // load its predecessor instead?
                types::decode(bytes).map_err(Error::CodingError)?
            }
            None => return Ok(None),
        };

        // Block results
        let results_path = format!("results/{}", height.raw());
        let results: BlockResults = match self
            .0
            .get(results_path)
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(bytes) => types::decode(bytes).map_err(Error::CodingError)?,
            None => return Ok(None),
        };

        // Epoch start height and time
        let next_epoch_min_start_height: BlockHeight = match self
            .0
            .get("next_epoch_min_start_height")
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(bytes) => types::decode(bytes).map_err(Error::CodingError)?,
            None => {
                tracing::error!(
                    "Couldn't load next epoch start height from the DB"
                );
                return Ok(None);
            }
        };
        let next_epoch_min_start_time: DateTimeUtc = match self
            .0
            .get("next_epoch_min_start_time")
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(bytes) => types::decode(bytes).map_err(Error::CodingError)?,
            None => {
                tracing::error!(
                    "Couldn't load next epoch start time from the DB"
                );
                return Ok(None);
            }
        };
        let tx_queue: TxQueue = match self
            .0
            .get("tx_queue")
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(bytes) => types::decode(bytes).map_err(Error::CodingError)?,
            None => {
                tracing::error!("Couldn't load tx queue from the DB");
                return Ok(None);
            }
        };

        // Load data at the height
        let prefix = format!("{}/", height.raw());
        let mut read_opts = ReadOptions::default();
        read_opts.set_total_order_seek(false);
        let next_height_prefix = format!("{}/", height.next_height().raw());
        read_opts.set_iterate_upper_bound(next_height_prefix);
        let mut merkle_tree_stores = MerkleTreeStoresRead::default();
        let mut hash = None;
        let mut epoch = None;
        let mut pred_epochs = None;
        let mut address_gen = None;
        for value in self.0.iterator_opt(
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
            read_opts,
        ) {
            let (key, bytes) = match value {
                Ok(data) => data,
                Err(e) => return Err(Error::DBError(e.into_string())),
            };
            let path = &String::from_utf8((*key).to_vec()).map_err(|e| {
                Error::Temporary {
                    error: format!(
                        "Cannot convert path from utf8 bytes to string: {}",
                        e
                    ),
                }
            })?;
            let segments: Vec<&str> =
                path.split(KEY_SEGMENT_SEPARATOR).collect();
            match segments.get(1) {
                Some(prefix) => match *prefix {
                    "tree" => match segments.get(2) {
                        Some(s) => {
                            let st = StoreType::from_str(s)?;
                            match segments.get(3) {
                                Some(&"root") => merkle_tree_stores.set_root(
                                    &st,
                                    types::decode(bytes)
                                        .map_err(Error::CodingError)?,
                                ),
                                Some(&"store") => merkle_tree_stores
                                    .set_store(st.decode_store(bytes)?),
                                _ => unknown_key_error(path)?,
                            }
                        }
                        None => unknown_key_error(path)?,
                    },
                    "header" => {
                        // the block header doesn't have to be restored
                    }
                    "hash" => {
                        hash = Some(
                            types::decode(bytes).map_err(Error::CodingError)?,
                        )
                    }
                    "epoch" => {
                        epoch = Some(
                            types::decode(bytes).map_err(Error::CodingError)?,
                        )
                    }
                    "pred_epochs" => {
                        pred_epochs = Some(
                            types::decode(bytes).map_err(Error::CodingError)?,
                        )
                    }
                    "address_gen" => {
                        address_gen = Some(
                            types::decode(bytes).map_err(Error::CodingError)?,
                        );
                    }
                    "diffs" => {
                        // ignore the diffs
                    }
                    _ => unknown_key_error(path)?,
                },
                None => unknown_key_error(path)?,
            }
        }
        match (hash, epoch, pred_epochs, address_gen) {
            (Some(hash), Some(epoch), Some(pred_epochs), Some(address_gen)) => {
                Ok(Some(BlockStateRead {
                    merkle_tree_stores,
                    hash,
                    height,
                    epoch,
                    pred_epochs,
                    results,
                    next_epoch_min_start_height,
                    next_epoch_min_start_time,
                    address_gen,
                    tx_queue,
                }))
            }
            _ => Err(Error::Temporary {
                error: "Essential data couldn't be read from the DB"
                    .to_string(),
            }),
        }
    }

    fn write_block(
        &mut self,
        state: BlockStateWrite,
        is_full_commit: bool,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        let BlockStateWrite {
            merkle_tree_stores,
            header,
            hash,
            height,
            epoch,
            pred_epochs,
            results,
            next_epoch_min_start_height,
            next_epoch_min_start_time,
            address_gen,
            tx_queue,
        }: BlockStateWrite = state;

        // Epoch start height and time
        if let Some(current_value) =
            self.0
                .get("next_epoch_min_start_height")
                .map_err(|e| Error::DBError(e.into_string()))?
        {
            // Write the predecessor value for rollback
            batch.put("pred/next_epoch_min_start_height", current_value);
        }
        batch.put(
            "next_epoch_min_start_height",
            types::encode(&next_epoch_min_start_height),
        );

        if let Some(current_value) = self
            .0
            .get("next_epoch_min_start_time")
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            // Write the predecessor value for rollback
            batch.put("pred/next_epoch_min_start_time", current_value);
        }
        batch.put(
            "next_epoch_min_start_time",
            types::encode(&next_epoch_min_start_time),
        );
        // Tx queue
        if let Some(pred_tx_queue) = self
            .0
            .get("tx_queue")
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            // Write the predecessor value for rollback
            batch.put("pred/tx_queue", pred_tx_queue);
        }
        batch.put("tx_queue", types::encode(&tx_queue));

        let prefix_key = Key::from(height.to_db_key());
        // Merkle tree
        {
            let prefix_key = prefix_key
                .push(&"tree".to_owned())
                .map_err(Error::KeyError)?;
            for st in StoreType::iter() {
                if *st == StoreType::Base || is_full_commit {
                    let prefix_key = prefix_key
                        .push(&st.to_string())
                        .map_err(Error::KeyError)?;
                    let root_key = prefix_key
                        .push(&"root".to_owned())
                        .map_err(Error::KeyError)?;
                    batch.put(
                        root_key.to_string(),
                        types::encode(merkle_tree_stores.root(st)),
                    );
                    let store_key = prefix_key
                        .push(&"store".to_owned())
                        .map_err(Error::KeyError)?;
                    batch.put(
                        store_key.to_string(),
                        merkle_tree_stores.store(st).encode(),
                    );
                }
            }
        }
        // Block header
        {
            if let Some(h) = header {
                let key = prefix_key
                    .push(&"header".to_owned())
                    .map_err(Error::KeyError)?;
                batch.put(
                    key.to_string(),
                    h.try_to_vec().expect("serialization failed"),
                );
            }
        }
        // Block hash
        {
            let key = prefix_key
                .push(&"hash".to_owned())
                .map_err(Error::KeyError)?;
            batch.put(key.to_string(), types::encode(&hash));
        }
        // Block epoch
        {
            let key = prefix_key
                .push(&"epoch".to_owned())
                .map_err(Error::KeyError)?;
            batch.put(key.to_string(), types::encode(&epoch));
        }
        // Block results
        {
            let results_path = format!("results/{}", height.raw());
            batch.put(results_path, types::encode(&results));
        }
        // Predecessor block epochs
        {
            let key = prefix_key
                .push(&"pred_epochs".to_owned())
                .map_err(Error::KeyError)?;
            batch.put(key.to_string(), types::encode(&pred_epochs));
        }
        // Address gen
        {
            let key = prefix_key
                .push(&"address_gen".to_owned())
                .map_err(Error::KeyError)?;
            batch.put(key.to_string(), types::encode(&address_gen));
        }

        // Block height
        batch.put("height", types::encode(&height));

        // Write the batch
        self.exec_batch(batch)?;

        // Flush without waiting
        self.flush(false)
    }

    fn read_block_header(&self, height: BlockHeight) -> Result<Option<Header>> {
        let prefix_key = Key::from(height.to_db_key());
        let key = prefix_key
            .push(&"header".to_owned())
            .map_err(Error::KeyError)?;
        let value = self
            .0
            .get(key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?;
        match value {
            Some(v) => Ok(Some(
                Header::try_from_slice(&v[..])
                    .map_err(Error::BorshCodingError)?,
            )),
            None => Ok(None),
        }
    }

    fn read_merkle_tree_stores(
        &self,
        height: BlockHeight,
    ) -> Result<Option<(BlockHeight, MerkleTreeStoresRead)>> {
        // Get the latest height at which the tree stores were written
        let height_key = Key::from(height.to_db_key());
        let key = height_key
            .push(&"pred_epochs".to_owned())
            .expect("Cannot obtain a storage key");
        let pred_epochs: Epochs = match self
            .0
            .get(key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(b) => types::decode(b).map_err(Error::CodingError)?,
            None => return Ok(None),
        };
        // Read the tree at the first height if no epoch update
        let stored_height = match pred_epochs.get_epoch_start_height(height) {
            Some(BlockHeight(0)) | None => BlockHeight(1),
            Some(h) => h,
        };

        let tree_key = Key::from(stored_height.to_db_key())
            .push(&"tree".to_owned())
            .map_err(Error::KeyError)?;
        let mut merkle_tree_stores = MerkleTreeStoresRead::default();
        for st in StoreType::iter() {
            let prefix_key =
                tree_key.push(&st.to_string()).map_err(Error::KeyError)?;
            let root_key = prefix_key
                .push(&"root".to_owned())
                .map_err(Error::KeyError)?;
            let bytes = self
                .0
                .get(root_key.to_string())
                .map_err(|e| Error::DBError(e.into_string()))?;
            match bytes {
                Some(b) => {
                    let root = types::decode(b).map_err(Error::CodingError)?;
                    merkle_tree_stores.set_root(st, root);
                }
                None => return Ok(None),
            }

            let store_key = prefix_key
                .push(&"store".to_owned())
                .map_err(Error::KeyError)?;
            let bytes = self
                .0
                .get(store_key.to_string())
                .map_err(|e| Error::DBError(e.into_string()))?;
            match bytes {
                Some(b) => {
                    merkle_tree_stores.set_store(st.decode_store(b)?);
                }
                None => return Ok(None),
            }
        }
        Ok(Some((stored_height, merkle_tree_stores)))
    }

    fn read_subspace_val(&self, key: &Key) -> Result<Option<Vec<u8>>> {
        let subspace_key =
            Key::parse("subspace").map_err(Error::KeyError)?.join(key);
        self.0
            .get(subspace_key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))
    }

    fn read_subspace_val_with_height(
        &self,
        key: &Key,
        height: BlockHeight,
        last_height: BlockHeight,
    ) -> Result<Option<Vec<u8>>> {
        // Check if the value changed at this height
        let key_prefix = Key::from(height.to_db_key())
            .push(&"diffs".to_owned())
            .map_err(Error::KeyError)?;
        let new_val_key = key_prefix
            .push(&"new".to_owned())
            .map_err(Error::KeyError)?
            .join(key)
            .to_string();

        // If it has a "new" val, it was written at this height
        match self
            .0
            .get(new_val_key)
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(new_val) => {
                return Ok(Some(new_val));
            }
            None => {
                let old_val_key = key_prefix
                    .push(&"old".to_owned())
                    .map_err(Error::KeyError)?
                    .join(key)
                    .to_string();
                // If it has an "old" val, it was deleted at this height
                if self.0.key_may_exist(old_val_key.clone()) {
                    // check if it actually exists
                    if self
                        .0
                        .get(old_val_key)
                        .map_err(|e| Error::DBError(e.into_string()))?
                        .is_some()
                    {
                        return Ok(None);
                    }
                }
            }
        }

        // If the value didn't change at the given height, we try to look for it
        // at successor heights, up to the `last_height`
        let mut raw_height = height.0 + 1;
        loop {
            // Try to find the next diff on this key
            let key_prefix = Key::from(BlockHeight(raw_height).to_db_key())
                .push(&"diffs".to_owned())
                .map_err(Error::KeyError)?;
            let old_val_key = key_prefix
                .push(&"old".to_owned())
                .map_err(Error::KeyError)?
                .join(key)
                .to_string();
            let old_val = self
                .0
                .get(old_val_key)
                .map_err(|e| Error::DBError(e.into_string()))?;
            // If it has an "old" val, it's the one we're looking for
            match old_val {
                Some(bytes) => return Ok(Some(bytes)),
                None => {
                    // Check if the value was created at this height instead,
                    // which would mean that it wasn't present before
                    let new_val_key = key_prefix
                        .push(&"new".to_owned())
                        .map_err(Error::KeyError)?
                        .join(key)
                        .to_string();
                    if self.0.key_may_exist(new_val_key.clone()) {
                        // check if it actually exists
                        if self
                            .0
                            .get(new_val_key)
                            .map_err(|e| Error::DBError(e.into_string()))?
                            .is_some()
                        {
                            return Ok(None);
                        }
                    }

                    if raw_height >= last_height.0 {
                        // Read from latest height
                        return self.read_subspace_val(key);
                    } else {
                        raw_height += 1
                    }
                }
            }
        }
    }

    fn write_subspace_val(
        &mut self,
        height: BlockHeight,
        key: &Key,
        value: impl AsRef<[u8]>,
    ) -> Result<i64> {
        let value = value.as_ref();
        let subspace_key =
            Key::parse("subspace").map_err(Error::KeyError)?.join(key);
        let size_diff = match self
            .0
            .get(subspace_key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(prev_value) => {
                let size_diff = value.len() as i64 - prev_value.len() as i64;
                self.write_subspace_diff(
                    height,
                    key,
                    Some(&prev_value),
                    Some(value),
                )?;
                size_diff
            }
            None => {
                self.write_subspace_diff(height, key, None, Some(value))?;
                value.len() as i64
            }
        };

        // Write the new key-val
        self.0
            .put(subspace_key.to_string(), value)
            .map_err(|e| Error::DBError(e.into_string()))?;

        Ok(size_diff)
    }

    fn delete_subspace_val(
        &mut self,
        height: BlockHeight,
        key: &Key,
    ) -> Result<i64> {
        let subspace_key =
            Key::parse("subspace").map_err(Error::KeyError)?.join(key);

        // Check the length of previous value, if any
        let prev_len = match self
            .0
            .get(subspace_key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(prev_value) => {
                let prev_len = prev_value.len() as i64;
                self.write_subspace_diff(height, key, Some(&prev_value), None)?;
                prev_len
            }
            None => 0,
        };

        // Delete the key-val
        self.0
            .delete(subspace_key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?;

        Ok(prev_len)
    }

    fn batch() -> Self::WriteBatch {
        RocksDBWriteBatch::default()
    }

    fn exec_batch(&mut self, batch: Self::WriteBatch) -> Result<()> {
        self.exec_batch(batch.0)
    }

    fn batch_write_subspace_val(
        &self,
        batch: &mut Self::WriteBatch,
        height: BlockHeight,
        key: &Key,
        value: impl AsRef<[u8]>,
    ) -> Result<i64> {
        let value = value.as_ref();
        let subspace_key =
            Key::parse("subspace").map_err(Error::KeyError)?.join(key);
        let size_diff = match self
            .0
            .get(subspace_key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(old_value) => {
                let size_diff = value.len() as i64 - old_value.len() as i64;
                // Persist the previous value
                Self::batch_write_subspace_diff(
                    batch,
                    height,
                    key,
                    Some(&old_value),
                    Some(value),
                )?;
                size_diff
            }
            None => {
                Self::batch_write_subspace_diff(
                    batch,
                    height,
                    key,
                    None,
                    Some(value),
                )?;
                value.len() as i64
            }
        };

        // Write the new key-val
        batch.put(&subspace_key.to_string(), value);

        Ok(size_diff)
    }

    fn batch_delete_subspace_val(
        &self,
        batch: &mut Self::WriteBatch,
        height: BlockHeight,
        key: &Key,
    ) -> Result<i64> {
        let subspace_key =
            Key::parse("subspace").map_err(Error::KeyError)?.join(key);

        // Check the length of previous value, if any
        let prev_len = match self
            .0
            .get(subspace_key.to_string())
            .map_err(|e| Error::DBError(e.into_string()))?
        {
            Some(prev_value) => {
                let prev_len = prev_value.len() as i64;
                // Persist the previous value
                Self::batch_write_subspace_diff(
                    batch,
                    height,
                    key,
                    Some(&prev_value),
                    None,
                )?;
                prev_len
            }
            None => 0,
        };

        // Delete the key-val
        batch.delete(subspace_key.to_string());

        Ok(prev_len)
    }

    fn prune_merkle_tree_stores(
        &mut self,
        epoch: Epoch,
        pred_epochs: &Epochs,
    ) -> Result<()> {
        match pred_epochs.get_start_height_of_epoch(epoch) {
            Some(height) => {
                let mut batch = WriteBatch::default();
                let prefix_key = Key::from(height.to_db_key())
                    .push(&"tree".to_owned())
                    .map_err(Error::KeyError)?;
                for st in StoreType::iter() {
                    if *st != StoreType::Base {
                        let prefix_key = prefix_key
                            .push(&st.to_string())
                            .map_err(Error::KeyError)?;
                        let root_key = prefix_key
                            .push(&"root".to_owned())
                            .map_err(Error::KeyError)?;
                        batch.delete(root_key.to_string());
                        let store_key = prefix_key
                            .push(&"store".to_owned())
                            .map_err(Error::KeyError)?;
                        batch.delete(store_key.to_string());
                    }
                }
                self.exec_batch(batch)
            }
            None => Ok(()),
        }
    }
}

impl<'iter> DBIter<'iter> for RocksDB {
    type PrefixIter = PersistentPrefixIterator<'iter>;

    fn iter_prefix(
        &'iter self,
        prefix: &Key,
    ) -> PersistentPrefixIterator<'iter> {
        iter_subspace_prefix(self, prefix)
    }

    fn iter_results(&'iter self) -> PersistentPrefixIterator<'iter> {
        let db_prefix = "results/".to_owned();
        let prefix = "results".to_owned();

        let mut read_opts = ReadOptions::default();
        // don't use the prefix bloom filter
        read_opts.set_total_order_seek(true);
        let mut upper_prefix = prefix.clone().into_bytes();
        if let Some(last) = upper_prefix.pop() {
            upper_prefix.push(last + 1);
        }
        read_opts.set_iterate_upper_bound(upper_prefix);

        let iter = self.0.iterator_opt(
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
            read_opts,
        );
        PersistentPrefixIterator(PrefixIterator::new(iter, db_prefix))
    }

    fn iter_old_diffs(
        &'iter self,
        height: BlockHeight,
    ) -> PersistentPrefixIterator<'iter> {
        iter_diffs_prefix(self, height, true)
    }

    fn iter_new_diffs(
        &'iter self,
        height: BlockHeight,
    ) -> PersistentPrefixIterator<'iter> {
        iter_diffs_prefix(self, height, false)
    }
}

fn iter_subspace_prefix<'iter>(
    db: &'iter RocksDB,
    prefix: &Key,
) -> PersistentPrefixIterator<'iter> {
    let db_prefix = "subspace/".to_owned();
    let prefix = format!("{}{}", db_prefix, prefix);
    iter_prefix(db, db_prefix, prefix)
}

fn iter_diffs_prefix(
    db: &RocksDB,
    height: BlockHeight,
    is_old: bool,
) -> PersistentPrefixIterator {
    let prefix = if is_old { "old" } else { "new" };
    let db_prefix = format!("{}/diffs/{}/", height.0.raw(), prefix);
    // get keys without a prefix
    iter_prefix(db, db_prefix.clone(), db_prefix)
}

fn iter_prefix(
    db: &RocksDB,
    db_prefix: String,
    prefix: String,
) -> PersistentPrefixIterator {
    let mut read_opts = ReadOptions::default();
    // don't use the prefix bloom filter
    read_opts.set_total_order_seek(true);
    let mut upper_prefix = prefix.clone().into_bytes();
    if let Some(last) = upper_prefix.pop() {
        upper_prefix.push(last + 1);
    }
    read_opts.set_iterate_upper_bound(upper_prefix);

    let iter = db.0.iterator_opt(
        IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        read_opts,
    );
    PersistentPrefixIterator(PrefixIterator::new(iter, db_prefix))
}

#[derive(Debug)]
pub struct PersistentPrefixIterator<'a>(
    PrefixIterator<rocksdb::DBIterator<'a>>,
);

impl<'a> Iterator for PersistentPrefixIterator<'a> {
    type Item = (String, Vec<u8>, u64);

    /// Returns the next pair and the gas cost
    fn next(&mut self) -> Option<(String, Vec<u8>, u64)> {
        match self.0.iter.next() {
            Some(result) => {
                let (key, val) =
                    result.expect("Prefix iterator shouldn't fail");
                let key = String::from_utf8(key.to_vec())
                    .expect("Cannot convert from bytes to key string");
                match key.strip_prefix(&self.0.db_prefix) {
                    Some(k) => {
                        let gas = k.len() + val.len();
                        Some((k.to_owned(), val.to_vec(), gas as _))
                    }
                    None => self.next(),
                }
            }
            None => None,
        }
    }
}

impl DBWriteBatch for RocksDBWriteBatch {
    fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.0.put(key, value)
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.0.delete(key)
    }
}

fn unknown_key_error(key: &str) -> Result<()> {
    Err(Error::UnknownKey {
        key: key.to_owned(),
    })
}

/// Try to increase NOFILE limit and set the `max_open_files` limit to it in
/// RocksDB options.
fn set_max_open_files(cf_opts: &mut rocksdb::Options) {
    #[cfg(unix)]
    imp::set_max_open_files(cf_opts);
    // Nothing to do on non-unix
    #[cfg(not(unix))]
    let _ = cf_opts;
}

#[cfg(unix)]
mod imp {
    use std::convert::TryInto;

    use rlimit::{Resource, Rlim};

    const DEFAULT_NOFILE_LIMIT: Rlim = Rlim::from_raw(16384);

    pub fn set_max_open_files(cf_opts: &mut rocksdb::Options) {
        let max_open_files = match increase_nofile_limit() {
            Ok(max_open_files) => Some(max_open_files),
            Err(err) => {
                tracing::error!("Failed to increase NOFILE limit: {}", err);
                None
            }
        };
        if let Some(max_open_files) =
            max_open_files.and_then(|max| max.as_raw().try_into().ok())
        {
            cf_opts.set_max_open_files(max_open_files);
        }
    }

    /// Try to increase NOFILE limit and return the current soft limit.
    fn increase_nofile_limit() -> std::io::Result<Rlim> {
        let (soft, hard) = Resource::NOFILE.get()?;
        tracing::debug!("Current NOFILE limit, soft={}, hard={}", soft, hard);

        let target = std::cmp::min(DEFAULT_NOFILE_LIMIT, hard);
        if soft >= target {
            tracing::debug!(
                "NOFILE limit already large enough, not attempting to increase"
            );
            Ok(soft)
        } else {
            tracing::debug!("Try to increase to {}", target);
            Resource::NOFILE.set(target, target)?;

            let (soft, hard) = Resource::NOFILE.get()?;
            tracing::debug!(
                "Increased NOFILE limit, soft={}, hard={}",
                soft,
                hard
            );
            Ok(soft)
        }
    }
}

#[cfg(test)]
mod test {
    use namada::ledger::storage::{MerkleTree, Sha256Hasher};
    use namada::types::address::EstablishedAddressGen;
    use namada::types::storage::{BlockHash, Epoch, Epochs};
    use tempfile::tempdir;

    use super::*;

    /// Test that a block written can be loaded back from DB.
    #[test]
    fn test_load_state() {
        let dir = tempdir().unwrap();
        let mut db = open(dir.path(), None).unwrap();

        let mut batch = RocksDB::batch();
        let last_height = BlockHeight::default();
        db.batch_write_subspace_val(
            &mut batch,
            last_height,
            &Key::parse("test").unwrap(),
            vec![1_u8, 1, 1, 1],
        )
        .unwrap();
        db.exec_batch(batch.0).unwrap();

        let merkle_tree = MerkleTree::<Sha256Hasher>::default();
        let merkle_tree_stores = merkle_tree.stores();
        let hash = BlockHash::default();
        let epoch = Epoch::default();
        let pred_epochs = Epochs::default();
        let height = BlockHeight::default();
        let next_epoch_min_start_height = BlockHeight::default();
        let next_epoch_min_start_time = DateTimeUtc::now();
        let address_gen = EstablishedAddressGen::new("whatever");
        let tx_queue = TxQueue::default();
        let results = BlockResults::default();
        let block = BlockStateWrite {
            merkle_tree_stores,
            header: None,
            hash: &hash,
            height,
            epoch,
            results: &results,
            pred_epochs: &pred_epochs,
            next_epoch_min_start_height,
            next_epoch_min_start_time,
            address_gen: &address_gen,
            tx_queue: &tx_queue,
        };

        db.write_block(block, true).unwrap();

        let _state = db
            .read_last_block()
            .expect("Should be able to read last block")
            .expect("Block should have been written");
    }

    #[test]
    fn test_read() {
        let dir = tempdir().unwrap();
        let mut db = open(dir.path(), None).unwrap();

        let key = Key::parse("test").unwrap();
        let batch_key = Key::parse("batch").unwrap();

        let mut batch = RocksDB::batch();
        let last_height = BlockHeight(100);
        db.batch_write_subspace_val(
            &mut batch,
            last_height,
            &batch_key,
            vec![1_u8, 1, 1, 1],
        )
        .unwrap();
        db.exec_batch(batch.0).unwrap();

        db.write_subspace_val(last_height, &key, vec![1_u8, 1, 1, 0])
            .unwrap();

        let mut batch = RocksDB::batch();
        let last_height = BlockHeight(111);
        db.batch_write_subspace_val(
            &mut batch,
            last_height,
            &batch_key,
            vec![2_u8, 2, 2, 2],
        )
        .unwrap();
        db.exec_batch(batch.0).unwrap();

        db.write_subspace_val(last_height, &key, vec![2_u8, 2, 2, 0])
            .unwrap();

        let prev_value = db
            .read_subspace_val_with_height(
                &batch_key,
                BlockHeight(100),
                last_height,
            )
            .expect("read should succeed");
        assert_eq!(prev_value, Some(vec![1_u8, 1, 1, 1]));
        let prev_value = db
            .read_subspace_val_with_height(&key, BlockHeight(100), last_height)
            .expect("read should succeed");
        assert_eq!(prev_value, Some(vec![1_u8, 1, 1, 0]));

        let updated_value = db
            .read_subspace_val_with_height(
                &batch_key,
                BlockHeight(111),
                last_height,
            )
            .expect("read should succeed");
        assert_eq!(updated_value, Some(vec![2_u8, 2, 2, 2]));
        let updated_value = db
            .read_subspace_val_with_height(&key, BlockHeight(111), last_height)
            .expect("read should succeed");
        assert_eq!(updated_value, Some(vec![2_u8, 2, 2, 0]));

        let latest_value = db
            .read_subspace_val(&batch_key)
            .expect("read should succeed");
        assert_eq!(latest_value, Some(vec![2_u8, 2, 2, 2]));
        let latest_value =
            db.read_subspace_val(&key).expect("read should succeed");
        assert_eq!(latest_value, Some(vec![2_u8, 2, 2, 0]));

        let mut batch = RocksDB::batch();
        let last_height = BlockHeight(222);
        db.batch_delete_subspace_val(&mut batch, last_height, &batch_key)
            .unwrap();
        db.exec_batch(batch.0).unwrap();

        db.delete_subspace_val(last_height, &key).unwrap();

        let deleted_value = db
            .read_subspace_val_with_height(
                &batch_key,
                BlockHeight(222),
                last_height,
            )
            .expect("read should succeed");
        assert_eq!(deleted_value, None);
        let deleted_value = db
            .read_subspace_val_with_height(&key, BlockHeight(222), last_height)
            .expect("read should succeed");
        assert_eq!(deleted_value, None);

        let latest_value = db
            .read_subspace_val(&batch_key)
            .expect("read should succeed");
        assert_eq!(latest_value, None);
        let latest_value =
            db.read_subspace_val(&key).expect("read should succeed");
        assert_eq!(latest_value, None);
    }
}
