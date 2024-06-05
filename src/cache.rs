use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::record_batch::RecordBatch;
use moka::sync::{Cache, CacheBuilder};

use crate::{
    proto::{
        read_batch, read_metadata, read_row, ArrowBatchFileMetadata, ArrowBatchTypes
    },
    reader::ArrowBatchContext
};


pub struct ArrowMetaCacheEntry {
    pub ts: SystemTime,
    pub meta: ArrowBatchFileMetadata
}

#[derive(Clone)]
pub struct ArrowCachedTables {
    pub root: RecordBatch,
    pub others: Cache<String, RecordBatch>,
}

pub struct ArrowBatchCache<'a> {
    context: &'a ArrowBatchContext,

    pub table_cache: Cache<String, Arc<ArrowCachedTables>>,
    pub metadata_cache: Cache<String, ArrowBatchFileMetadata>,
}

impl<'a> ArrowBatchCache<'a> {
    const DEFAULT_TABLE_CACHE: u64 = 10;

    pub fn new(
        context: &'a ArrowBatchContext
    ) -> Self {
        ArrowBatchCache {
            context,
            table_cache: Cache::new(Self::DEFAULT_TABLE_CACHE),
            metadata_cache: CacheBuilder::new(u64::MAX)
                .time_to_live(Duration::from_secs(60))
                .build(),
        }
    }

    pub fn get_metadata_for(
        &self,
        adjusted_ordinal: u64,
        table_name: &str,
    ) -> (String, bool) {
        let file_path = self
            .context
            .table_file_map
            .get(&adjusted_ordinal)
            .and_then(|m| m.get(table_name))
            .expect(format!("File path for {}-{} not found", adjusted_ordinal, table_name).as_str());

        let meta = read_metadata(file_path).unwrap();

        let cache_key = format!("{}-{}", adjusted_ordinal, table_name);

        if let Some(cached_meta) = self.metadata_cache.get(cache_key.as_str()) {
            if cached_meta.size == meta.size {
                return (cache_key, false);
            }
            self.metadata_cache.remove(cache_key.as_str());
        }

        self.metadata_cache.insert(cache_key.clone(), meta);
        (
            cache_key,
            true
        )
    }

    pub fn direct_load_table(
        &self,
        table_name: &str,
        adjusted_ordinal: u64,
        batch_index: usize,
    ) -> (String, Option<RecordBatch>) {
        let file_path = self
            .context
            .table_file_map
            .get(&adjusted_ordinal)
            .and_then(|m| m.get(table_name));

        match file_path {
            Some(path) => {
                let metadata = read_metadata(path).unwrap();
                let table = read_batch(path, &metadata, batch_index).unwrap();
                (table_name.to_string(), Some(table))
            }
            None => (table_name.to_string(), None),
        }
    }

    pub fn get_tables_for(&self, ordinal: u64) -> Option<(u64, Arc<ArrowCachedTables>)> {
        let adjusted_ordinal = self.context.get_ordinal(ordinal);

        let (bucket_metadata_key, metadata_updated) =
            self.get_metadata_for(adjusted_ordinal, "root");

        let bucket_metadata = self.metadata_cache.get(bucket_metadata_key.as_str()).unwrap();

        let bucket_start_ordinal = bucket_metadata.batches.get(0).unwrap().header.start_ordinal;
        let bucket_last_ordinal = bucket_metadata.batches.get(
            bucket_metadata.batches.len() - 1
        ).unwrap().header.last_ordinal;

        if ordinal < bucket_start_ordinal || ordinal > bucket_last_ordinal {
            println!("bucket {} -> {}", bucket_start_ordinal, bucket_last_ordinal);
            return None;
        }

        let mut batch_index = 0;
        while ordinal > bucket_metadata.batches.get(batch_index).unwrap().header.last_ordinal {
            batch_index += 1;
        }

        let batch_meta = bucket_metadata.batches.get(batch_index).unwrap();

        let cache_key = format!("{}-{}", adjusted_ordinal, batch_index);

        if self.table_cache.contains_key(&cache_key) {
            if !metadata_updated {
                let cached_table = self.table_cache.get(&cache_key).unwrap();
                return Some((batch_meta.header.start_ordinal, cached_table));
            }
        } else {
            self.table_cache.remove(&cache_key);
        }

        let table_load_list = std::iter::once(self.direct_load_table("root", adjusted_ordinal, batch_index))
            .chain(
                self.context
                    .table_mappings
                    .keys()
                    .map(|table_name| {
                        self.direct_load_table(table_name, adjusted_ordinal, batch_index)
                    }),
            )
            .collect::<Vec<_>>();

        let tables = ArrowCachedTables {
            root: table_load_list[0].1.as_ref().expect("Root table not found").clone(),
            others: Cache::new(Self::DEFAULT_TABLE_CACHE),
        };

        for (table_name, table) in table_load_list.into_iter().skip(1) {
            if let Some(table) = table {
                tables.others.insert(table_name, table);
            }
        }

        self.table_cache.insert(cache_key.clone(), Arc::from(tables));

        Some((batch_meta.header.start_ordinal, self.table_cache.get(&cache_key).unwrap()))
    }

}
