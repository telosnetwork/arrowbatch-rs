use std::sync::{Arc, Mutex};
use std::fs::metadata;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use moka::sync::{Cache, CacheBuilder};

use crate::reader::get_relative_table_index;
use crate::{
    proto::{
        read_batch, read_metadata, ArrowBatchFileMetadata
    },
    reader::ArrowBatchContext
};


pub struct ArrowBatchCache {
    context: Arc<Mutex<ArrowBatchContext>>,

    pub table_cache: Cache<String, Arc<RecordBatch>>,
    pub metadata_cache: Cache<u64, ArrowBatchFileMetadata>,
}

impl ArrowBatchCache {
    const DEFAULT_TABLE_CACHE: u64 = 10;

    pub fn new(
        context: Arc<Mutex<ArrowBatchContext>>
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
        adjusted_ordinal: u64
    ) -> (u64, bool) {
        let table_file_map =
            self.context.lock().unwrap().table_file_map.clone();
        let file_path = table_file_map
            .get(&adjusted_ordinal)
            .expect(format!("File path for {} not found", adjusted_ordinal).as_str());

        if let Some(cached_meta) = self.metadata_cache.get(&adjusted_ordinal) {
            let file_fs_meta = metadata(file_path);
            let file_size = file_fs_meta.unwrap().len();

            if cached_meta.size == (file_size as usize) {
                return (adjusted_ordinal, false);
            }
            self.metadata_cache.remove(&adjusted_ordinal);
        }

        let meta = read_metadata(file_path).unwrap();
        self.metadata_cache.insert(adjusted_ordinal, meta);
        (
            adjusted_ordinal,
            true
        )
    }

    pub fn direct_load_table(
        &self,
        adjusted_ordinal: u64,
        batch_index: usize
    ) -> Option<RecordBatch> {
        let table_file_map =
            self.context.lock().unwrap().table_file_map.clone();

        let file_path = table_file_map
            .get(&adjusted_ordinal);

        match file_path {
            Some(path) => {
                let metadata = self.metadata_cache.get(&adjusted_ordinal).unwrap();
                let table = read_batch(path, &metadata, batch_index).unwrap();
                Some(table)
            }
            None => None
        }
    }

    pub fn get_table_for(&self, ordinal: u64) -> Option<Arc<RecordBatch>> {
        let adjusted_ordinal = self.context.lock().unwrap().get_ordinal(ordinal);

        let (bucket_metadata_key, metadata_updated) =
            self.get_metadata_for(adjusted_ordinal);

        let bucket_metadata = self.metadata_cache.get(&bucket_metadata_key).unwrap();

        let (batch_index, _) = get_relative_table_index(ordinal, &bucket_metadata);

        let cache_key = format!("{}-{}", adjusted_ordinal, batch_index);

        if self.table_cache.contains_key(&cache_key) {
            if !metadata_updated {
                let cached_table = self.table_cache.get(&cache_key).unwrap();
                return Some(cached_table);
            }
        } else {
            self.table_cache.remove(&cache_key);
        }

        let table = self.direct_load_table(adjusted_ordinal, batch_index as usize).unwrap();

        self.table_cache.insert(cache_key.clone(), Arc::from(table));

        Some(self.table_cache.get(&cache_key).unwrap())
    }

}
