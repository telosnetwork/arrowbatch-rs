use std::sync::{Arc, Mutex, MutexGuard};
use std::fs::metadata;

use arrow::record_batch::RecordBatch;
use moka::sync::Cache;

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
    const DEFAULT_TABLE_CACHE: u64 = 3;

    pub fn new(
        context: Arc<Mutex<ArrowBatchContext>>
    ) -> Self {
        ArrowBatchCache {
            context,
            table_cache: Cache::new(Self::DEFAULT_TABLE_CACHE),
            metadata_cache: Cache::new(Self::DEFAULT_TABLE_CACHE),
        }
    }

    pub fn get_metadata_for(
        &self,
        adjusted_ordinal: u64,
        locked_context: Option<Arc<MutexGuard<ArrowBatchContext>>>
    ) -> (u64, bool) {
        let must_lock = locked_context.is_none();
        let context = if must_lock {
            Arc::new(self.context.lock().unwrap())
        } else {
            locked_context.unwrap()
        };

        let file_path = context.table_file_map
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
        if must_lock {
            drop(context);
        }
        (
            adjusted_ordinal,
            true
        )
    }

    pub fn direct_load_table(
        &self,
        adjusted_ordinal: u64,
        batch_index: usize,
        locked_context: Option<Arc<MutexGuard<ArrowBatchContext>>>
    ) -> Option<RecordBatch> {
        let must_lock = locked_context.is_none();
        let context = if must_lock {
            Arc::new(self.context.lock().unwrap())
        } else {
            locked_context.unwrap()
        };

        let table_file_map = context.table_file_map.clone();

        let file_path = table_file_map
            .get(&adjusted_ordinal);

        if must_lock {
            drop(context);
        }

        match file_path {
            Some(path) => {
                let metadata = self.metadata_cache.get(&adjusted_ordinal).unwrap();
                let table = read_batch(path, &metadata, batch_index).unwrap();
                Some(table)
            }
            None => None
        }
    }

    pub fn get_table_for(
        &self,
        ordinal: u64,
        locked_context: Option<Arc<MutexGuard<ArrowBatchContext>>>
    ) -> Option<Arc<RecordBatch>> {
        let must_lock = locked_context.is_none();
        let context = if must_lock {
            Arc::new(self.context.lock().unwrap())
        } else {
            locked_context.unwrap()
        };

        let adjusted_ordinal = context.get_ordinal(ordinal);

        let (bucket_metadata_key, metadata_updated) =
            self.get_metadata_for(adjusted_ordinal, Some(context.clone()));

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

        let table = self.direct_load_table(
            adjusted_ordinal,
            batch_index as usize,
            Some(context.clone())
        ).unwrap();

        self.table_cache.insert(cache_key.clone(), Arc::from(table));

        if must_lock {
            drop(context);
        };

        Some(self.table_cache.get(&cache_key).unwrap())
    }

}
