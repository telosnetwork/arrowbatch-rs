use std::fs;
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use log::debug;
use serde::{Serialize, Deserialize};

use crate::proto::{
    read_batch, read_metadata, read_row, ArrowBatchFileMetadata, ArrowBatchTypes, ArrowTableMapping
};

use crate::cache::ArrowBatchCache;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchContextDef {
    pub alias: Option<String>,
    pub ordinal: String,
    pub stream_size: Option<String>,
    pub map: Vec<ArrowTableMapping>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchConfig {
    pub data_dir: String,
    pub bucket_size: u64,
    pub dump_size: u64
}

pub type ArrowRow = Vec<ArrowBatchTypes>;

pub const DEFAULT_TABLE_CACHE_SIZE: usize = 10;
pub const DEFAULT_ALIAS: &str = "table";

pub type TableFileMap = HashMap<u64, String>;
pub type TableMapping = Vec<ArrowTableMapping>;

pub struct ArrowBatchContext {
    pub config: ArrowBatchConfig,
    pub data_definition: ArrowBatchContextDef,
    pub alias: String,
    pub ordinal_index: usize,

    pub table_file_map: TableFileMap,
    pub table_mapping: TableMapping,

    pub wip_file: Option<String>,

    pub first_ordinal: Option<u64>,
    pub last_ordinal: Option<u64>,
}

pub struct ArrowBatchIterContext {
    pub current_block: u64,

    pub current_table: Option<Arc<RecordBatch>>,
    pub current_meta: Option<Arc<ArrowBatchFileMetadata>>
}

pub fn get_relative_table_index(ordinal: u64, meta: &ArrowBatchFileMetadata) -> (u64, u64) {
    let bucket_start = meta.batches[0].header.start_ordinal;
    let bucket_end = meta.batches[meta.batches.len() - 1].header.last_ordinal;

    if ordinal < bucket_start || ordinal > bucket_end {
        panic!("Ordinal {} is not in bucket range ({}-{}).", ordinal, bucket_start, bucket_end);
    }

    let mut batch_index: u64 = 0;
    while ordinal > meta.batches[batch_index as usize].header.last_ordinal {
        batch_index += 1;
    }

    (batch_index, ordinal - meta.batches[batch_index as usize].header.start_ordinal)
}

impl ArrowBatchContext {
    pub fn new(
        config: ArrowBatchConfig
    ) -> Arc<Mutex<Self>> {
        let table_file_map = HashMap::new();

        let data_def_string = fs::read_to_string(
            PathBuf::from(&config.data_dir).join("context.json")).unwrap();

        let defs: ArrowBatchContextDef = serde_json::from_str(
            &data_def_string).expect("Invalid format");

        let alias = match defs.alias.clone() {
            Some(a) => a,
            None => DEFAULT_ALIAS.to_string()
        };

        let ordinal_index = match defs.map.iter().position(|m| m.name == defs.ordinal) {
            Some(i) => i,
            None => panic!("Could not find ordinal mapping in definition")
        };

        let mut new_context = ArrowBatchContext {
            config,
            data_definition: defs.clone(),
            alias,
            ordinal_index,

            table_file_map,
            table_mapping: defs.map.clone(),
            wip_file: None,
            first_ordinal: None, last_ordinal: None
        };
        new_context.reload_on_disk_buckets();
        let context_arc = Arc::new(Mutex::new(new_context));

        context_arc
    }

    pub fn get_ordinal(&self, ordinal: u64) -> u64 {
        ordinal / self.config.bucket_size
    }

    pub fn get_ordinal_suffix(&self, ordinal: u64) -> String {
        format!("{:0>8}", self.get_ordinal(ordinal))
    }

    pub fn bucket_to_ordinal(&self, table_bucket_name: &String) -> Option<u64> {
        let mut bucket_name = table_bucket_name.clone();
        if bucket_name.contains(".wip") {
            bucket_name = bucket_name.replace(".wip", "");
        }

        bucket_name
            .chars()
            .filter(|c| c.is_digit(10))
            .collect::<String>()
            .parse::<u64>()
            .ok()
    }

    fn load_table_file_map(&mut self, bucket: &str) {
        let bucket_full_path = PathBuf::from(
            &self.config.data_dir).join(bucket);

        let table_files = fs::read_dir(&bucket_full_path)
            .unwrap_or_else(|_| panic!("Failed to read directory {:?}", bucket_full_path))
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_file() && path.extension().unwrap_or_default() == "ab" {
                    Some(entry.file_name().into_string().unwrap())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();


        let maybe_table_file = table_files.iter().find(|file| file.as_str() == format!("{}.ab", self.alias));
        let maybe_wip_file = table_files.iter().find(|file| file.as_str() == format!("{}.ab.wip", self.alias));

        if let Some(wip_file) = maybe_wip_file {
            self.wip_file = Some(bucket_full_path.join(wip_file).to_str().unwrap().to_string());
        }
        if let Some(ordinal) = self.bucket_to_ordinal(&bucket.to_string()) {
            if let Some(table_file) = maybe_table_file {
                self.table_file_map.insert(
                    ordinal,
                    bucket_full_path.join(table_file.clone()).to_str().unwrap().to_string()
                );
            }
        }
    }

    pub fn reload_on_disk_buckets(&mut self) {
        self.table_file_map.clear();
        self.wip_file = None;

        let buckets = fs::read_dir(&self.config.data_dir)
            .unwrap_or_else(|_| panic!("Failed to read directory {}", &self.config.data_dir))
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_dir() {
                    Some(entry.file_name().into_string().unwrap())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        let sorted_buckets = {
            let mut buckets_with_ord = buckets.iter().filter_map(|&ref bucket| {
                self.bucket_to_ordinal(bucket).map(|ord| (ord, bucket.clone()))
            }).collect::<Vec<(u64, String)>>();

            buckets_with_ord.sort_by(|a, b| a.0.cmp(&b.0));
            buckets_with_ord.into_iter().map(|(_, b)| b).collect::<Vec<String>>()
        };

        for bucket in sorted_buckets.iter() {
            self.load_table_file_map(bucket);
        }

        let last_bucket = self.table_file_map.keys().max();
        let first_bucket = self.table_file_map.keys().min();

        match (first_bucket, last_bucket) {
            (Some(first), Some(last)) => {
                let first_table = self.table_file_map.get(first).unwrap();
                let first_meta = read_metadata(&first_table).unwrap();
                self.first_ordinal = Some(first_meta.batches.get(0).unwrap().header.start_ordinal);

                let last_table = self.table_file_map.get(last).unwrap();
                let last_meta = read_metadata(&last_table).unwrap();
                self.last_ordinal = Some(last_meta.batches.last().unwrap().header.last_ordinal);
            },
            _ => ()
        }
    }
}

pub struct ArrowBatchReader {
    pub context: Arc<Mutex<ArrowBatchContext>>,
    pub cache: ArrowBatchCache,
}

impl ArrowBatchReader {

    pub fn new(
        context: Arc<Mutex<ArrowBatchContext>>
    ) -> Self {
        ArrowBatchReader {
            context: context.clone(),
            cache: ArrowBatchCache::new(context),
        }
    }

    pub fn get_row(&self, ordinal: u64) -> Option<ArrowRow> {
        let context = Arc::new(self.context.lock().unwrap());
        let adjusted_ordinal = context.get_ordinal(ordinal);
        let table = match self.cache.get_table_for(ordinal, Some(context.clone())) {
            Some(t) => t,
            None => return None
        };
        let meta = self.cache.metadata_cache.get(&adjusted_ordinal).unwrap();

        let (_, relative_index) = get_relative_table_index(ordinal, &meta);
        let row = read_row(&table, &context.table_mapping, relative_index as usize).unwrap();
        drop(context);

        Some(row)
    }
}

pub struct ArrowBatchSequentialReader {
    pub context: Arc<Mutex<ArrowBatchContext>>,
    pub iter_ctx: Arc<Mutex<ArrowBatchIterContext>>,
    pub start_ordinal: u64,
    pub stop_ordinal: u64,
}

impl ArrowBatchSequentialReader {

    pub fn new(
        from: u64, to: u64,
        context: Arc<Mutex<ArrowBatchContext>>
    ) -> Self {
        let iter_ctx = Arc::new(Mutex::new(ArrowBatchIterContext{
            current_block: from - 1,
            current_meta: None,
            current_table: None
        }));
        ArrowBatchSequentialReader {
            context: context.clone(),
            iter_ctx,
            start_ordinal: from,
            stop_ordinal: to,
        }
    }

    fn ensure_tables(&self) {
        let context = Arc::new(self.context.lock().unwrap());
        let mut iter_ctx = self.iter_ctx.lock().unwrap();

        let last_adjusted_ordinal = context.get_ordinal(iter_ctx.current_block);
        let next_block = iter_ctx.current_block + 1;
        let new_adjusted_ordinal = context.get_ordinal(next_block);

        let bucket_metadata = match iter_ctx.current_meta.clone() {
            Some(val) => val,
            None => {
                let file_path = context.table_file_map
                    .get(&new_adjusted_ordinal)
                    .expect(&format!("File path for {} not found", new_adjusted_ordinal));

                let meta = Arc::new(read_metadata(file_path).unwrap());

                iter_ctx.current_meta = Some(meta.clone());

                meta
            }
        };

        let (last_batch_index, _) = get_relative_table_index(iter_ctx.current_block, &bucket_metadata);
        let (new_batch_index, _) = get_relative_table_index(next_block, &bucket_metadata);

        let must_update = last_adjusted_ordinal != new_adjusted_ordinal || last_batch_index != new_batch_index;

        if iter_ctx.current_table.is_none() || must_update {
            let bucket_metadata = iter_ctx.current_meta.clone().unwrap();

            let (batch_index, _) = get_relative_table_index(next_block, &bucket_metadata);

            let file_path = context.table_file_map
                .get(&new_adjusted_ordinal)
                .expect(&format!("File path for {} not found", new_adjusted_ordinal));

            let table = read_batch(
                file_path,
                &bucket_metadata,
                batch_index as usize
            ).unwrap();

            iter_ctx.current_table = Some(Arc::new(table));
        }
    }

    pub fn imut_next(&self) -> Option<ArrowRow> {
        let mut iter_ctx = self.iter_ctx.lock().unwrap();
        if iter_ctx.current_block > self.stop_ordinal {
            return None;
        }
        drop(iter_ctx);
        self.ensure_tables();
        iter_ctx = self.iter_ctx.lock().unwrap();
        iter_ctx.current_block += 1;
        let context = Arc::new(self.context.lock().unwrap());

        let meta = iter_ctx.current_meta.clone().unwrap();
        let table = iter_ctx.current_table.clone().unwrap();

        let (_, relative_index) = get_relative_table_index(iter_ctx.current_block, &meta);
        let row = read_row(&table, &context.table_mapping, relative_index as usize).unwrap();
        drop(context);
        drop(iter_ctx);

        Some(row)
    }
}

impl Iterator for ArrowBatchSequentialReader {
    type Item = ArrowRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.imut_next()
    }
}
