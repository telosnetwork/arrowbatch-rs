use std::fs;
use std::path::PathBuf;
use std::collections::HashMap;

use crate::proto::{read_metadata, read_batch};

use arrow::array::RecordBatch;
use arrow::error::ArrowError;

use serde::{Serialize, Deserialize};
use crate::proto::{ArrowTableMapping, ArrowBatchTypes, read_row};


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchRootTable {
    pub name: Option<String>,
    pub ordinal: String,
    pub map: Vec<ArrowTableMapping>
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchContextDef {
    pub root: ArrowBatchRootTable,
    pub others: HashMap<String, Vec<ArrowTableMapping>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchConfig {
    pub data_dir: String,
    pub bucket_size: u64,
    pub dump_size: u64
}

pub const DEFAULT_TABLE_CACHE_SIZE: usize = 10;

pub struct ArrowBatchReader {
    pub config: ArrowBatchConfig,
    pub table_cache: HashMap<String, RecordBatch>,

    pub data_definition: ArrowBatchContextDef,

    pub table_file_map: HashMap<u64, HashMap<String, String>>,
    pub table_mappings: HashMap<String, Vec<ArrowTableMapping>>
}

impl ArrowBatchReader {

    pub fn new(
        config: &ArrowBatchConfig
    ) -> Self {
        let table_cache = HashMap::new();
        let table_file_map = HashMap::new();

        let data_def_string = fs::read_to_string(
            PathBuf::from(&config.data_dir).join("context.json")).unwrap();

        let defs: ArrowBatchContextDef = serde_json::from_str(
            &data_def_string).expect("Invalid format");

        let mut table_mappings = defs.others.clone();
        let mut root_mappings = defs.root.map.clone();

        root_mappings.insert(
            0,
            ArrowTableMapping {
                name: defs.root.ordinal.clone(), data_type: "u64".to_string(),
                optional: None, length: None, array: None, reference: None
            }
        );

        table_mappings.insert("root".to_string(), root_mappings);

        ArrowBatchReader {
            config: config.clone(),
            table_cache,

            data_definition: defs.clone(),

            table_file_map,
            table_mappings
        }
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
        let bucket_full_path = PathBuf::from(&self.config.data_dir).join(bucket);
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

        let mut table_files_map = HashMap::new();
        for (table_name, _) in self.table_mappings.iter() {
            let mut name = table_name.clone();
            if name == "root" && self.data_definition.root.name.is_some() {
                name = self.data_definition.root.name.clone().unwrap();
            }

            let file = table_files.iter().find(|file| file.as_str() == format!("{}.ab", name));
            if let Some(file) = file {
                table_files_map.insert(
                    table_name.clone(),
                    bucket_full_path.join(file).to_str().unwrap().to_string(),
                );
            }
        }
        if let Some(ordinal) = self.bucket_to_ordinal(&bucket.to_string()) {
            self.table_file_map.insert(ordinal, table_files_map);
        }
    }

    pub fn reload_on_disk_buckets(&mut self) {
        self.table_file_map.clear();
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
    }

    pub fn get_root_row(&mut self, ordinal: u64) -> Result<Vec<ArrowBatchTypes>, ArrowError> {
        let adjusted_ordinal = self.get_ordinal(ordinal);

        let table_map = self.table_file_map.get(&adjusted_ordinal).expect("Could not fetch row");
        let table_path = table_map.get("root").expect("Could not fetch row");

        let metadata = read_metadata(table_path.as_str())?;
        let first_table = read_batch(table_path.as_str(), &metadata, 0)?;

        let mapping = self.table_mappings.get("root").unwrap();
        let first_row = read_row(&first_table, &mapping, 0)?;

        let disk_start_ordinal = match first_row[0] {
            ArrowBatchTypes::U64(value) => value,
            _ => panic!("Root oridnal field is not u64!?")
        };
        let relative_index = ordinal - disk_start_ordinal;
        let batch_index = relative_index / self.config.dump_size;
        let table_index = relative_index % self.config.dump_size;

        let cache_key = format!("{}-{}", adjusted_ordinal, batch_index);

        let table = if !self.table_cache.contains_key(&cache_key) {
            let new_table = if batch_index != 0 {
                read_batch(table_path.as_str(), &metadata, table_index as usize)?
            } else {
                first_table.clone()
            };

            // Maybe trim cache if necessary
            if self.table_cache.len() > DEFAULT_TABLE_CACHE_SIZE {
                let oldest_key = self.table_cache.keys().next().unwrap().clone();
                self.table_cache.remove(&oldest_key);
            }

            self.table_cache.insert(cache_key.clone(), new_table.clone());
            new_table
        } else {
            self.table_cache.get(&cache_key).unwrap().clone()
        };

        return read_row(&table, &mapping, batch_index as usize);
    }
}
