use std::fs;
use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashMap;

use arrow::datatypes::DataType;

use num_bigint::BigInt;

use crate::proto::{read_metadata, read_batch};
use crate::utils::{hex_str_to_bigint, string_at_dictionary_column};

use arrow::array::{
    RecordBatch, Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    Int8Array, Int16Array, Int32Array, Int64Array
};
use arrow::error::ArrowError;

use serde::{Serialize, Deserialize};

extern crate hex;
extern crate rlp;
use rlp::Rlp;

extern crate base64;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowReference {
    pub table: String,
    pub field: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowTableMapping {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub optional: Option<bool>,
    pub length: Option<usize>,
    pub array: Option<bool>,
    pub reference: Option<ArrowReference>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchRootTable {
    pub name: Option<String>,
    pub ordinal: String,
    pub map: Vec<ArrowTableMapping>
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrowBatchContext {
    pub root: ArrowBatchRootTable,
    pub others: HashMap<String, Vec<ArrowTableMapping>>,
}

#[derive(Debug)]
pub enum ArrowBatchTypes {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    UVar(BigInt),

    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),

    Bytes(Vec<u8>),
    Str(String),

    Checksum160(String),
    Checksum256(String),

    U16Array(Vec<u16>),
    U32Array(Vec<u32>),
    U64Array(Vec<u64>),

    BytesArray(Vec<Vec<u8>>),
    StrArray(Vec<String>),
}

fn decode_row_value(schema_type: &DataType, column: &Arc<dyn Array>, field: &ArrowTableMapping, row_index: usize) -> ArrowBatchTypes {
    let mut decoded: ArrowBatchTypes;
    match field.data_type.as_str() {
        "u8" => {
            assert!(schema_type == &DataType::UInt8);
            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            decoded = ArrowBatchTypes::U8(array.value(row_index));
        },
        "u16" => {
            assert!(schema_type == &DataType::UInt16);
            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            decoded = ArrowBatchTypes::U16(array.value(row_index));
        },
        "u32" => {
            assert!(schema_type == &DataType::UInt32, "Expected UInt32, found {:?} for column {}", schema_type, field.name);
            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            decoded = ArrowBatchTypes::U32(array.value(row_index));
        },
        "u64" => {
            assert!(schema_type == &DataType::UInt64);
            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            decoded = ArrowBatchTypes::U64(array.value(row_index));
        },
        "uintvar" => {
            let base64_val = string_at_dictionary_column(column, schema_type, row_index);
            let bytes_val = base64::decode(base64_val).unwrap();
            let hex_val = hex::encode(bytes_val);
            decoded = ArrowBatchTypes::UVar(hex_str_to_bigint(&hex_val));
        },
        "i8" => {
            assert!(schema_type == &DataType::Int8);
            let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
            decoded = ArrowBatchTypes::I8(array.value(row_index));
        },
        "i16" => {
            assert!(schema_type == &DataType::Int16);
            let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
            decoded = ArrowBatchTypes::I16(array.value(row_index));
        },
        "i32" => {
            assert!(schema_type == &DataType::Int32);
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            decoded = ArrowBatchTypes::I32(array.value(row_index));
        },
        "i64" => {
            assert!(schema_type == &DataType::Int64);
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            decoded = ArrowBatchTypes::I64(array.value(row_index));
        },
        "bytes" => {
            let base64_val = string_at_dictionary_column(column, schema_type, row_index);
            let val = base64::decode(base64_val).unwrap();
            decoded = ArrowBatchTypes::Bytes(val);
        },
        "string" => {
            let val = string_at_dictionary_column(column, schema_type, row_index);
            decoded = ArrowBatchTypes::Str(val.to_string());
        },
        "checksum160" => {
            let base64_val = string_at_dictionary_column(column, schema_type, row_index);
            let bytes_val = base64::decode(base64_val).unwrap();
            assert!(bytes_val.len() == 20);
            let hex_val = hex::encode(bytes_val);
            decoded = ArrowBatchTypes::Checksum160(hex_val);
        },
        "checksum256" => {
            let base64_val = string_at_dictionary_column(column, schema_type, row_index);
            let bytes_val = base64::decode(base64_val).unwrap();
            assert!(bytes_val.len() == 32);
            let hex_val = hex::encode(bytes_val);
            decoded = ArrowBatchTypes::Checksum256(hex_val);
        },
        _ => {
            panic!("Unsupported field {}", field.data_type);
        }
    }

    decoded
}

fn decode_rlp_array<T, F>(bytes_array: Vec<u8>, decoder: F) -> Vec<T>
where
    F: Fn(&Rlp, usize) -> Result<T, rlp::DecoderError>,
{
    let rlp = Rlp::new(&bytes_array);
    assert!(rlp.is_list());
    (0..rlp.item_count().unwrap())
        .map(|i| decoder(&rlp, i).unwrap())
        .collect()
}


pub fn read_row(record_batch: &RecordBatch, mapping: &Vec<ArrowTableMapping>, row_index: usize) -> Result<Vec<ArrowBatchTypes>, ArrowError> {
    let schema = record_batch.schema();
    let mut row = Vec::new();

    for field in mapping.iter() {
        let column = record_batch.column_by_name(field.name.as_str()).unwrap();
        let schema_field = schema.field_with_name(field.name.as_str()).unwrap();
        let schema_type = schema_field.data_type();

        if let Some(true) = field.array {
            let base64_array = string_at_dictionary_column(column, schema_type, row_index);
            let bytes_array = base64::decode(base64_array).unwrap();

            let decoded_array = match field.data_type.as_str() {
                "u8" => ArrowBatchTypes::Bytes(
                    decode_rlp_array(bytes_array, |rlp, i| rlp.val_at(i))),

                "u16" => ArrowBatchTypes::U16Array(
                    decode_rlp_array(bytes_array, |rlp, i| rlp.val_at(i))),

                "u32" => ArrowBatchTypes::U32Array(
                    decode_rlp_array(bytes_array, |rlp, i| rlp.val_at(i))),

                "u64" => ArrowBatchTypes::U64Array(
                    decode_rlp_array(bytes_array, |rlp, i| rlp.val_at(i))),

                "bytes" => ArrowBatchTypes::BytesArray(
                    decode_rlp_array(bytes_array, |rlp, i| {
                        let b64_bytes = rlp.val_at::<Vec<u8>>(i).unwrap_or_default();
                        let bytes = base64::decode(String::from_utf8(b64_bytes).unwrap()).unwrap();
                        Ok(bytes)
                    })),

                "string" => ArrowBatchTypes::StrArray(
                    decode_rlp_array(bytes_array, |rlp, i| {
                        let bytes = rlp.val_at::<Vec<u8>>(i).unwrap_or_default();
                        Ok(String::from_utf8(bytes).unwrap_or_default())
                    })),

                _ => return Err(ArrowError::ParseError("Unsupported field type".to_string())),
            };

            row.push(decoded_array);
        } else {
            row.push(decode_row_value(schema_type, column, field, row_index));
        }
    }

    Ok(row)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ArrowBatchConfig {
    data_dir: String,
    bucket_size: u64,
    dump_size: u64
}

pub const DEFAULT_TABLE_CACHE_SIZE: usize = 10;

struct ArrowBatchReader {
    config: ArrowBatchConfig,
    table_cache: HashMap<String, RecordBatch>,

    data_definition: ArrowBatchContext,

    table_file_map: HashMap<u64, HashMap<String, String>>,
    table_mappings: HashMap<String, Vec<ArrowTableMapping>>
}

impl ArrowBatchReader {

    pub fn new(
        config: &ArrowBatchConfig,
        defs: &ArrowBatchContext
    ) -> Self {
        let table_cache = HashMap::new();
        let table_file_map = HashMap::new();
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

    fn get_ordinal(&self, ordinal: u64) -> u64 {
        ordinal / self.config.bucket_size
    }

    fn get_ordinal_suffix(&self, ordinal: u64) -> String {
        format!("{:0>8}", self.get_ordinal(ordinal))
    }

    fn bucket_to_ordinal(&self, table_bucket_name: &String) -> Option<u64> {
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

    fn reload_on_disk_buckets(&mut self) {
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

    fn get_root_row(&mut self, ordinal: u64) -> Result<Vec<ArrowBatchTypes>, ArrowError> {
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_block_table() {
        let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
        let data_def_string = fs::read_to_string(defs_path).unwrap();
        let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");

        let path = "../telosevm-translator/arrow-data-beta/00000018/block.ab";
        let metadata = read_metadata(path).unwrap();
        let mut root_mapping = defs.root.map.clone();
        root_mapping.insert(0, ArrowTableMapping{
            name: defs.root.ordinal,
            data_type: "u64".to_string(),
            array: None,
            length: None,
            optional: None,
            reference: None
        });
        let batch = read_batch(path, &metadata, 0).unwrap();
        for i in 0..batch.num_rows() {
            read_row(&batch, &root_mapping, i).unwrap();
        }
    }

    #[test]
    fn test_read_tx_table() {
        let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
        let data_def_string = fs::read_to_string(defs_path).unwrap();
        let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");

        let path = "../telosevm-translator/arrow-data-beta/00000018/tx.ab";
        let metadata = read_metadata(path).unwrap();
        let batch = read_batch(path, &metadata, 0).unwrap();
        for i in 0..batch.num_rows() {
            read_row(&batch, &defs.others.get("tx").unwrap(), i).unwrap();
        }
    }

    #[test]
    fn test_read_tx_log_table() {
        let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
        let data_def_string = fs::read_to_string(defs_path).unwrap();
        let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");

        let path = "../telosevm-translator/arrow-data-beta/00000018/tx_log.ab";
        let metadata = read_metadata(path).unwrap();
        let batch = read_batch(path, &metadata, 0).unwrap();
        for i in 0..batch.num_rows() {
            read_row(&batch, &defs.others.get("tx_log").unwrap(), i).unwrap();
        }
    }

    #[test]
    fn test_reader() {
        let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
        let data_def_string = fs::read_to_string(defs_path).unwrap();
        let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");
        let mut reader = ArrowBatchReader::new(
            &ArrowBatchConfig {
                data_dir: "../telosevm-translator/arrow-data-beta".to_string(),
                bucket_size: 10_000_000, dump_size: 100_000
            },
            &defs
        );

        reader.reload_on_disk_buckets();

        let row = reader.get_root_row(180_698_860);
        println!("{:#?}", row);
    }
}
