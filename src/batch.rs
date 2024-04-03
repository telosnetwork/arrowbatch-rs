use std::fs;
use std::io::Error;
use std::sync::Arc;

use arrow::datatypes::DataType;

use num_bigint::BigInt;

use crate::proto::{read_metadata, read_batch};
use crate::utils::{hex_str_to_bigint, string_at_dictionary_column};

use arrow::array::{
    RecordBatch, Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, Int32DictionaryArray
};

use serde::{Serialize, Deserialize};

extern crate hex;
extern crate rlp;
use rlp::{DecoderError, Rlp, RlpStream};

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

#[derive(Serialize, Deserialize, Debug)]
pub struct ArrowBatchRootTable {
    pub name: Option<String>,
    pub ordinal: String,
    pub map: Vec<ArrowTableMapping>
}


#[derive(Serialize, Deserialize, Debug)]
pub struct ArrowBatchContext {
    pub root: ArrowBatchRootTable,
    pub others: std::collections::HashMap<String, Vec<ArrowTableMapping>>,
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


pub fn read_row(record_batch: &RecordBatch, mapping: &Vec<ArrowTableMapping>, row_index: usize) -> Result<Vec<ArrowBatchTypes>, Error> {
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

                _ => return Err(std::io::Error::new(std::io::ErrorKind::Other, "Unsupported array field type")),
            };

            row.push(decoded_array);
        } else {
            row.push(decode_row_value(schema_type, column, field, row_index));
        }
    }

    Ok(row)
}


// #[cfg(test)]
// mod tests {
//     use super::*;
// 
//     #[test]
//     fn test_read_block_table() {
//         let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
//         let data_def_string = fs::read_to_string(defs_path).unwrap();
//         let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");
// 
//         let path = "../telosevm-translator/arrow-data-beta/00000018/block.ab";
//         let metadata = read_metadata(path).unwrap();
//         let mut root_mapping = defs.root.map.clone();
//         root_mapping.insert(0, ArrowTableMapping{
//             name: defs.root.ordinal,
//             data_type: "u64".to_string(),
//             array: None,
//             length: None,
//             optional: None,
//             reference: None
//         });
//         let batch = read_batch(path, &metadata, 0).unwrap();
//         for i in 0..batch.num_rows() {
//             read_row(&batch, &root_mapping, i).unwrap();
//         }
//     }
// 
//     #[test]
//     fn test_read_tx_table() {
//         let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
//         let data_def_string = fs::read_to_string(defs_path).unwrap();
//         let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");
// 
//         let path = "../telosevm-translator/arrow-data-beta/00000018/tx.ab";
//         let metadata = read_metadata(path).unwrap();
//         let batch = read_batch(path, &metadata, 0).unwrap();
//         for i in 0..batch.num_rows() {
//             read_row(&batch, &defs.others.get("tx").unwrap(), i).unwrap();
//         }
//     }
// 
//     #[test]
//     fn test_read_tx_log_table() {
//         let defs_path = "../telosevm-translator/arrow-data-beta/context.json";
//         let data_def_string = fs::read_to_string(defs_path).unwrap();
//         let defs: ArrowBatchContext = serde_json::from_str(&data_def_string).expect("Invalid format");
// 
//         let path = "../telosevm-translator/arrow-data-beta/00000018/tx_log.ab";
//         let metadata = read_metadata(path).unwrap();
//         let batch = read_batch(path, &metadata, 0).unwrap();
//         for i in 0..batch.num_rows() {
//             read_row(&batch, &defs.others.get("tx_log").unwrap(), i).unwrap();
//         }
//     }
// }
