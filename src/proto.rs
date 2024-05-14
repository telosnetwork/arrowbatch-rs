use std::fs::File;
use std::sync::Arc;
use std::io::{self, Read, Seek, SeekFrom};
use std::convert::TryInto;

use arrow::ipc::reader::FileReader;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::array::{
    Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    Int64Array
};

use zstd::stream::decode_all;

use arrow::datatypes::DataType;

use num_bigint::BigInt;

use crate::utils::{hex_str_to_bigint, string_at_dictionary_column};

extern crate hex;
extern crate rlp;
use rlp::Rlp;

extern crate base64;

use serde::{Serialize, Deserialize};


#[derive(Debug, PartialEq)]
pub enum ArrowBatchCompression {
    Uncompressed = 0,
    Zstd = 1,
}

#[derive(Debug)]
pub struct ArrowBatchGlobalHeader {
    pub version_constant: String,
}

#[derive(Debug)]
pub struct ArrowBatchHeader {
    pub header_constant: String,
    pub batch_byte_size: usize,
    pub compression: ArrowBatchCompression,
}

#[derive(Debug)]
pub struct ArrowBatchMetadata {
    pub header: ArrowBatchHeader,
    pub start: usize,
    pub end: usize
}

#[derive(Debug)]
pub struct ArrowBatchFileMetadata {
    pub size: usize,
    pub header: ArrowBatchGlobalHeader,
    pub batches: Vec<ArrowBatchMetadata>,
}


pub const ARROW_BATCH_VERSION_CONSTANT: &'static str = "ARROW-BATCH1";
pub const GLOBAL_HEADER_SIZE: usize = ARROW_BATCH_VERSION_CONSTANT.len();
pub const ARROW_BATCH_HEADER_CONSTANT: &'static str = "ARROW-BATCH-TABLE";
pub const BATCH_HEADER_SIZE: usize = ARROW_BATCH_HEADER_CONSTANT.len() + 8 + 1;


pub fn new_global_header() -> Vec<u8> {
    ARROW_BATCH_VERSION_CONSTANT.as_bytes().to_vec()
}

pub fn new_batch_header(byte_size: u64, compression: ArrowBatchCompression) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(ARROW_BATCH_HEADER_CONSTANT.as_bytes());
    buffer.extend_from_slice(&byte_size.to_le_bytes());
    buffer.push(compression as u8);
    buffer
}

pub fn read_global_header(buffer: &[u8]) -> ArrowBatchGlobalHeader {
    let version_constant = std::str::from_utf8(&buffer[0..GLOBAL_HEADER_SIZE]).unwrap_or_default().to_string();
    ArrowBatchGlobalHeader { version_constant }
}

pub fn read_batch_header(buffer: &[u8]) -> ArrowBatchHeader {
    let header_constant = std::str::from_utf8(&buffer[0..ARROW_BATCH_HEADER_CONSTANT.len()]).unwrap_or_default().to_string();
    let size_start = ARROW_BATCH_HEADER_CONSTANT.len();
    let batch_byte_size = usize::from_le_bytes(buffer[size_start..size_start+8].try_into().unwrap());
    let compression = match buffer[size_start + 8] {
        0 => ArrowBatchCompression::Uncompressed,
        1 => ArrowBatchCompression::Zstd,
        _ => panic!("Invalid compression type"),
    };
    ArrowBatchHeader { header_constant, batch_byte_size, compression }
}

pub fn read_metadata(file_path: &str) -> io::Result<ArrowBatchFileMetadata> {
    let mut file = File::open(file_path)?;
    let mut buffer = vec![0; GLOBAL_HEADER_SIZE];

    // Read the global header
    file.read_exact(&mut buffer)?;
    let global_header = read_global_header(&buffer);

    // Prepare to read batch headers and their metadata
    let mut batches = Vec::new();
    let mut offset = GLOBAL_HEADER_SIZE;

    while offset < file.metadata()?.len() as usize {
        // Read batch header
        let mut buffer = vec![0; BATCH_HEADER_SIZE];
        file.seek(SeekFrom::Start(offset as u64))?;
        file.read_exact(&mut buffer)?;
        let header = read_batch_header(&buffer);

        // Calculate the start and end of the current batch's data
        let start = offset + BATCH_HEADER_SIZE;
        let end = start + header.batch_byte_size - 1;
        batches.push(ArrowBatchMetadata{header, start, end});

        // Update offset for next iteration
        offset = end + 1;
    }

    Ok(ArrowBatchFileMetadata {
        size: offset,
        header: global_header,
        batches,
    })
}

pub fn read_batch(file_path: &str, metadata: &ArrowBatchFileMetadata, batch_index: usize) -> Result<RecordBatch, ArrowError> {
    let batch_meta = &metadata.batches[batch_index];
    let mut file = File::open(file_path)?;
    let mut buffer = vec![0u8; (batch_meta.end - batch_meta.start + 1) as usize];

    // Move to the start of the batch data and read it into the buffer
    file.seek(io::SeekFrom::Start(batch_meta.start as u64))?;
    file.read_exact(&mut buffer)?;

    // Decompress if necessary
    let decompressed_data = match batch_meta.header.compression {
        ArrowBatchCompression::Uncompressed => buffer,
        ArrowBatchCompression::Zstd => decode_all(&buffer[..])?,
    };

    // Here we assume the decompressed data is directly an Arrow IPC file format
    // This part would be adjusted based on actual data format and requirements
    let cursor = io::Cursor::new(decompressed_data);
    let mut reader = FileReader::try_new(cursor, None).expect("Failed to read Arrow file");
    reader.next().unwrap()
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrowReference {
    pub table: String,
    pub field: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArrowTableMapping {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub optional: Option<bool>,
    pub length: Option<usize>,
    pub array: Option<bool>,
    #[serde(rename = "ref")]
    pub reference: Option<ArrowReference>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ArrowBatchTypes {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    UVar(BigInt),

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
    let decoded: ArrowBatchTypes;
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
            // assert!(bytes_val.len() == 32);
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
