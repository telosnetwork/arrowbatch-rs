use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::convert::TryInto;

use arrow::ipc::reader::FileReader;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;

use zstd::stream::decode_all;


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
