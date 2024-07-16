use std::env;

use chrono::Local;
use arrowbatch::{
    proto::ArrowBatchTypes,
    reader::{ArrowBatchConfig, ArrowBatchContext, ArrowBatchSequentialReader}
};

#[test]
fn test_reader_incremental() {
    let config = ArrowBatchConfig {
        data_dir: "/data/arrow-data-full".to_string(),
        bucket_size: 10_000_000_u64,
        dump_size: 100_000_u64
    };

    env_logger::init();

    let context = ArrowBatchContext::new(config);

    context.lock().unwrap().reload_on_disk_buckets();

    let reader = ArrowBatchSequentialReader::new(1, 10_000_000_u64, context);

    for row in reader {
        let block_num = match row[1] {
            ArrowBatchTypes::U64(b) => b,
            _ => panic!("expected second column to be block num")
        };
        let evm_hash = match &row[4] {
            ArrowBatchTypes::Checksum256(h) => h,
            _ => panic!("expected fifth column to be evm hash")
        };
        if block_num % 1_000 == 0 {
            let now = Local::now();
            println!("{} | {}: {}", now.format("%Y-%m-%dT%H:%M:%S%.3f"), block_num, evm_hash);
        }
    }
}
