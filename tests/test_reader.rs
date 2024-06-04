use arrowbatch::{
    proto::ArrowBatchTypes,
    reader::{ArrowBatchConfig, ArrowBatchContext, ArrowBatchReader}
};

#[test]
fn test_reader_first_tx() {
    let config = ArrowBatchConfig {
        data_dir: "./arrow-data-from-333M".to_string(),
        bucket_size: 10_000_000_u64,
        dump_size: 100_000_u64
    };

    let mut context = ArrowBatchContext::new(config);

    context.reload_on_disk_buckets();

    let mut reader = ArrowBatchReader::new(&context);

    let first_block = 332933058_u64;
    let _first_row = reader.get_row(first_block).unwrap();

    let first_tx_block = 332933060_u64;
    let _first_tx_row = reader.get_row(first_tx_block).unwrap();

    let block_hash = match _first_tx_row.row.get(3).unwrap() {
        ArrowBatchTypes::Checksum256(h) => h,
        _ => panic!("expected third column to be evm block hash")
    };

    assert_eq!(block_hash, "92f43133871dc956ff5b31984b7d996b3e1fc1ea9beb0eb0c074f4242c1a8a25");

    assert!(_first_tx_row.refs.len() > 0);

    assert!(_first_tx_row.refs.contains_key("tx"));

    let txs = _first_tx_row.refs.get("tx").unwrap();

    assert_eq!(txs.len(), 1);

    let tx = txs.get(0).unwrap();

    let tx_hash = match tx.row.get(5).unwrap() {
        ArrowBatchTypes::Checksum256(h) => h,
        _ => panic!("expected fifth column to be tx id")
    };

    assert_eq!(tx_hash, "45b9db19991400ac90260cad8eea660e63b3856ad974e5eac339edd30c96925e");
}


// use chrono::Local;
// #[test]
// fn test_reader_incremental() {
//     let config = ArrowBatchConfig {
//         data_dir: "./arrow-data-from-333M".to_string(),
//         bucket_size: 10_000_000_u64,
//         dump_size: 100_000_u64
//     };
// 
//     let mut context = ArrowBatchContext::new(config);
// 
//     context.reload_on_disk_buckets();
// 
//     let mut reader = ArrowBatchReader::new(&context);
// 
//     let read_size = 300_000_u64;
//     let first_block = 332933058_u64;
//     let last_block = first_block + read_size;
// 
//     for block_num in first_block..last_block {
//         let row = reader.get_row(block_num).unwrap();
//         let evm_hash = match row.row.get(3).unwrap() {
//             ArrowBatchTypes::Checksum256(h) => h,
//             _ => panic!("expected fourth column to be evm hash")
//         };
//         if block_num % 1_000 == 0 {
//             let now = Local::now();
//             println!("{} | {}: {}", now.format("%Y-%m-%dT%H:%M:%S%.3f"), block_num, evm_hash);
//         }
//     }
// }
