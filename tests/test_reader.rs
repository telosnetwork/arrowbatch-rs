use arrowbatch::{
    proto::ArrowBatchTypes,
    reader::{ArrowBatchConfig, ArrowBatchContext, ArrowBatchReader, ArrowBatchSequentialReader}
};

#[test]
fn test_reader_first_tx() {
    let config = ArrowBatchConfig {
        data_dir: "/data/arrow-data-full".to_string(),
        bucket_size: 10_000_000_u64,
        dump_size: 100_000_u64
    };

    let context = ArrowBatchContext::new(config);

    context.lock().unwrap().reload_on_disk_buckets();

    let reader = ArrowBatchReader::new(context);

    let block = 181885080_u64;
    let _row = reader.get_row(block).unwrap();

    println!("{:#?}", _row);
}
