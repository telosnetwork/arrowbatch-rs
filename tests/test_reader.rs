use arrowbatch::{
    proto::ArrowBatchTypes,
    reader::{ArrowBatchConfig, ArrowBatchContext, ArrowBatchReader}
};

#[test]
fn test_reader() {
    let config = ArrowBatchConfig {
        data_dir: "/home/g/repos/telosevm-translator/arrow-data-beta-1".to_string(),
        bucket_size: 10_000_000_u64,
        dump_size: 100_000_u64
    };

    let mut context = ArrowBatchContext::new(config);

    context.reload_on_disk_buckets();

    let mut reader = ArrowBatchReader::new(&context);

    let block = 180840088_u64;
    let row = reader.get_row(block).unwrap();

    assert!(row.refs.len() > 0);

    assert!(row.refs.contains_key("tx"));

    let tx = row.refs.get("tx").unwrap().get(0).unwrap();

    let tx_hash = match tx.row.get(5).unwrap() {
        ArrowBatchTypes::Checksum256(h) => h,
        _ => panic!("expected fifth column to be tx id")
    };

    assert!(
        tx_hash == "23007c552a11452dc58f127491ff413167381ed9ecc068e9c1aca6cbca017028"
    );
}
