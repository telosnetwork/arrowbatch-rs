use num::{BigInt, Num};

pub fn hex_str_to_bigint(hex: &str) -> BigInt {
    let hex_str = if hex.starts_with("0x") || hex.starts_with("0X") {
        &hex[2..]
    } else {
        hex
    };

    BigInt::from_str_radix(hex_str, 16).unwrap()
}


use std::sync::Arc;
use arrow::array::{Array, Int32DictionaryArray, StringArray};
use arrow::datatypes::DataType;

pub fn string_at_dictionary_column<'a>(column: &'a Arc<dyn Array>, schema_type: &DataType, row_index: usize) -> &'a str {
    let (key_type, value_type) = match schema_type {
        DataType::Dictionary(key_type, value_type) => (key_type, value_type),
        _ => { panic!("Unknown dictionary type!"); }
    };
    assert!(**key_type == DataType::Int32 && **value_type == DataType::Utf8);

    let dict_array = column.as_any().downcast_ref::<Int32DictionaryArray>().unwrap();
    let keys_array = dict_array.keys();
    let values_array = dict_array.values();
    let values_array = values_array.as_any().downcast_ref::<StringArray>().unwrap();

    let key = keys_array.value(row_index) as usize;
    // Check for null values
    let mut value = "";
    if dict_array.is_valid(row_index) {
        value = values_array.value(key);
    }
    value
}
