use std::fs;
use std::path::PathBuf;
use std::collections::{HashMap, HashSet};

use serde::{Serialize, Deserialize};

use crate::proto::{
    read_row, ArrowBatchTypes, ArrowTableMapping
};

use crate::cache::{ArrowBatchCache, ArrowCachedTables};


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefInfo {
    pub parent_index: usize,
    pub parent_mapping: ArrowTableMapping,
    pub child_index: usize,
    pub child_mapping: ArrowTableMapping
}

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

#[derive(Debug, Clone)]
pub struct RowWithRefs {
    pub row: Vec<ArrowBatchTypes>,
    pub refs: HashMap<String, Vec<RowWithRefs>>
}

pub const DEFAULT_TABLE_CACHE_SIZE: usize = 10;

pub type TableFileMap = HashMap<u64, HashMap<String, String>>;
pub type TableMappings = HashMap<String, Vec<ArrowTableMapping>>;
pub type ReferenceMappings = HashMap<String, HashMap<String, RefInfo>>;

pub struct ArrowBatchContext {
    pub config: ArrowBatchConfig,
    pub data_definition: ArrowBatchContextDef,

    pub table_file_map: TableFileMap,
    pub table_mappings: TableMappings,
    pub ref_mappings: ReferenceMappings
}

fn get_table_mapping<'a>(table_name: &String, table_mappings: &'a TableMappings) -> &'a Vec<ArrowTableMapping> {
    match table_mappings.get(table_name) {
        Some(mapping) => return mapping,
        None => panic!("No mapping for table \"{}\"", table_name)
    }
}

fn gen_mappings_for_table(table_name: &String, table_mappings: &TableMappings) -> HashMap<String, RefInfo> {
    let mut ref_mappings = HashMap::new();

    let table_mapping = get_table_mapping(table_name, table_mappings);

    for (ref_name, ref_mapping) in table_mappings.iter() {
        if let Some(child_index) = ref_mapping.iter()
            .position(|field| field.reference.as_ref().is_some_and(|r| r.table == *table_name)) {

            let child_mapping = ref_mapping[child_index].clone();
            let parent_index = table_mapping.iter()
                .position(|m| m.name == child_mapping.reference.as_ref().unwrap().field).unwrap();

            let parent_mapping = table_mapping[parent_index].clone();

            ref_mappings.insert(ref_name.clone(), RefInfo {
                child_index,
                child_mapping,
                parent_index,
                parent_mapping
            });
        }
    }

    ref_mappings
}

impl ArrowBatchContext {
    pub fn new(
        config: ArrowBatchConfig
    ) -> Self {
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

        let mut ref_mappings = HashMap::new();

        for (table_name, _table_mapping) in table_mappings.iter() {
            ref_mappings.insert(
                table_name.clone(),
                gen_mappings_for_table(table_name, &table_mappings)
            );
        }

        ArrowBatchContext {
            config,
            data_definition: defs,

            table_file_map,
            table_mappings,
            ref_mappings
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
        let bucket_full_path = PathBuf::from(
            &self.config.data_dir).join(bucket);

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
}

pub struct ArrowBatchReader<'a> {
    context: &'a ArrowBatchContext,
    cache: ArrowBatchCache<'a>,
}

fn get_rows_by_ref(
    context: &ArrowBatchContext,
    referenced_table: &String,
    referenced_field: &ArrowTableMapping,
    reference: &ArrowBatchTypes,
    tables: &ArrowCachedTables
) -> HashMap<String, Vec<Vec<ArrowBatchTypes>>> {

    println!("get_rows_by_ref {} on {}", referenced_table, referenced_field.name);

    let root_mapping = get_table_mapping(referenced_table, &context.table_mappings);
    match root_mapping.iter()
        .position(|m| m.name == referenced_field.name) {
        Some(_) => (),
        None => panic!("reference not found on table!")
    };

    let references = context.ref_mappings.get(referenced_table).unwrap();

    let mut refs = HashMap::new();

    for (table_name, table) in tables.others.iter() {

        if (table_name == referenced_table) ||
            !(references.contains_key(table_name)) ||
            (references.get(table_name)
                .is_some_and(
                    |r| r.child_mapping.reference.as_ref().unwrap().field != referenced_field.name))
        {
            continue;
        }

        let mut rows = Vec::new();

        let mappings = get_table_mapping(table_name, &context.table_mappings);

        let ref_field_idx = mappings.iter()
            .position(|m| m.reference.as_ref().is_some_and(
                |r| r.table == *referenced_table &&
                    r.field == referenced_field.name)).unwrap();

        let mut start_idx: i64 = -1;
        for i in 0..table.num_rows() {
            let row = read_row(table, mappings, i).unwrap();

            if row[ref_field_idx] == *reference {
                rows.push(row);

                if start_idx == -1 {
                    start_idx = i as i64;
                }
            } else if start_idx != -1 {
                break;
            }
        }

        println!("found {} refs to {} on {}", rows.len(), referenced_table, table_name);

        refs.insert(table_name.clone(), rows);
    }

    refs
}

fn gen_refs(
    context: &ArrowBatchContext,
    table_name: &String,
    row: &Vec<ArrowBatchTypes>,
    tables: &ArrowCachedTables
) -> RowWithRefs {

    let references = match context.ref_mappings.get(table_name) {
        Some(refs) => {
            let mut unique_refs = HashSet::new();
            let mut processed_refs = HashSet::new();
            for r in refs.values() {
                let p_ref = (r.parent_index, r.parent_mapping.clone());
                if !processed_refs.contains(&p_ref) {
                    processed_refs.insert(p_ref);
                    unique_refs.insert(r);
                }
            }
            unique_refs
        },
        None => panic!("No references for \"{}\"", table_name)
    };
    let mut child_row_map = HashMap::new();

    // println!("gen_refs {}: {:?}", table_name, references.keys());

    for reference in references.iter() {
        let refs = get_rows_by_ref(
            context,
            &table_name,
            &reference.parent_mapping,
            &row[reference.parent_index],
            tables
        );

        for (child_name, child_rows) in refs.iter() {
            if !child_row_map.contains_key(child_name) {
                child_row_map.insert(child_name.clone(), Vec::new());
            }

            let row_container = child_row_map.get_mut(child_name).unwrap();

            for child_row in child_rows.iter() {
                row_container.push(
                    gen_refs(context, child_name, child_row, tables));
            }
        }
    }

    RowWithRefs { row: row.clone(), refs: child_row_map }

}

impl<'a> ArrowBatchReader<'a> {

    pub fn new(
        context: &'a ArrowBatchContext
    ) -> Self {
        ArrowBatchReader {
            context,
            cache: ArrowBatchCache::new(context),
        }
    }

    pub fn get_row(&mut self, ordinal: u64) -> Option<RowWithRefs> {
        let (start_ord, tables) = match self.cache.get_tables_for(ordinal) {
            Some(t) => t,
            None => panic!("Tables for \"{}\" not found!", ordinal)
        };

        let relative_index = ordinal - start_ord;
        let table_index = (relative_index % self.context.config.dump_size) as usize;

        let root_mapping = self.context.table_mappings.get("root").unwrap();
        let row = read_row(&tables.root, root_mapping, table_index).unwrap();
        let result = gen_refs(self.context, &"root".to_string(), &row, &tables);

        Some(result)
    }
}
