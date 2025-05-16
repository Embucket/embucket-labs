use fake::locales::Data;
use fake::faker::{name::raw::Name, lorem::en::Word};
use fake::{Dummy, Fake, locales::EN};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use core_metastore::VolumeType;

///// Traits

pub trait Generator<T> {
    fn generate(&self, index: usize) -> T;
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct WithCount<T, G>
where
    G: Generator<T>,
{
    count: usize,
    generator: G,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T, G> WithCount<T, G>
where
    G: Generator<T>,
{
    pub fn new(count: usize, generator: G) -> Self {
        Self { count, generator, _marker: PhantomData }
    }

    pub fn generate(&self) -> Vec<T> {
        (0..self.count)
            .map(|i| self.generator.generate(i))
            .collect()
    }
}

///// Super Volume

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SuperVolumeType {
    Volume(Volume),
    VolumeGenerator(VolumeGenerator),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SuperVolume {
    #[serde(flatten)]
    pub volume: SuperVolumeType,
}

impl SuperVolume {
    pub fn materialize(self) -> Volume {
        match self.volume {
            SuperVolumeType::VolumeGenerator(volume_generator) => volume_generator.generate(0),
            SuperVolumeType::Volume(volume) => volume,
        }
    }
}

///// Volume

#[derive(Debug, Serialize, Deserialize)]
pub struct Volume {
    pub volume_name: String,
    pub databases: Vec<Database>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeGenerator {
    databases_gen: WithCount<Database, DatabaseGenerator>,
    volume_name: Option<String>, // if None value will be generated    
}

impl Generator<Volume> for VolumeGenerator {
    fn generate(&self, _index: usize) -> Volume {
        Volume {
            databases: self.databases_gen.generate(),
            volume_name: self.volume_name.clone().unwrap_or_else(|| Name(EN).fake()),
        }
    }
}

///// Database

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    name: String,
    schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DatabaseGenerator {
    name: Option<String>, // if None value will be generated
    schemas_gen: WithCount<Schema, SchemaGenerator>,
}

impl Generator<Database> for DatabaseGenerator {
    fn generate(&self, _index: usize) -> Database {
        Database {
            name: Name(EN).fake(),
            schemas: self.schemas_gen.generate(),
        }
    }
}


///// Schema

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SchemaGenerator {
    name: Option<String>, // if None value will be generated
    tables_gen: WithCount<Table, TableGenerator>,
}

impl Generator<Schema> for SchemaGenerator {
    fn generate(&self, _index: usize) -> Schema {
        Schema {
            name: Name(EN).fake(),
            tables: self.tables_gen.generate(),
        }
    }
}


///// Table

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableGenerator {
    name: Option<String>, // if None value will be generated
    columns_gen: WithCount<Column, ColumnGenerator>,
}

impl Generator<Table> for TableGenerator {
    fn generate(&self, _index: usize) -> Table {
        Table {
            name: Name(EN).fake(),
        }
    }
}


///// Column

#[derive(Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ColumnGenerator {
    name: Option<String>, // if None value will be generated
}

impl Generator<Column> for ColumnGenerator {
    fn generate(&self, _index: usize) -> Column {
        Column {
            name: Name(EN).fake(),
            col_type: "".to_string(),
        }
    }
}


    // let table_gen = TableGenerator;
    // let schema_gen = SchemaGenerator {
    //     table_gen: WithCount::new(3, table_gen),
    // };
    // let db_gen = DatabaseGenerator {
    //     schema_gen: WithCount::new(2, schema_gen),
    // };

    // let dbs = WithCount::new(1, db_gen).generate();


// #[derive(Debug, Serialize, Deserialize)]
// struct SeedFactor {
//     pub name: String,
//     pub x_min: u16,
//     pub x_max: u16,
//     pub rng_seed: Option<u64>,
// }
