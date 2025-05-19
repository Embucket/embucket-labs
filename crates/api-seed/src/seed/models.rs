use core_metastore::VolumeType;
use fake::faker::{lorem::en::Word, name::raw::Name};
use fake::{Fake, locales::EN};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

struct FakeProvider;

impl FakeProvider {
    pub fn volume_type() -> VolumeType {
        VolumeType::Memory // no random volume type
    }
    pub fn person_name() -> String {
        Name(EN).fake()
    }
    pub fn entity_name() -> String {
        let one: String = Word().fake();
        let two: String = Word().fake();
        format!("{}_{}", one.to_lowercase(), two.to_lowercase())
    }
}

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
        Self {
            count,
            generator,
            _marker: PhantomData,
        }
    }

    pub fn generate(&self) -> Vec<T> {
        (0..self.count)
            .map(|i| self.generator.generate(i))
            .collect()
    }
}

///// Seed Root

#[derive(Debug, Serialize, Deserialize)]
pub struct SeedRoot {
    pub volumes: Vec<SuperVolume>,
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
    pub volume_type: VolumeType,
    pub databases: Vec<Database>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VolumeGenerator {
    pub volume_name: Option<String>, // if None value will be generated
    pub volume_type: Option<VolumeType>,
    pub databases_gen: WithCount<Database, DatabaseGenerator>,
}

impl Generator<Volume> for VolumeGenerator {
    fn generate(&self, _index: usize) -> Volume {
        Volume {
            volume_name: self
                .volume_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name()),
            volume_type: self.volume_type.clone().unwrap_or(VolumeType::Memory),
            databases: self.databases_gen.generate(),
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
            name: FakeProvider::entity_name(),
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
            name: FakeProvider::entity_name(),
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
            name: FakeProvider::entity_name(),
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
            name: FakeProvider::entity_name(),
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
