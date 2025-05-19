use serde::{Deserialize, Serialize};
use super::{Generator, WithCount, schema::{Schema, SchemaGenerator}};
use crate::seed::fake_provider::FakeProvider;

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatabaseSeedType {
    Database(Database),
    DatabaseGenerator(DatabaseGenerator),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseSeed {
    #[serde(flatten)]
    pub database: DatabaseSeedType,
}

impl DatabaseSeed {
    pub fn materialize(self) -> Database {
        match self.database {
            DatabaseSeedType::DatabaseGenerator(database_generator) => database_generator.generate(0),
            DatabaseSeedType::Database(database) => database,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    name: String,
    schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseGenerator {
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