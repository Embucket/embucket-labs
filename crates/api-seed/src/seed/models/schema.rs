use serde::{Deserialize, Serialize};
use super::{Generator, WithCount, table::{Table, TableGenerator}};
use crate::seed::fake_provider::FakeProvider;

///// Schema

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaGenerator {
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
