use serde::{Deserialize, Serialize};
use super::{Generator, WithCount};
use crate::seed::fake_provider::FakeProvider;

///// Table

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableGenerator {
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