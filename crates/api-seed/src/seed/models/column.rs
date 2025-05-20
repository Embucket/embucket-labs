use rand::{rng, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use super::{Generator, WithCount};
use crate::seed::fake_provider::FakeProvider;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub col_name: String,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ColumnType {
    Number,
    Real,
    Varchar,
    Boolean,
    Int,
    Date,
    Timestamp,
    Variant,
    Object,
    Array,
}

const COLUMN_TYPES: [ColumnType; 10] = [
    ColumnType::Number,
    ColumnType::Real,
    ColumnType::Varchar,
    ColumnType::Boolean,
    ColumnType::Int,
    ColumnType::Date,
    ColumnType::Timestamp,
    ColumnType::Variant,
    ColumnType::Object,
    ColumnType::Array,
];

#[derive(Debug, Serialize, Deserialize)]
pub enum ColumnsTemplateType {
    Columns(Vec<Column>),
    ColumnsTemplate(WithCount<Column, ColumnGenerator>)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnGenerator {
    pub col_name: Option<String>, // if None value will be generated
}

impl Generator<Column> for ColumnGenerator {
    fn generate(&self, index: usize) -> Column {
        let mut rng = rand::rng();
        let col_type = COLUMN_TYPES.choose(&mut rng).unwrap();

        Column {
            col_name: self.col_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name()),
            col_type: col_type.clone(),
        }
    }
}
