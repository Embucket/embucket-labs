use super::{Generator, WithCount};
use crate::seed::fake_provider::FakeProvider;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    pub col_name: String,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnType {
    String,
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

const COLUMN_TYPES: [ColumnType; 11] = [
    ColumnType::String,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnsTemplateType {
    Columns(Vec<Column>),
    ColumnsTemplate(WithCount<Column, ColumnGenerator>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnGenerator {
    pub col_name: Option<String>, // if None value will be generated
}

impl Generator<Column> for ColumnGenerator {
    fn generate(&self, index: usize) -> Column {
        let mut rng = rand::rng();
        match COLUMN_TYPES.choose(&mut rng) {
            Some(col_type) => Column {
                col_name: self
                    .col_name
                    .clone()
                    .unwrap_or_else(FakeProvider::entity_name),
                col_type: col_type.clone(),
            },
            None => Column {
                col_name: format!("dummy{index}"),
                col_type: ColumnType::String,
            },
        }
    }
}
