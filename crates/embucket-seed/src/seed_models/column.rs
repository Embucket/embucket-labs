use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::seed_generator::{Generator, WithCount, fake_provider::FakeProvider};

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

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let column_type: &str = match *self {
            Self::String => "string",
            Self::Number => "number",
            Self::Real => "real",
            Self::Varchar => "varchar",
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Date => "date",
            Self::Timestamp => "timestamp",
            Self::Variant => "variant",
            Self::Object => "object",
            Self::Array => "array",
        };
        write!(f, "{column_type}")
    }
}

const COLUMN_TYPES: [ColumnType; 9] = [
    ColumnType::String,
    ColumnType::Number,
    ColumnType::Real,
    ColumnType::Varchar,
    ColumnType::Boolean,
    ColumnType::Int,
    ColumnType::Date,
    ColumnType::Timestamp,
    ColumnType::Variant,
    // Not supported:
    // ColumnType::Object,
    // ColumnType::Array,
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
                    .unwrap_or_else(|| FakeProvider::entity_name(index)),
                col_type: col_type.clone(),
            },
            None => Column {
                col_name: format!("dummy{index}"),
                col_type: ColumnType::String,
            },
        }
    }
}
