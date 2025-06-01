use serde::{Deserialize, Serialize};

use crate::seed_generator::{Generator, WithCount, fake_provider::FakeProvider};
use crate::seed_models::column::{Column, ColumnsTemplateType};

///// Table

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TablesTemplateType {
    Tables(Vec<Table>),
    TablesTemplate(WithCount<Table, TableGenerator>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableGenerator {
    pub table_name: Option<String>, // if None value will be generated
    pub columns: ColumnsTemplateType,
}

impl Generator<Table> for TableGenerator {
    fn generate(&self, index: usize) -> Table {
        Table {
            table_name: self
                .table_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name(index)),
            columns: match &self.columns {
                ColumnsTemplateType::ColumnsTemplate(column_template) => {
                    // handle WithCount template
                    column_template.vec_with_count(index)
                }
                ColumnsTemplateType::Columns(columns) => columns.clone(),
            },
        }
    }
}

///// Column
