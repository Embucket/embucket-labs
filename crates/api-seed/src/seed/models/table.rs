use super::{
    Generator, WithCount,
    column::{Column, ColumnsTemplateType},
};
use crate::seed::fake_provider::FakeProvider;
use serde::{Deserialize, Serialize};

///// Table

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TablesTemplateType {
    Tables(Vec<Table>),
    TablesTemplate(WithCount<Table, TableGenerator>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableGenerator {
    pub name: Option<String>, // if None value will be generated
    pub columns: ColumnsTemplateType,
}

impl Generator<Table> for TableGenerator {
    fn generate(&self, index: usize) -> Table {
        Table {
            name: self
                .name
                .clone()
                .unwrap_or_else(FakeProvider::entity_name),
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
