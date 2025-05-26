use crate::seed_generator::{Generator, WithCount, fake_provider::FakeProvider};
use crate::seed_models::table::{Table, TablesTemplateType};
use serde::{Deserialize, Serialize};

///// Schema

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub schema_name: String,
    pub tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchemasTemplateType {
    Schemas(Vec<Schema>),
    SchemasTemplate(WithCount<Schema, SchemaGenerator>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaGenerator {
    pub schema_name: Option<String>, // if None value will be generated
    pub tables: TablesTemplateType,
}

impl Generator<Schema> for SchemaGenerator {
    fn generate(&self, index: usize) -> Schema {
        Schema {
            schema_name: self
                .schema_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name(index)),
            tables: match &self.tables {
                TablesTemplateType::TablesTemplate(table_template) => {
                    // handle WithCount template
                    table_template.vec_with_count(index)
                }
                TablesTemplateType::Tables(tables) => tables.clone(),
            },
        }
    }
}
