use serde::{Deserialize, Serialize};

use crate::seed_generator::{Generator, WithCount, fake_provider::FakeProvider};
use crate::seed_models::schema::{Schema, SchemasTemplateType};

// This is different from metastore's equivalent
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Database {
    pub database_name: String,
    pub schemas: Vec<Schema>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DatabasesTemplateType {
    Databases(Vec<Database>),
    DatabasesTemplate(WithCount<Database, DatabaseGenerator>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseGenerator {
    pub database_name: Option<String>, // if None value will be generated
    pub schemas: SchemasTemplateType,
}

impl Generator<Database> for DatabaseGenerator {
    fn generate(&self, index: usize) -> Database {
        Database {
            database_name: self
                .database_name
                .clone()
                .unwrap_or_else(|| FakeProvider::entity_name(index)),
            schemas: match &self.schemas {
                SchemasTemplateType::SchemasTemplate(schema_template) => {
                    // handle WithCount template
                    schema_template.vec_with_count(index)
                }
                SchemasTemplateType::Schemas(schemas) => schemas.clone(),
            },
        }
    }
}
