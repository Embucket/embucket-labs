use crate::information_schema::config::InformationSchemaConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_common::config::{ConfigEntry, ConfigField, ConfigOptions, Visit};
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::{Debug, Display};
use std::sync::Arc;

#[derive(Debug)]
pub struct InformationSchemaDfSettings {
    schema: SchemaRef,
}

impl InformationSchemaDfSettings {
    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("created_on", DataType::Utf8, true),
            Field::new("updated_on", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("session_id", DataType::Utf8, true),
        ]))
    }

    pub fn new() -> Self {
        let schema = Self::schema();
        Self { schema }
    }

    fn builder(&self) -> InformationSchemaDfSettingsBuilder {
        InformationSchemaDfSettingsBuilder {
            names: StringBuilder::new(),
            values: StringBuilder::new(),
            descriptions: StringBuilder::new(),
            created_ons: StringBuilder::new(),
            updated_ons: StringBuilder::new(),
            types: StringBuilder::new(),
            session_ids: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaDfSettings {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        InformationSchemaConfig::make_df_settings(ctx.session_config().options(), &mut builder);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter([builder
                .finish()
                .map_err(|e| DataFusionError::ArrowError(e, None))]),
        ))
    }
}

pub struct InformationSchemaDfSettingsBuilder {
    schema: SchemaRef,
    names: StringBuilder,
    values: StringBuilder,
    descriptions: StringBuilder,
    types: StringBuilder,
    created_ons: StringBuilder,
    updated_ons: StringBuilder,
    session_ids: StringBuilder,
}

impl InformationSchemaDfSettingsBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn add_setting(
        &mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
        setting_type: impl AsRef<str>,
        description: impl AsRef<str>,
        created_on: Option<String>,
        updated_on: Option<String>,
        session_id: Option<String>,
    ) {
        self.names.append_value(name.as_ref());
        self.values.append_value(value.as_ref());
        self.descriptions.append_value(description);
        self.types.append_value(setting_type.as_ref());
        self.created_ons.append_option(created_on);
        self.updated_ons.append_option(updated_on);
        self.session_ids.append_option(session_id);
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.names.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.descriptions.finish()),
                Arc::new(self.created_ons.finish()),
                Arc::new(self.updated_ons.finish()),
                Arc::new(self.types.finish()),
                Arc::new(self.session_ids.finish()),
            ],
        )
    }
}

pub struct ConfigVisitor {
    entries: Vec<ConfigEntry>,
}
impl ConfigVisitor {
    pub fn standard_entries(config: &ConfigOptions) -> Vec<ConfigEntry> {
        let mut visitor = Self { entries: vec![] };
        config.visit(&mut visitor, "datafusion", "");
        visitor.entries
    }
}
impl Visit for ConfigVisitor {
    fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
        self.entries.push(ConfigEntry {
            key: key.to_string(),
            value: Some(value.to_string()),
            description,
        });
    }

    fn none(&mut self, key: &str, description: &'static str) {
        self.entries.push(ConfigEntry {
            key: key.to_string(),
            value: None,
            description,
        });
    }
}
