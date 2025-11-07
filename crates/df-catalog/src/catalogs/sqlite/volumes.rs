use crate::catalogs::sqlite::metastore_config::MetastoreViewConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::Int64Builder,
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct VolumesView {
    schema: SchemaRef,
    config: MetastoreViewConfig,
}

impl VolumesView {
    pub(crate) fn new(config: MetastoreViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("volume_id", DataType::Int64, false),
            Field::new("volume_name", DataType::Utf8, false),
            Field::new("volume_type", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("updated_at", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> VolumesViewBuilder {
        VolumesViewBuilder {
            volume_ids: Int64Builder::new(),
            volume_names: StringBuilder::new(),
            volume_types: StringBuilder::new(),
            created_at_timestamps: StringBuilder::new(),
            updated_at_timestamps: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for VolumesView {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_volumes(&mut builder).await?;
                Ok(builder.finish()?)
            }),
        ))
    }
}

pub struct VolumesViewBuilder {
    schema: SchemaRef,
    volume_ids: Int64Builder,
    volume_names: StringBuilder,
    volume_types: StringBuilder,
    created_at_timestamps: StringBuilder,
    updated_at_timestamps: StringBuilder,
}

impl VolumesViewBuilder {
    pub fn add_volume(
        &mut self,
        volume_id: i64,
        volume_name: impl AsRef<str>,
        volume_type: impl AsRef<str>,
        created_at: impl AsRef<str>,
        updated_at: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallible.
        self.volume_ids.append_value(volume_id);
        self.volume_names.append_value(volume_name.as_ref());
        self.volume_types.append_value(volume_type.as_ref());
        self.created_at_timestamps.append_value(created_at.as_ref());
        self.updated_at_timestamps.append_value(updated_at.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.volume_ids.finish()),
                Arc::new(self.volume_names.finish()),
                Arc::new(self.volume_types.finish()),
                Arc::new(self.created_at_timestamps.finish()),
                Arc::new(self.updated_at_timestamps.finish()),
            ],
        )
    }
}
