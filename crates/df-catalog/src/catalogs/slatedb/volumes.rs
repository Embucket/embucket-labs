use crate::catalogs::slatedb::config::SlateDBViewConfig;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct VolumesView {
    schema: SchemaRef,
    config: SlateDBViewConfig,
}

impl VolumesView {
    pub(crate) fn new(config: SlateDBViewConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("volume_name", DataType::Utf8, false),
            Field::new("volume_type", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> VolumesViewBuilder {
        VolumesViewBuilder {
            volume_names: StringBuilder::new(),
            volume_types: StringBuilder::new(),
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
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        ))
    }
}

pub struct VolumesViewBuilder {
    schema: SchemaRef,
    volume_names: StringBuilder,
    volume_types: StringBuilder,
}

impl VolumesViewBuilder {
    pub fn add_volume(&mut self, volume_name: impl AsRef<str>, volume_type: impl AsRef<str>) {
        // Note: append_value is actually infallible.
        self.volume_names.append_value(volume_name.as_ref());
        self.volume_types.append_value(volume_type.as_ref());
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.volume_names.finish()),
                Arc::new(self.volume_types.finish()),
            ],
        )
    }
}
