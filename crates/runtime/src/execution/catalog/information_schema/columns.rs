//! [`InformationSchemaColumns`] that implements the SQL [Information Schema Column] for Snowflake.
//!
//! [Information Schema Column]: https://docs.snowflake.com/en/sql-reference/info-schema/columns

use crate::execution::catalog::information_schema::config::InformationSchemaConfig;
use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_array::builder::UInt64Builder;
use arrow_schema::ArrowError;
use datafusion::execution::TaskContext;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::SendableRecordBatchStream;
use std::fmt::Debug;
use std::sync::Arc;
use DataType::{
    Binary, Decimal128, Float16, Float32, Float64, Int16, Int32, Int8, LargeBinary, LargeUtf8,
    UInt16, UInt32, UInt64, UInt8, Utf8,
};

#[derive(Debug)]
pub struct InformationSchemaColumns {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaColumns {
    pub fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", Utf8, false),
            Field::new("table_schema", Utf8, false),
            Field::new("table_name", Utf8, false),
            Field::new("column_name", Utf8, false),
            Field::new("ordinal_position", UInt64, false),
            Field::new("column_default", Utf8, true),
            Field::new("is_nullable", Utf8, false),
            Field::new("data_type", Utf8, false),
            Field::new("character_maximum_length", UInt64, true),
            Field::new("character_octet_length", UInt64, true),
            Field::new("numeric_precision", UInt64, true),
            Field::new("numeric_precision_radix", UInt64, true),
            Field::new("numeric_scale", UInt64, true),
            Field::new("datetime_precision", UInt64, true),
            Field::new("interval_type", Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaColumnsBuilder {
        // StringBuilder requires providing an initial capacity, so
        // pick 10 here arbitrarily as this is not performance
        // critical code and the number of tables is unavailable here.
        let default_capacity = 10;

        InformationSchemaColumnsBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            column_names: StringBuilder::new(),
            ordinal_positions: UInt64Builder::with_capacity(default_capacity),
            column_defaults: StringBuilder::new(),
            is_nullables: StringBuilder::new(),
            data_types: StringBuilder::new(),
            character_maximum_lengths: UInt64Builder::with_capacity(default_capacity),
            character_octet_lengths: UInt64Builder::with_capacity(default_capacity),
            numeric_precisions: UInt64Builder::with_capacity(default_capacity),
            numeric_precision_radixes: UInt64Builder::with_capacity(default_capacity),
            numeric_scales: UInt64Builder::with_capacity(default_capacity),
            datetime_precisions: UInt64Builder::with_capacity(default_capacity),
            interval_types: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaColumns {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_columns(&mut builder).await?;
                builder
                    .finish()
                    .map_err(|e| DataFusionError::ArrowError(e, None))
            }),
        ))
    }
}

pub struct InformationSchemaColumnsBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    column_names: StringBuilder,
    ordinal_positions: UInt64Builder,
    column_defaults: StringBuilder,
    is_nullables: StringBuilder,
    data_types: StringBuilder,
    character_maximum_lengths: UInt64Builder,
    character_octet_lengths: UInt64Builder,
    numeric_precisions: UInt64Builder,
    numeric_precision_radixes: UInt64Builder,
    numeric_scales: UInt64Builder,
    datetime_precisions: UInt64Builder,
    interval_types: StringBuilder,
}

impl InformationSchemaColumnsBuilder {
    #[allow(clippy::as_conversions, clippy::cast_sign_loss)]
    pub fn add_column(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        field_position: usize,
        field: &Field,
    ) {
        // Note: append_value is actually infallable.
        self.catalog_names.append_value(catalog_name);
        self.schema_names.append_value(schema_name);
        self.table_names.append_value(table_name);

        self.column_names.append_value(field.name());

        self.ordinal_positions.append_value(field_position as u64);

        // DataFusion does not support column default values, so null
        self.column_defaults.append_null();

        // "YES if the column is possibly nullable, NO if it is known not nullable. "
        let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
        self.is_nullables.append_value(nullable_str);

        // "System supplied type" --> Use debug format of the datatype
        self.data_types
            .append_value(format!("{:?}", field.data_type()));

        // "If data_type identifies a character or bit string type, the
        // declared maximum length; null for all other data types or
        // if no maximum length was declared."
        //
        // Arrow has no equivalent of VARCHAR(20), so we leave this as Null
        self.character_maximum_lengths.append_option(None);

        // "Maximum length, in bytes, for binary data, character data,
        // or text and image data."
        let char_len: Option<u64> = match field.data_type() {
            Utf8 | Binary => Some(i32::MAX as u64),
            LargeBinary | LargeUtf8 => Some(i64::MAX as u64),
            _ => None,
        };
        self.character_octet_lengths.append_option(char_len);

        // numeric_precision: "If data_type identifies a numeric type, this column
        // contains the (declared or implicit) precision of the type
        // for this column. The precision indicates the number of
        // significant digits. It can be expressed in decimal (base
        // 10) or binary (base 2) terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null."
        //
        // numeric_radix: If data_type identifies a numeric type, this
        // column indicates in which base the values in the columns
        // numeric_precision and numeric_scale are expressed. The
        // value is either 2 or 10. For all other data types, this
        // column is null.
        //
        // numeric_scale: If data_type identifies an exact numeric
        // type, this column contains the (declared or implicit) scale
        // of the type for this column. The scale indicates the number
        // of significant digits to the right of the decimal point. It
        // can be expressed in decimal (base 10) or binary (base 2)
        // terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null.
        let (numeric_precision, numeric_radix, numeric_scale) = match field.data_type() {
            Int8 | UInt8 => (Some(8), Some(2), None),
            Int16 | UInt16 => (Some(16), Some(2), None),
            Int32 | UInt32 => (Some(32), Some(2), None),
            // From max value of 65504 as explained on
            // https://en.wikipedia.org/wiki/Half-precision_floating-point_format#Exponent_encoding
            Float16 => (Some(15), Some(2), None),
            // Numbers from postgres `real` type
            Float32 | Float64 => (Some(24), Some(2), None),
            // Numbers from postgres `double` type and postgres `double` type
            Decimal128(precision, scale) => {
                (Some(u64::from(*precision)), Some(10), Some(*scale as u64))
            }
            _ => (None, None, None),
        };

        self.numeric_precisions.append_option(numeric_precision);
        self.numeric_precision_radixes.append_option(numeric_radix);
        self.numeric_scales.append_option(numeric_scale);

        self.datetime_precisions.append_option(None);
        self.interval_types.append_null();
    }

    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.column_names.finish()),
                Arc::new(self.ordinal_positions.finish()),
                Arc::new(self.column_defaults.finish()),
                Arc::new(self.is_nullables.finish()),
                Arc::new(self.data_types.finish()),
                Arc::new(self.character_maximum_lengths.finish()),
                Arc::new(self.character_octet_lengths.finish()),
                Arc::new(self.numeric_precisions.finish()),
                Arc::new(self.numeric_precision_radixes.finish()),
                Arc::new(self.numeric_scales.finish()),
                Arc::new(self.datetime_precisions.finish()),
                Arc::new(self.interval_types.finish()),
            ],
        )
    }
}
