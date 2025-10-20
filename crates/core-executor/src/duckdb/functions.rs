use crate::error::{self as ex_error, Result as CoreResult};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_common::config::ConfigOptions;
use duckdb::vscalar::{ArrowFunctionSignature, VArrowScalar};
use duckdb::Connection;
use embucket_functions::string_binary::length::LengthFunc;
use snafu::ResultExt;
use std::{error::Error, sync::Arc};
pub struct DfUdfWrapper<T: ScalarUDFImpl> {
    inner: T,
}

impl<T: ScalarUDFImpl> DfUdfWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: ScalarUDFImpl + Default> VArrowScalar for DfUdfWrapper<T> {
    type State = ();

    fn invoke(_state: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let num_rows = input.num_rows();
        let schema = input.schema();
        let func = T::default();
        let args: Vec<ColumnarValue> = input
            .columns()
            .iter()
            .map(|col| ColumnarValue::Array(col.clone()))
            .collect();

        let arg_fields: Vec<FieldRef> = schema
            .fields()
            .iter()
            .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), f.is_nullable())))
            .collect();

        let input_types: Vec<DataType> = arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();

        let return_field = Arc::new(Field::new(
            func.name(),
            func.return_type(&input_types)?,
            true,
        ));

        let args_struct = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: num_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args_struct)?;

        match result {
            ColumnarValue::Array(arr) => Ok(arr),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(num_rows)?;
                Ok(array)
            }
        }
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        let func = T::default();
        let sig = func.signature();

        match &sig.type_signature {
            datafusion::logical_expr::TypeSignature::Exact(types) => vec![ArrowFunctionSignature::exact(
                types.clone(),
                func.return_type(types).unwrap_or(DataType::Utf8),
            )],
            datafusion::logical_expr::TypeSignature::Variadic(valid_types) => vec![ArrowFunctionSignature::exact(
                vec![valid_types.first().cloned().unwrap_or(DataType::Utf8)],
                func
                    .return_type(&[valid_types.first().cloned().unwrap_or(DataType::Utf8)])
                    .unwrap_or(DataType::Utf8),
            )],
            _ => vec![ArrowFunctionSignature::exact(
                vec![DataType::Utf8],
               DataType::Utf8,
            )],
        }
    }
}

pub fn register_all_udfs(conn: &Connection) -> CoreResult<()> {
    conn.register_scalar_function::<DfUdfWrapper<LengthFunc>>("length_test")
        .context(ex_error::DuckdbSnafu)?;
    Ok(())
}
