use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use datafusion::{common::Result, execution::FunctionRegistry, logical_expr::ScalarUDF};
use datafusion_common::{downcast_value, DataFusionError};
use std::sync::Arc;

pub(crate) mod aggregate;
mod convert_timezone;
mod date_add;
mod date_diff;
mod date_from_parts;
//pub mod geospatial;
mod booland;
mod boolor;
mod boolxor;
mod parse_json;
pub mod table;
mod time_from_parts;
mod timestamp_from_parts;
mod to_boolean;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        convert_timezone::get_udf(),
        date_add::get_udf(),
        parse_json::get_udf(),
        date_diff::get_udf(),
        timestamp_from_parts::get_udf(),
        time_from_parts::get_udf(),
        date_from_parts::get_udf(),
        booland::get_udf(),
        boolor::get_udf(),
        boolxor::get_udf(),
        to_boolean::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}

mod macros {
    macro_rules! make_udf_function {
    ($udf_type:ty) => {
        paste::paste! {
            static [< STATIC_ $udf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
                std::sync::OnceLock::new();

            pub fn get_udf() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
                [< STATIC_ $udf_type:upper >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                            <$udf_type>::default(),
                        ))
                    })
                    .clone()
            }
        }
    }
}

    pub(crate) use make_udf_function;
}

pub(crate) fn array_to_boolean(arr: &ArrayRef) -> Result<BooleanArray> {
    let arr = arr.as_ref();
    let mut boolean_array = BooleanArray::builder(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            boolean_array.append_null();
        } else {
            let b = match arr.data_type() {
                DataType::Boolean => downcast_value!(arr, BooleanArray).value(i),
                DataType::Int8 => downcast_value!(arr, Int8Array).value(i) != 0,
                DataType::Int16 => downcast_value!(arr, Int16Array).value(i) != 0,
                DataType::Int32 => downcast_value!(arr, Int32Array).value(i) != 0,
                DataType::Int64 => downcast_value!(arr, Int64Array).value(i) != 0,
                DataType::UInt8 => downcast_value!(arr, UInt8Array).value(i) != 0,
                DataType::UInt16 => downcast_value!(arr, UInt16Array).value(i) != 0,
                DataType::UInt32 => downcast_value!(arr, UInt32Array).value(i) != 0,
                DataType::UInt64 => downcast_value!(arr, UInt64Array).value(i) != 0,
                DataType::Float32 => downcast_value!(arr, Float32Array).value(i) != 0.,
                DataType::Float64 => downcast_value!(arr, Float64Array).value(i) != 0.,
                DataType::Decimal128(_, _) => downcast_value!(arr, Decimal128Array).value(i) != 0,
                _ => {
                    return Err(DataFusionError::Internal(
                        "only supports boolean, numeric, decimal, float types".to_string(),
                    ))
                }
            };

            boolean_array.append_value(b);
        }
    }
    Ok(boolean_array.finish())
}
