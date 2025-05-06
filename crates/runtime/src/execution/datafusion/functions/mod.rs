use crate::execution::datafusion::functions::to_boolean::ToBooleanFunc;
use arrow_array::{
    ArrayRef, ArrowNativeTypeOp, BooleanArray, Decimal128Array, Decimal256Array, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use datafusion::{common::Result, execution::FunctionRegistry, logical_expr::ScalarUDF};
use datafusion_common::DataFusionError;
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
        Arc::new(ScalarUDF::from(ToBooleanFunc::new(false))),
        Arc::new(ScalarUDF::from(ToBooleanFunc::new(true))),
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

macro_rules! numeric_to_boolean {
    ($arr:expr, $type:ty) => {{
        let mut boolean_array = BooleanArray::builder($arr.len());
        let arr = $arr.as_any().downcast_ref::<$type>().unwrap();
        for v in arr {
            if let Some(v) = v {
                boolean_array.append_value(!v.is_zero());
            } else {
                boolean_array.append_null();
            }
        }

        boolean_array.finish()
    }};
}

#[allow(clippy::cognitive_complexity, clippy::unwrap_used)]
pub(crate) fn array_to_boolean(arr: &ArrayRef) -> Result<BooleanArray> {
    Ok(match arr.data_type() {
        DataType::Boolean => {
            let mut boolean_array = BooleanArray::builder(arr.len());
            let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            for v in arr {
                if let Some(v) = v {
                    boolean_array.append_value(v);
                } else {
                    boolean_array.append_null();
                }
            }
            boolean_array.finish()
        }
        DataType::Int8 => numeric_to_boolean!(&arr, Int8Array),
        DataType::Int16 => numeric_to_boolean!(&arr, Int16Array),
        DataType::Int32 => numeric_to_boolean!(&arr, Int32Array),
        DataType::Int64 => numeric_to_boolean!(&arr, Int64Array),
        DataType::UInt8 => numeric_to_boolean!(&arr, UInt8Array),
        DataType::UInt16 => numeric_to_boolean!(&arr, UInt16Array),
        DataType::UInt32 => numeric_to_boolean!(&arr, UInt32Array),
        DataType::UInt64 => numeric_to_boolean!(&arr, UInt64Array),
        DataType::Float16 => numeric_to_boolean!(&arr, Float16Array),
        DataType::Float32 => numeric_to_boolean!(&arr, Float32Array),
        DataType::Float64 => numeric_to_boolean!(&arr, Float64Array),
        DataType::Decimal128(_, _) => numeric_to_boolean!(&arr, Decimal128Array),
        DataType::Decimal256(_, _) => numeric_to_boolean!(&arr, Decimal256Array),
        _ => {
            return Err(DataFusionError::Internal(
                "only supports boolean, numeric, decimal, float types".to_string(),
            ))
        }
    })
}
