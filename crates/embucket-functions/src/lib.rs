pub use crate::aggregate::register_udafs;
use crate::conversion::{ToBooleanFunc, ToTimeFunc, to_array};
use crate::semi_structured::get::GetFunc;
use crate::semi_structured::is_typeof;
use crate::semi_structured::is_typeof::IsTypeofFunc;
use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, BooleanArray, Decimal128Array, Decimal256Array,
    Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::{common::Result, execution::FunctionRegistry, logical_expr::ScalarUDF};
use datafusion_common::DataFusionError;
#[doc(hidden)]
pub use std::iter as __std_iter;
use std::sync::Arc;

pub(crate) mod aggregate;
pub mod conditional;
pub mod conversion;
pub mod datetime;
//pub mod geospatial;
mod json;
#[path = "semi-structured/mod.rs"]
pub mod semi_structured;
pub mod session;
#[path = "string-binary/mod.rs"]
pub mod string_binary;
pub mod table;
#[cfg(test)]
pub mod tests;
pub mod visitors;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        datetime::convert_timezone::get_udf(),
        datetime::date_add::get_udf(),
        semi_structured::json::parse_json::get_udf(),
        semi_structured::json::try_parse_json::get_udf(),
        datetime::date_diff::get_udf(),
        datetime::timestamp_from_parts::get_udf(),
        datetime::time_from_parts::get_udf(),
        datetime::date_from_parts::get_udf(),
        datetime::last_day::get_udf(),
        datetime::add_months::get_udf(),
        datetime::monthname::get_udf(),
        datetime::dayname::get_udf(),
        datetime::previous_day::get_udf(),
        datetime::next_day::get_udf(),
        conditional::booland::get_udf(),
        conditional::boolor::get_udf(),
        conditional::boolxor::get_udf(),
        conditional::iff::get_udf(),
        conditional::equal_null::get_udf(),
        conditional::nullifzero::get_udf(),
        semi_structured::object::is_object::get_udf(),
        semi_structured::array::is_array::get_udf(),
        string_binary::rtrimmed_length::get_udf(),
        semi_structured::get_path::get_udf(),
        string_binary::insert::get_udf(),
        string_binary::jarowinkler_similarity::get_udf(),
        semi_structured::array::strtok_to_array::get_udf(),
        semi_structured::object::object_keys::get_udf(),
        semi_structured::json::try_parse_json::get_udf(),
        semi_structured::typeof_func::get_udf(),
        to_array::get_udf(),
        Arc::new(ScalarUDF::from(ToBooleanFunc::new(false))),
        Arc::new(ScalarUDF::from(ToBooleanFunc::new(true))),
        Arc::new(ScalarUDF::from(ToTimeFunc::new(false))),
        Arc::new(ScalarUDF::from(ToTimeFunc::new(true))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Null))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Boolean))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Double))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Real))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Integer))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::String))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Array))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Object))),
        Arc::new(ScalarUDF::from(GetFunc::new(true))),
        Arc::new(ScalarUDF::from(GetFunc::new(false))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Null))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Boolean))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Double))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Real))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Integer))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::String))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Array))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Object))),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    semi_structured::register_udfs(registry)?;
    session::register_session_context_udfs(registry)?;
    Ok(())
}

mod macros {
    // Adopted from itertools: https://docs.rs/itertools/latest/src/itertools/lib.rs.html#321-360
    macro_rules! izip {
        // @closure creates a tuple-flattening closure for .map() call. usage:
        // @closure partial_pattern => partial_tuple , rest , of , iterators
        // eg. izip!( @closure ((a, b), c) => (a, b, c) , dd , ee )
        ( @closure $p:pat => $tup:expr ) => {
            |$p| $tup
        };

        // The "b" identifier is a different identifier on each recursion level thanks to hygiene.
        ( @closure $p:pat => ( $($tup:tt)* ) , $_iter:expr $( , $tail:expr )* ) => {
            $crate::macros::izip!(@closure ($p, b) => ( $($tup)*, b ) $( , $tail )*)
        };

        // unary
        ($first:expr $(,)*) => {
            $crate::__std_iter::IntoIterator::into_iter($first)
        };

        // binary
        ($first:expr, $second:expr $(,)*) => {
            $crate::__std_iter::Iterator::zip(
                $crate::__std_iter::IntoIterator::into_iter($first),
                $second,
            )
        };

        // n-ary where n > 2
        ( $first:expr $( , $rest:expr )* $(,)* ) => {
            {
                let iter = $crate::__std_iter::IntoIterator::into_iter($first);
                $(
                    let iter = $crate::__std_iter::Iterator::zip(iter, $rest);
                )*
                $crate::__std_iter::Iterator::map(
                    iter,
                    $crate::macros::izip!(@closure a => (a) $( , $rest )*)
                )
            }
        };
    }

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

    macro_rules! make_udaf_function {
        ($udaf_type:ty) => {
            paste::paste! {
                static [< STATIC_ $udaf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> =
                    std::sync::OnceLock::new();

                pub fn get_udaf() -> std::sync::Arc<datafusion::logical_expr::AggregateUDF> {
                    [< STATIC_ $udaf_type:upper >]
                        .get_or_init(|| {
                            std::sync::Arc::new(datafusion::logical_expr::AggregateUDF::new_from_impl(
                                <$udaf_type>::default(),
                            ))
                        })
                        .clone()
                }
            }
        }
    }

    pub(crate) use izip;
    pub(crate) use make_udaf_function;
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
        DataType::Null => BooleanArray::new_null(arr.len()),
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
        DataType::Utf8View => {
            // special case, because scalar null (like func(NULL)) is treated as a Utf8View
            let mut boolean_array = BooleanArray::builder(arr.len());
            let arr = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
            for v in arr {
                if v.is_some() {
                    return Err(DataFusionError::Internal(format!(
                        "unsupported {:?} type. Only supports boolean, numeric, decimal, float types",
                        arr.data_type()
                    )));
                }

                boolean_array.append_null();
            }

            boolean_array.finish()
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "unsupported {:?} type. Only supports boolean, numeric, decimal, float types",
                arr.data_type()
            )));
        }
    })
}
