pub use crate::aggregate::register_udafs;
use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, BooleanArray, Decimal128Array, Decimal256Array,
    Float16Array, Float32Array, Float64Array, GenericStringArray, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringBuilder, StringArray, StringBuilder, StringViewArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::error::Result as DFResult;
use datafusion::{common::Result, execution::FunctionRegistry};

#[doc(hidden)]
pub use std::iter as __std_iter;
use std::sync::Arc;

pub use crate::aggregate::errors as aggregate_errors;
pub use crate::conversion::errors as conversion_errors;
pub use crate::datetime::errors as datetime_errors;
use crate::session_params::SessionParams;

pub(crate) mod aggregate;
pub mod arrow_error;
pub mod conditional;
pub mod conversion;
pub mod datetime;
pub mod df_error;

// Explicitely disable non-working geospatial, as workaround for cargo test --all-features
// #[cfg(feature = "geospatial")]
// pub mod geospatial;
pub mod crypto;
pub mod encryption;
mod errors;
pub mod expr_planner;
mod json;
pub mod numeric;
pub mod regexp;
#[path = "semi-structured/mod.rs"]
pub mod semi_structured;
pub mod session;
pub mod session_params;
#[path = "string-binary/mod.rs"]
pub mod string_binary;
pub mod system;
pub mod table;
#[cfg(test)]
pub mod tests;
mod utils;
pub mod visitors;
pub mod window;

pub fn register_udfs(
    registry: &mut dyn FunctionRegistry,
    session_params: &Arc<SessionParams>,
) -> Result<()> {
    conditional::register_udfs(registry)?;
    conversion::register_udfs(registry, session_params)?;
    crypto::register_udfs(registry)?;
    datetime::register_udfs(registry, session_params)?;
    numeric::register_udfs(registry)?;
    encryption::register_udfs(registry)?;
    string_binary::register_udfs(registry)?;
    semi_structured::register_udfs(registry)?;
    regexp::register_udfs(registry)?;
    system::register_udfs(registry)?;
    session::register_session_context_udfs(registry, session_params)?;
    window::register_udwfs(registry)?;
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
                    return df_error::UnsupportedTypeSnafu {
                        data_type: arr.data_type().clone(),
                    }
                    .fail()?;
                }

                boolean_array.append_null();
            }

            boolean_array.finish()
        }
        _ => {
            return df_error::UnsupportedTypeSnafu {
                data_type: arr.data_type().clone(),
            }
            .fail()?;
        }
    })
}

/// A common trait to abstract over different string builders
pub(crate) trait StringBuildLike {
    fn append_value(&mut self, v: &str);
    fn append_null(&mut self);
    fn finish(self) -> ArrayRef;
}

impl StringBuildLike for StringBuilder {
    fn append_value(&mut self, v: &str) {
        self.append_value(v);
    }
    fn append_null(&mut self) {
        self.append_null();
    }
    fn finish(mut self) -> ArrayRef {
        Arc::new(Self::finish(&mut self))
    }
}

impl StringBuildLike for LargeStringBuilder {
    fn append_value(&mut self, v: &str) {
        self.append_value(v);
    }
    fn append_null(&mut self) {
        self.append_null();
    }
    fn finish(mut self) -> ArrayRef {
        Arc::new(Self::finish(&mut self))
    }
}

/// This represents any string-like array in `DataFusion`
/// including `Utf8`, `Utf8View`, and `LargeUtf8`.
///
/// This is useful for writing UDFs that work across different string representations.
#[derive(Debug, Clone)]
pub(crate) enum Utf8LikeArray {
    Utf8(Arc<StringArray>),
    Utf8View(Arc<StringViewArray>),
    LargeUtf8(Arc<GenericStringArray<i64>>),
}

impl Utf8LikeArray {
    /// Converts a supported string array (`Utf8`, `Utf8View`, `LargeUtf8`)
    /// into `Utf8LikeArray`
    ///
    /// This allows UDFs to accept any string input type without panicking.
    ///
    /// # Errors
    /// Returns an error if the input type is unsupported or the downcast fails.
    pub fn try_from_array(array: &ArrayRef) -> DFResult<Self> {
        match array.data_type() {
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        ArrowError::InvalidArgumentError("Expected StringArray for Utf8".into())
                    })?;
                Ok(Self::Utf8(Arc::new(arr.clone())))
            }
            DataType::Utf8View => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        ArrowError::InvalidArgumentError(
                            "Expected StringViewArray for Utf8View".into(),
                        )
                    })?;
                Ok(Self::Utf8View(Arc::new(arr.clone())))
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<GenericStringArray<i64>>()
                    .ok_or_else(|| {
                        ArrowError::InvalidArgumentError(
                            "Expected GenericStringArray<i64> for LargeUtf8".into(),
                        )
                    })?;
                Ok(Self::LargeUtf8(Arc::new(arr.clone())))
            }
            other => datafusion_common::exec_err!(
                "Expected Utf8, Utf8View, or LargeUtf8 but got {other:?}"
            ),
        }
    }

    /// Returns a boolean whether value at the given index is null.
    #[must_use]
    pub fn is_null(&self, i: usize) -> bool {
        match self {
            Self::Utf8(arr) => arr.is_null(i),
            Self::Utf8View(arr) => arr.is_null(i),
            Self::LargeUtf8(arr) => arr.is_null(i),
        }
    }

    /// Returns the length of the array.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Utf8(arr) => arr.len(),
            Self::Utf8View(arr) => arr.len(),
            Self::LargeUtf8(arr) => arr.len(),
        }
    }

    /// Returns the value at the given index as an `Option<&str>`, or `None` if null.
    #[must_use]
    pub fn value(&self, i: usize) -> Option<&str> {
        match self {
            Self::Utf8(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            }
            Self::Utf8View(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            }
            Self::LargeUtf8(arr) => {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            }
        }
    }
}

#[must_use]
pub fn to_snowflake_datatype(data_type: &DataType) -> String {
    let s: &str = match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => "fixed",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "real",
        DataType::Boolean => "boolean",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                "timestamp_tz"
            } else {
                "timestamp_ntz"
            }
        }
        DataType::Binary | DataType::BinaryView => "binary",
        _ => "text",
    };

    s.to_string()
}
