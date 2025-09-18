use datafusion::arrow::array::{Array, ArrayRef, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::cast::{
    as_int64_array, as_large_string_array, as_string_array, as_string_view_array,
};

use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use rand::SeedableRng;
use rand::distr::{Alphanumeric, SampleString};
use rand::rngs::StdRng;
use std::any::Any;
use std::sync::Arc;

use crate::string_binary::errors::{
    ArrayLengthMismatchSnafu, InvalidArgumentCountSnafu, InvalidArgumentTypeSnafu,
    InvalidParameterValueSnafu, NumericValueNotRecognizedSnafu,
};

/// `RANDSTR` SQL function
///
/// Generates a random alphanumeric string of the specified length using
/// characters from [0-9a-zA-Z]. The randomness is derived from the provided
/// generator seed. For the same inputs, the output is deterministic.
///
/// Syntax: `RANDSTR(<length>, <gen>)`
///
/// Arguments:
/// - `length`: Target string length (non-negative integer).
/// - `gen`: Generator seed (integer). The same seed and length yields the same result.
///
/// Example: `SELECT RANDSTR(5, 1234) AS value;`
///
/// Returns:
/// - A string consisting of characters [0-9a-zA-Z] of the requested length.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RandStrFunc {
    signature: Signature,
}

impl Default for RandStrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RandStrFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RandStrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "randstr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            InvalidArgumentCountSnafu {
                function_name: "RANDSTR".to_string(),
                expected: "2".to_string(),
                actual: arg_types.len(),
            }
            .fail()?;
        }

        let mut coerced: Vec<DataType> = Vec::with_capacity(2);
        for (idx, dt) in arg_types.iter().enumerate() {
            match dt {
                DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => coerced.push(DataType::Int64),
                other => {
                    let position = match idx {
                        0 => "1st",
                        1 => "2nd",
                        _ => "argument",
                    };
                    InvalidArgumentTypeSnafu {
                        function_name: "RANDSTR".to_string(),
                        position: position.to_string(),
                        expected_type: "numeric or NULL".to_string(),
                        actual_type: format!("{other:?}"),
                    }
                    .fail()?;
                }
            }
        }

        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let len_arr = match &args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let gen_arr = match &args[1] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        if len_arr.len() != gen_arr.len() {
            ArrayLengthMismatchSnafu.fail()?;
        }

        // Coerce both arguments to Int64 (truncate fractional part). Unsafe cast is fine here due to the type signature is always Int64.
        let len_cast = cast_to_i64_with_numeric_error(&len_arr)?;
        let gen_cast = cast_to_i64_with_numeric_error(&gen_arr)?;

        let lengths = as_int64_array(&len_cast)?;
        let seeds = as_int64_array(&gen_cast)?;

        let mut builder = StringBuilder::new();
        for i in 0..lengths.len() {
            if lengths.is_null(i) || seeds.is_null(i) {
                builder.append_null();
                continue;
            }

            let len_val = lengths.value(i);
            if len_val < 0 {
                InvalidParameterValueSnafu {
                    value: len_val,
                    reason: "length must not be negative".to_string(),
                }
                .fail()?;
            }

            let target_len: usize = match usize::try_from(len_val) {
                Ok(v) => v,
                Err(_) => InvalidParameterValueSnafu {
                    value: len_val,
                    reason: "length is out of supported range".to_string(),
                }
                .fail()?,
            };
            let seed_bits = u64::from_ne_bytes(seeds.value(i).to_ne_bytes());
            let mut rng = StdRng::seed_from_u64(seed_bits);
            let s = Alphanumeric.sample_string(&mut rng, target_len);
            builder.append_value(&s);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

crate::macros::make_udf_function!(RandStrFunc);

fn cast_to_i64_with_numeric_error(arr: &ArrayRef) -> DFResult<ArrayRef> {
    match cast(arr, &DataType::Int64) {
        Ok(v) => Ok(v),
        Err(_) => match arr.data_type() {
            DataType::Utf8 => {
                let s = as_string_array(arr)?;
                NumericValueNotRecognizedSnafu {
                    value: if s.len() > 0 && !s.is_null(0) {
                        s.value(0).to_string()
                    } else {
                        String::new()
                    },
                }
                .fail()?
            }
            DataType::Utf8View => {
                let s = as_string_view_array(arr)?;
                NumericValueNotRecognizedSnafu {
                    value: if s.len() > 0 && !s.is_null(0) {
                        s.value(0).to_string()
                    } else {
                        String::new()
                    },
                }
                .fail()?
            }
            DataType::LargeUtf8 => {
                let s = as_large_string_array(arr)?;
                NumericValueNotRecognizedSnafu {
                    value: if s.len() > 0 && !s.is_null(0) {
                        s.value(0).to_string()
                    } else {
                        String::new()
                    },
                }
                .fail()?
            }
            _ => NumericValueNotRecognizedSnafu {
                value: String::new(),
            }
            .fail()?,
        },
    }
}
