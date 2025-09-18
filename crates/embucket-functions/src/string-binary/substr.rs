use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_int64_array;
use datafusion_expr::Expr;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use std::any::Any;
use std::sync::Arc;

use super::errors::{
    FailedToDowncastSnafu, InvalidArgumentTypeSnafu, NegativeSubstringLengthSnafu,
    NotEnoughArgumentsSnafu, TooManyArgumentsSnafu, UnsupportedDataTypeSnafu,
};

/// Returns the portion of the string or binary data starting at a specified position.
///
/// Compatible with Snowflake's SUBSTR function behavior, including support for negative indices.
/// For string data, operates on UTF-8 characters. For binary data, operates on bytes.
///
/// Arguments:
/// * `base_expr` - The input string, binary, or coercible value
/// * `start_expr` - Starting position (1-based). Negative values count from the end.
///   For strings: character position. For binary: byte position.
/// * `length_expr` - Optional. Maximum number of characters (for strings) or bytes (for binary) to return
///
/// Returns:
/// A string or binary value containing the specified substring
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SubstrFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SubstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SubstrFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("substring")],
        }
    }
}

const fn is_string_coercible(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Utf8
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

const fn is_binary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary)
}

fn coerce_string_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => data_type.clone(),
        _ => DataType::Utf8,
    }
}

fn coerce_binary_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Binary | DataType::LargeBinary => data_type.clone(),
        _ => DataType::Binary,
    }
}

const fn position_name(idx: usize) -> &'static str {
    match idx {
        0 => "first",
        1 => "second",
        2 => "third",
        _ => "unknown",
    }
}

impl ScalarUDFImpl for SubstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match &arg_types[0] {
            DataType::Binary => Ok(DataType::Binary),
            DataType::LargeBinary => Ok(DataType::LargeBinary),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() < 2 {
            return NotEnoughArgumentsSnafu {
                function_call: self.name().to_string(),
                expected: 2usize,
                actual: args.len(),
            }
            .fail()?;
        }

        if args.len() > 3 {
            return TooManyArgumentsSnafu {
                function_call: self.name().to_string(),
                expected: 3usize,
                actual: args.len(),
            }
            .fail()?;
        }

        let arrays = datafusion_expr::ColumnarValue::values_to_arrays(&args)?;

        let base_array = &arrays[0];
        let start_array = &arrays[1];
        let length_array = if arrays.len() == 3 {
            Some(arrays[2].as_ref())
        } else {
            None
        };

        let result = substr_snowflake(base_array, start_array, length_array)?;
        Ok(ColumnarValue::Array(result))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> DFResult<ExprSimplifyResult> {
        if args.len() >= 2
            && args.len() <= 3
            && let (Expr::Literal(string_scalar, _), Expr::Literal(start_scalar, _)) =
                (&args[0], &args[1])
        {
            if string_scalar.is_null() || start_scalar.is_null() {
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::Null,
                    None,
                )));
            }

            let string_val = string_scalar.to_string();
            if let Ok(start_val) = start_scalar.to_string().parse::<i64>() {
                let length_val = if args.len() == 3 {
                    if let Expr::Literal(length_scalar, _) = &args[2] {
                        if length_scalar.is_null() {
                            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                                ScalarValue::Null,
                                None,
                            )));
                        }
                        length_scalar.to_string().parse::<i64>().ok()
                    } else {
                        None
                    }
                } else {
                    None
                };

                let result = compute_substr_string(
                    &string_val,
                    start_val,
                    length_val.and_then(|l| u64::try_from(l).ok()),
                );
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::Utf8View(Some(result)),
                    None,
                )));
            }
        }

        Ok(ExprSimplifyResult::Original(args))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() < 2 {
            return NotEnoughArgumentsSnafu {
                function_call: self.name().to_string(),
                expected: 2usize,
                actual: arg_types.len(),
            }
            .fail()?;
        }

        if arg_types.len() > 3 {
            return TooManyArgumentsSnafu {
                function_call: self.name().to_string(),
                expected: 3usize,
                actual: arg_types.len(),
            }
            .fail()?;
        }

        let first_data_type = match &arg_types[0] {
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer() && is_string_coercible(value_type) {
                    coerce_string_type(value_type)
                } else {
                    return InvalidArgumentTypeSnafu {
                        function_name: self.name().to_string(),
                        position: position_name(0).to_string(),
                        expected_type: "a string or binary coercible type".to_string(),
                        actual_type: format!("{:?}", &arg_types[0]),
                    }
                    .fail()?;
                }
            }
            data_type if is_string_coercible(data_type) => coerce_string_type(data_type),
            data_type if is_binary_type(data_type) => coerce_binary_type(data_type),
            _ => {
                return InvalidArgumentTypeSnafu {
                    function_name: self.name().to_string(),
                    position: position_name(0).to_string(),
                    expected_type: "a string or binary coercible type".to_string(),
                    actual_type: format!("{:?}", &arg_types[0]),
                }
                .fail()?;
            }
        };

        if !arg_types[1].is_integer() && !arg_types[1].is_null() {
            return InvalidArgumentTypeSnafu {
                function_name: self.name().to_string(),
                position: position_name(1).to_string(),
                expected_type: "an integer".to_string(),
                actual_type: format!("{:?}", &arg_types[1]),
            }
            .fail()?;
        }

        if arg_types.len() == 3 && !arg_types[2].is_integer() && !arg_types[2].is_null() {
            return InvalidArgumentTypeSnafu {
                function_name: self.name().to_string(),
                position: position_name(2).to_string(),
                expected_type: "an integer".to_string(),
                actual_type: format!("{:?}", &arg_types[2]),
            }
            .fail()?;
        }

        let coerced_types = if arg_types.len() == 2 {
            vec![first_data_type, DataType::Int64]
        } else {
            vec![first_data_type, DataType::Int64, DataType::Int64]
        };

        Ok(coerced_types)
    }
}

fn compute_substr_string(input: &str, start: i64, length: Option<u64>) -> String {
    if input.is_empty() {
        return String::new();
    }

    let char_count = i64::try_from(input.chars().count()).unwrap_or(i64::MAX);

    let actual_start = match start.cmp(&0) {
        std::cmp::Ordering::Less => char_count + start + 1,
        std::cmp::Ordering::Equal => 1,
        std::cmp::Ordering::Greater => start,
    };

    if actual_start <= 0 || actual_start > char_count {
        return String::new();
    }

    let Ok(start_idx) = usize::try_from(actual_start - 1) else {
        return String::new();
    };

    let chars: Vec<char> = input.chars().collect();

    let end_idx = if let Some(len) = length {
        if len == 0 {
            return String::new();
        }
        let len_usize = usize::try_from(len).unwrap_or(usize::MAX);
        std::cmp::min(start_idx.saturating_add(len_usize), chars.len())
    } else {
        chars.len()
    };

    if start_idx >= chars.len() {
        return String::new();
    }

    chars[start_idx..end_idx].iter().collect()
}

fn compute_substr_binary(input: &[u8], start: i64, length: Option<u64>) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let byte_count = i64::try_from(input.len()).unwrap_or(i64::MAX);

    let actual_start = match start.cmp(&0) {
        std::cmp::Ordering::Less => byte_count + start + 1,
        std::cmp::Ordering::Equal => 1,
        std::cmp::Ordering::Greater => start,
    };

    if actual_start <= 0 || actual_start > byte_count {
        return Vec::new();
    }

    let Ok(start_idx) = usize::try_from(actual_start - 1) else {
        return Vec::new();
    };

    let end_idx = if let Some(len) = length {
        if len == 0 {
            return Vec::new();
        }
        let len_usize = usize::try_from(len).unwrap_or(usize::MAX);
        std::cmp::min(start_idx.saturating_add(len_usize), input.len())
    } else {
        input.len()
    };

    if start_idx >= input.len() {
        return Vec::new();
    }

    input[start_idx..end_idx].to_vec()
}

macro_rules! handle_array_type {
    ($base_array:expr, $i:expr, $data_type:ident, $array_type:ty, $array_name:literal) => {
        if let Some(arr) = $base_array.as_any().downcast_ref::<$array_type>() {
            arr.value($i).to_string()
        } else {
            return FailedToDowncastSnafu {
                array_type: $array_name.to_string(),
            }
            .fail()?;
        }
    };
}

macro_rules! handle_binary_array_type {
    ($base_array:expr, $i:expr, $array_type:ty, $array_name:literal) => {
        if let Some(arr) = $base_array.as_any().downcast_ref::<$array_type>() {
            arr.value($i).to_vec()
        } else {
            return FailedToDowncastSnafu {
                array_type: $array_name.to_string(),
            }
            .fail()?;
        }
    };
}

fn extract_string_value(base_array: &dyn Array, i: usize) -> DFResult<String> {
    let string_val: String = match base_array.data_type() {
        DataType::Utf8 => {
            handle_array_type!(
                base_array,
                i,
                Utf8,
                datafusion::arrow::array::StringArray,
                "StringArray"
            )
        }
        DataType::LargeUtf8 => {
            handle_array_type!(
                base_array,
                i,
                LargeUtf8,
                datafusion::arrow::array::LargeStringArray,
                "LargeStringArray"
            )
        }
        DataType::Utf8View => {
            handle_array_type!(
                base_array,
                i,
                Utf8View,
                datafusion::arrow::array::StringViewArray,
                "StringViewArray"
            )
        }
        _ => unreachable!(),
    };
    Ok(string_val)
}

fn extract_numeric_string_value(base_array: &dyn Array, i: usize) -> DFResult<String> {
    let string_val: String = match base_array.data_type() {
        DataType::Int8 => {
            handle_array_type!(
                base_array,
                i,
                Int8,
                datafusion::arrow::array::Int8Array,
                "Int8Array"
            )
        }
        DataType::Int16 => {
            handle_array_type!(
                base_array,
                i,
                Int16,
                datafusion::arrow::array::Int16Array,
                "Int16Array"
            )
        }
        DataType::Int32 => {
            handle_array_type!(
                base_array,
                i,
                Int32,
                datafusion::arrow::array::Int32Array,
                "Int32Array"
            )
        }
        DataType::Int64 => {
            handle_array_type!(
                base_array,
                i,
                Int64,
                datafusion::arrow::array::Int64Array,
                "Int64Array"
            )
        }
        DataType::UInt8 => {
            handle_array_type!(
                base_array,
                i,
                UInt8,
                datafusion::arrow::array::UInt8Array,
                "UInt8Array"
            )
        }
        DataType::UInt16 => {
            handle_array_type!(
                base_array,
                i,
                UInt16,
                datafusion::arrow::array::UInt16Array,
                "UInt16Array"
            )
        }
        DataType::UInt32 => {
            handle_array_type!(
                base_array,
                i,
                UInt32,
                datafusion::arrow::array::UInt32Array,
                "UInt32Array"
            )
        }
        DataType::UInt64 => {
            handle_array_type!(
                base_array,
                i,
                UInt64,
                datafusion::arrow::array::UInt64Array,
                "UInt64Array"
            )
        }
        DataType::Float32 => {
            handle_array_type!(
                base_array,
                i,
                Float32,
                datafusion::arrow::array::Float32Array,
                "Float32Array"
            )
        }
        DataType::Float64 => {
            handle_array_type!(
                base_array,
                i,
                Float64,
                datafusion::arrow::array::Float64Array,
                "Float64Array"
            )
        }
        _ => unreachable!(),
    };
    Ok(string_val)
}

fn extract_value(base_array: &dyn Array, i: usize) -> DFResult<String> {
    match base_array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            extract_string_value(base_array, i)
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => extract_numeric_string_value(base_array, i),
        _ => unreachable!(),
    }
}

fn process_string_arrays(
    base_array: &dyn Array,
    start_array: &datafusion::arrow::array::Int64Array,
    length_array: Option<&datafusion::arrow::array::Int64Array>,
) -> DFResult<Arc<dyn Array>> {
    let mut result_builder = StringBuilder::new();

    for i in 0..base_array.len() {
        if base_array.is_null(i) || start_array.is_null(i) {
            result_builder.append_null();
            continue;
        }

        if let Some(length_arr) = &length_array
            && length_arr.is_null(i)
        {
            result_builder.append_null();
            continue;
        }

        let string_val = extract_value(base_array, i)?;
        let start_val = start_array.value(i);
        let length_val = length_array.as_ref().map(|arr| arr.value(i));

        if let Some(length_val) = length_val
            && length_val < 0
        {
            return NegativeSubstringLengthSnafu {
                function_name: "substr".to_string(),
                start: start_val,
                length: length_val,
            }
            .fail()?;
        }

        let length_u64 = length_val.and_then(|l| u64::try_from(l).ok());
        let result = compute_substr_string(&string_val, start_val, length_u64);
        result_builder.append_value(result);
    }

    Ok(Arc::new(result_builder.finish()))
}

fn process_binary_arrays(
    base_array: &dyn Array,
    start_array: &datafusion::arrow::array::Int64Array,
    length_array: Option<&datafusion::arrow::array::Int64Array>,
) -> DFResult<Arc<dyn Array>> {
    use datafusion::arrow::array::BinaryBuilder;

    let mut result_builder = BinaryBuilder::new();

    for i in 0..base_array.len() {
        if base_array.is_null(i) || start_array.is_null(i) {
            result_builder.append_null();
            continue;
        }

        if let Some(length_arr) = &length_array
            && length_arr.is_null(i)
        {
            result_builder.append_null();
            continue;
        }

        let binary_val: Vec<u8> = match base_array.data_type() {
            DataType::Binary => {
                handle_binary_array_type!(
                    base_array,
                    i,
                    datafusion::arrow::array::BinaryArray,
                    "BinaryArray"
                )
            }
            DataType::LargeBinary => {
                handle_binary_array_type!(
                    base_array,
                    i,
                    datafusion::arrow::array::LargeBinaryArray,
                    "LargeBinaryArray"
                )
            }
            _ => unreachable!(),
        };

        let start_val = start_array.value(i);
        let length_val = length_array.as_ref().map(|arr| arr.value(i));

        if let Some(length_val) = length_val
            && length_val < 0
        {
            return NegativeSubstringLengthSnafu {
                function_name: "substr".to_string(),
                start: start_val,
                length: length_val,
            }
            .fail()?;
        }

        let length_u64 = length_val.and_then(|l| u64::try_from(l).ok());

        let result = compute_substr_binary(&binary_val, start_val, length_u64);
        result_builder.append_value(&result);
    }

    Ok(Arc::new(result_builder.finish()))
}

fn process_large_binary_arrays(
    base_array: &dyn Array,
    start_array: &datafusion::arrow::array::Int64Array,
    length_array: Option<&datafusion::arrow::array::Int64Array>,
) -> DFResult<Arc<dyn Array>> {
    use datafusion::arrow::array::LargeBinaryBuilder;

    let mut result_builder = LargeBinaryBuilder::new();

    for i in 0..base_array.len() {
        if base_array.is_null(i) || start_array.is_null(i) {
            result_builder.append_null();
            continue;
        }

        if let Some(length_arr) = &length_array
            && length_arr.is_null(i)
        {
            result_builder.append_null();
            continue;
        }

        let binary_val: Vec<u8> = match base_array.data_type() {
            DataType::LargeBinary => {
                handle_binary_array_type!(
                    base_array,
                    i,
                    datafusion::arrow::array::LargeBinaryArray,
                    "LargeBinaryArray"
                )
            }
            _ => unreachable!(),
        };

        let start_val = start_array.value(i);
        let length_val = length_array.as_ref().map(|arr| arr.value(i));

        if let Some(length_val) = length_val
            && length_val < 0
        {
            return NegativeSubstringLengthSnafu {
                function_name: "substr".to_string(),
                start: start_val,
                length: length_val,
            }
            .fail()?;
        }

        let length_u64 = length_val.and_then(|l| u64::try_from(l).ok());

        let result = compute_substr_binary(&binary_val, start_val, length_u64);
        result_builder.append_value(&result);
    }

    Ok(Arc::new(result_builder.finish()))
}

fn substr_snowflake(
    base_array: &dyn Array,
    start_array: &dyn Array,
    length_array: Option<&dyn Array>,
) -> DFResult<Arc<dyn Array>> {
    let start_array = as_int64_array(start_array)?;
    let length_array = if let Some(arr) = length_array {
        Some(as_int64_array(arr)?)
    } else {
        None
    };

    match base_array.data_type() {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => process_string_arrays(base_array, start_array, length_array),
        DataType::Binary => process_binary_arrays(base_array, start_array, length_array),
        DataType::LargeBinary => process_large_binary_arrays(base_array, start_array, length_array),
        other => UnsupportedDataTypeSnafu {
            function_name: "substr".to_string(),
            data_type: format!("{other:?}"),
            expected_types: "string or binary coercible types".to_string(),
        }
        .fail()?,
    }
}

crate::macros::make_udf_function!(SubstrFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_substr_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(SubstrFunc::new()));

        let q = "SELECT substr('mystring', 3, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| st     |",
                "+--------+"
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_substr_negative_index() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(SubstrFunc::new()));

        let q = "SELECT substr('mystring', -1, 3) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| g      |",
                "+--------+"
            ],
            &result
        );

        let q = "SELECT substr('mystring', -3, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| in     |",
                "+--------+"
            ],
            &result
        );

        let q = "SELECT substr('mystring', -2, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| ng     |",
                "+--------+"
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_substr_edge_cases() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(SubstrFunc::new()));

        let q = "SELECT substr(NULL, 1, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "|        |",
                "+--------+"
            ],
            &result
        );

        let q = "SELECT substr('abc', 0, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| ab     |",
                "+--------+"
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_substr_numeric_types() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(SubstrFunc::new()));

        let q = "SELECT substr(1.23, -2, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| 23     |",
                "+--------+"
            ],
            &result
        );

        let q = "SELECT substr(12345, 2, 3) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| 234    |",
                "+--------+"
            ],
            &result
        );

        let q = "SELECT substr(-567, -2, 2) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| 67     |",
                "+--------+"
            ],
            &result
        );

        let q = "SELECT substr(123.456, 3, 4) as result;";
        let result = ctx.sql(q).await?.collect().await?;
        assert_batches_eq!(
            &[
                "+--------+",
                "| result |",
                "+--------+",
                "| 3.45   |",
                "+--------+"
            ],
            &result
        );

        Ok(())
    }

    #[test]
    fn test_compute_substr_string_direct() {
        let result = compute_substr_string("mystring", -2, Some(2));
        assert_eq!(result, "ng", "substr('mystring', -2, 2) should return 'ng'");

        let result = compute_substr_string("mystring", -1, Some(1));
        assert_eq!(result, "g", "substr('mystring', -1, 1) should return 'g'");

        let result = compute_substr_string("mystring", -3, Some(2));
        assert_eq!(result, "in", "substr('mystring', -3, 2) should return 'in'");

        let result = compute_substr_string("mystring", -8, Some(3));
        assert_eq!(
            result, "mys",
            "substr('mystring', -8, 3) should return 'mys'"
        );

        let result = compute_substr_string("mystring", -2, Some(3));
        assert_eq!(
            result, "ng",
            "substr('mystring', -2, 3) should return 'ng' (limited by string end)"
        );

        let result = compute_substr_string("hello", -2, Some(2));
        assert_eq!(result, "lo", "substr('hello', -2, 2) should return 'lo'");

        let result = compute_substr_string("1.23", -2, Some(2));
        assert_eq!(result, "23", "substr('1.23', -2, 2) should return '23'");

        let result = compute_substr_string("12345", 2, Some(3));
        assert_eq!(result, "234", "substr('12345', 2, 3) should return '234'");
    }
}
