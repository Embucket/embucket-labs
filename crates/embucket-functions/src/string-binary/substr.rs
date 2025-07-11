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

///
///
///
/// Arguments:
///
///
/// Returns:
#[derive(Debug)]
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

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
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

        let string_array = &arrays[0];
        let start_array = &arrays[1];
        let length_array = if arrays.len() == 3 {
            Some(arrays[2].as_ref())
        } else {
            None
        };

        let result = substr_snowflake(string_array, start_array, length_array)?;
        Ok(ColumnarValue::Array(result))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> DFResult<ExprSimplifyResult> {
        if args.len() >= 2 && args.len() <= 3 {
            if let (Expr::Literal(string_scalar), Expr::Literal(start_scalar)) =
                (&args[0], &args[1])
            {
                if string_scalar.is_null() || start_scalar.is_null() {
                    return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                        ScalarValue::Null,
                    )));
                }

                let string_val = string_scalar.to_string();
                if let Some(start_val) = start_scalar.to_string().parse::<i64>().ok() {
                    let length_val = if args.len() == 3 {
                        if let Expr::Literal(length_scalar) = &args[2] {
                            if length_scalar.is_null() {
                                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                                    ScalarValue::Null,
                                )));
                            }
                            length_scalar.to_string().parse::<i64>().ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let result = compute_snowflake_substr(
                        &string_val,
                        start_val,
                        length_val.and_then(|l| u64::try_from(l).ok()),
                    );
                    return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                        ScalarValue::Utf8View(Some(result)),
                    )));
                }
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
            DataType::Null => DataType::Utf8,
            DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => arg_types[0].clone(),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64 => DataType::Utf8,
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer() {
                    match value_type.as_ref() {
                        DataType::Null => DataType::Utf8,
                        DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => {
                            *value_type.clone()
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
                        | DataType::Float64 => DataType::Utf8,
                        _ => {
                            return InvalidArgumentTypeSnafu {
                                function_name: self.name().to_string(),
                                position: "first".to_string(),
                                expected_type: "a string coercible type".to_string(),
                                actual_type: format!("{:?}", arg_types[0]),
                            }
                            .fail()?;
                        }
                    }
                } else {
                    return InvalidArgumentTypeSnafu {
                        function_name: self.name().to_string(),
                        position: "first".to_string(),
                        expected_type: "a string coercible type".to_string(),
                        actual_type: format!("{:?}", arg_types[0]),
                    }
                    .fail()?;
                }
            }
            _ => {
                return InvalidArgumentTypeSnafu {
                    function_name: self.name().to_string(),
                    position: "first".to_string(),
                    expected_type: "a string coercible type".to_string(),
                    actual_type: format!("{:?}", arg_types[0]),
                }
                .fail()?;
            }
        };

        if ![DataType::Int64, DataType::Int32, DataType::Null].contains(&arg_types[1]) {
            return InvalidArgumentTypeSnafu {
                function_name: self.name().to_string(),
                position: "second".to_string(),
                expected_type: "an integer".to_string(),
                actual_type: format!("{:?}", arg_types[1]),
            }
            .fail()?;
        }

        if arg_types.len() == 3
            && ![DataType::Int64, DataType::Int32, DataType::Null].contains(&arg_types[2])
        {
            return InvalidArgumentTypeSnafu {
                function_name: self.name().to_string(),
                position: "third".to_string(),
                expected_type: "an integer".to_string(),
                actual_type: format!("{:?}", arg_types[2]),
            }
            .fail()?;
        }

        if arg_types.len() == 2 {
            Ok(vec![first_data_type, DataType::Int64])
        } else {
            Ok(vec![first_data_type, DataType::Int64, DataType::Int64])
        }
    }
}

fn compute_snowflake_substr(input: &str, start: i64, length: Option<u64>) -> String {
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

fn process_arrays(
    string_array: &dyn Array,
    start_array: &datafusion::arrow::array::Int64Array,
    length_array: Option<&datafusion::arrow::array::Int64Array>,
) -> DFResult<Arc<dyn Array>> {
    let mut result_builder = StringBuilder::new();

    for i in 0..string_array.len() {
        if string_array.is_null(i) || start_array.is_null(i) {
            result_builder.append_null();
            continue;
        }

        if let Some(length_arr) = &length_array {
            if length_arr.is_null(i) {
                result_builder.append_null();
                continue;
            }
        }

        let string_val: String = match string_array.data_type() {
            DataType::Utf8 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "StringArray".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::LargeUtf8 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "LargeStringArray".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Utf8View => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "StringViewArray".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Int8 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int8Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "Int8Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Int16 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int16Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "Int16Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Int32 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "Int32Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Int64 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "Int64Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::UInt8 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt8Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "UInt8Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::UInt16 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt16Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "UInt16Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::UInt32 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt32Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "UInt32Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::UInt64 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "UInt64Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Float32 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float32Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "Float32Array".to_string(),
                    }
                    .fail()?;
                }
            }
            DataType::Float64 => {
                if let Some(arr) = string_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Float64Array>()
                {
                    arr.value(i).to_string()
                } else {
                    return FailedToDowncastSnafu {
                        array_type: "Float64Array".to_string(),
                    }
                    .fail()?;
                }
            }
            _ => unreachable!(),
        };

        let start_val = start_array.value(i);
        let length_val = length_array.as_ref().map(|arr| arr.value(i));

        if let Some(length_val) = length_val {
            if length_val < 0 {
                return NegativeSubstringLengthSnafu {
                    function_name: "substr".to_string(),
                    start: start_val,
                    length: length_val,
                }
                .fail()?;
            }
        }

        let length_u64 = length_val.and_then(|l| u64::try_from(l).ok());

        let result = compute_snowflake_substr(&string_val, start_val, length_u64);
        result_builder.append_value(result);
    }

    Ok(Arc::new(result_builder.finish()))
}

fn substr_snowflake(
    string_array: &dyn Array,
    start_array: &dyn Array,
    length_array: Option<&dyn Array>,
) -> DFResult<Arc<dyn Array>> {
    let start_array = as_int64_array(start_array)?;
    let length_array = if let Some(arr) = length_array {
        Some(as_int64_array(arr)?)
    } else {
        None
    };

    match string_array.data_type() {
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
        | DataType::Float64 => process_arrays(string_array, start_array, length_array),
        other => UnsupportedDataTypeSnafu {
            function_name: "substr".to_string(),
            data_type: format!("{:?}", other),
            expected_types: "string coercible types".to_string(),
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
    async fn test_snowflake_substr_basic() -> DFResult<()> {
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
    async fn test_snowflake_substr_negative_index() -> DFResult<()> {
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
    async fn test_snowflake_substr_edge_cases() -> DFResult<()> {
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
    async fn test_snowflake_substr_numeric_types() -> DFResult<()> {
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
    fn test_compute_snowflake_substr_direct() {
        let result = compute_snowflake_substr("mystring", -2, Some(2));
        assert_eq!(result, "ng", "substr('mystring', -2, 2) should return 'ng'");

        let result = compute_snowflake_substr("mystring", -1, Some(1));
        assert_eq!(result, "g", "substr('mystring', -1, 1) should return 'g'");

        let result = compute_snowflake_substr("mystring", -3, Some(2));
        assert_eq!(result, "in", "substr('mystring', -3, 2) should return 'in'");

        let result = compute_snowflake_substr("mystring", -8, Some(3));
        assert_eq!(
            result, "mys",
            "substr('mystring', -8, 3) should return 'mys'"
        );

        let result = compute_snowflake_substr("mystring", -2, Some(3));
        assert_eq!(
            result, "ng",
            "substr('mystring', -2, 3) should return 'ng' (limited by string end)"
        );

        let result = compute_snowflake_substr("hello", -2, Some(2));
        assert_eq!(result, "lo", "substr('hello', -2, 2) should return 'lo'");

        let result = compute_snowflake_substr("1.23", -2, Some(2));
        assert_eq!(result, "23", "substr('1.23', -2, 2) should return '23'");

        let result = compute_snowflake_substr("12345", 2, Some(3));
        assert_eq!(result, "234", "substr('12345', 2, 3) should return '234'");
    }
}
