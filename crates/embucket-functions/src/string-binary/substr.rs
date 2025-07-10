use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{ScalarValue, exec_err, plan_err};
use datafusion_expr::Expr;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use std::any::Any;
use std::sync::Arc;

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

    fn name(&self) -> &str {
        "substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return plan_err!(
                "The {} function requires 2 or 3 arguments, but got {}.",
                self.name(),
                arg_types.len()
            );
        }

        let first_data_type = match &arg_types[0] {
            DataType::Null => Ok(DataType::Utf8),
            DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => Ok(arg_types[0].clone()),
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer() {
                    match value_type.as_ref() {
                        DataType::Null => Ok(DataType::Utf8),
                        DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => {
                            Ok(*value_type.clone())
                        }
                        _ => plan_err!(
                            "The first argument of the {} function can only be a string, but got {:?}.",
                            self.name(),
                            arg_types[0]
                        ),
                    }
                } else {
                    plan_err!(
                        "The first argument of the {} function can only be a string, but got {:?}.",
                        self.name(),
                        arg_types[0]
                    )
                }
            }
            _ => plan_err!(
                "The first argument of the {} function can only be a string, but got {:?}.",
                self.name(),
                arg_types[0]
            ),
        }?;

        if ![DataType::Int64, DataType::Int32, DataType::Null].contains(&arg_types[1]) {
            return plan_err!(
                "The second argument of the {} function can only be an integer, but got {:?}.",
                self.name(),
                arg_types[1]
            );
        }

        if arg_types.len() == 3
            && ![DataType::Int64, DataType::Int32, DataType::Null].contains(&arg_types[2])
        {
            return plan_err!(
                "The third argument of the {} function can only be an integer, but got {:?}.",
                self.name(),
                arg_types[2]
            );
        }

        if arg_types.len() == 2 {
            Ok(vec![first_data_type.to_owned(), DataType::Int64])
        } else {
            Ok(vec![
                first_data_type.to_owned(),
                DataType::Int64,
                DataType::Int64,
            ])
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() < 2 || args.len() > 3 {
            return exec_err!(
                "The {} function requires 2 or 3 arguments, but got {}.",
                self.name(),
                args.len()
            );
        }

        let string_arg = &args[0];
        let start_arg = &args[1];
        let length_arg = if args.len() == 3 {
            Some(&args[2])
        } else {
            None
        };

        let string_array = match string_arg {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let start_array = match start_arg {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let length_array = if let Some(length_arg) = length_arg {
            Some(match length_arg {
                ColumnarValue::Array(array) => Arc::clone(array),
                ColumnarValue::Scalar(scalar) => scalar.to_array()?,
            })
        } else {
            None
        };

        let result = substr_snowflake(&string_array, &start_array, length_array.as_deref())?;
        Ok(ColumnarValue::Array(result))
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

                if let (Some(string_val), Some(start_val)) = (
                    string_scalar.to_string().parse::<String>().ok(),
                    start_scalar.to_string().parse::<i64>().ok(),
                ) {
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

                    if let Some(result) = compute_snowflake_substr(
                        &string_val,
                        start_val,
                        length_val.map(|l| l as u64),
                    ) {
                        return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                            ScalarValue::Utf8View(Some(result)),
                        )));
                    }
                }
            }
        }

        Ok(ExprSimplifyResult::Original(args))
    }
}

fn compute_snowflake_substr(input: &str, start: i64, length: Option<u64>) -> Option<String> {
    if input.is_empty() {
        return Some(String::new());
    }

    let char_count = input.chars().count() as i64;

    let actual_start = if start < 0 {
        char_count + start + 1
    } else if start == 0 {
        1
    } else {
        start
    };

    if actual_start <= 0 || actual_start > char_count {
        return Some(String::new());
    }

    let start_idx = (actual_start - 1) as usize;

    let chars: Vec<char> = input.chars().collect();

    let end_idx = if let Some(len) = length {
        if len == 0 {
            return Some(String::new());
        }
        std::cmp::min(start_idx + len as usize, chars.len())
    } else {
        chars.len()
    };

    if start_idx >= chars.len() {
        return Some(String::new());
    }

    Some(chars[start_idx..end_idx].iter().collect())
}

fn substr_snowflake(
    string_array: &dyn Array,
    start_array: &dyn Array,
    length_array: Option<&dyn Array>,
) -> DFResult<Arc<dyn Array>> {
    match string_array.data_type() {
        DataType::Utf8 => {
            let string_array = match string_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>() {
                Some(arr) => arr,
                None => return datafusion_common::exec_err!("Expected StringArray"),
            };

            let start_array = as_int64_array(start_array)?;
            let length_array = if let Some(arr) = length_array {
                Some(as_int64_array(arr)?)
            } else {
                None
            };

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

                let string_val = string_array.value(i);
                let start_val = start_array.value(i);
                let length_val = length_array.as_ref().map(|arr| arr.value(i));

                if let Some(length_val) = length_val {
                    if length_val < 0 {
                        return exec_err!(
                            "negative substring length not allowed: substr(<str>, {start_val}, {length_val})"
                        );
                    }
                }

                let length_u64 = length_val.map(|l| l as u64);

                if let Some(result) = compute_snowflake_substr(string_val, start_val, length_u64) {
                    result_builder.append_value(result);
                } else {
                    result_builder.append_null();
                }
            }

            Ok(Arc::new(result_builder.finish()))
        }
        DataType::LargeUtf8 => {
            let string_array = match string_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeStringArray>() {
                Some(arr) => arr,
                None => return datafusion_common::exec_err!("Expected LargeStringArray"),
            };

            let start_array = as_int64_array(start_array)?;
            let length_array = if let Some(arr) = length_array {
                Some(as_int64_array(arr)?)
            } else {
                None
            };

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

                let string_val = string_array.value(i);
                let start_val = start_array.value(i);
                let length_val = length_array.as_ref().map(|arr| arr.value(i));

                if let Some(length_val) = length_val {
                    if length_val < 0 {
                        return exec_err!(
                            "negative substring length not allowed: substr(<str>, {start_val}, {length_val})"
                        );
                    }
                }

                let length_u64 = length_val.map(|l| l as u64);

                if let Some(result) = compute_snowflake_substr(string_val, start_val, length_u64) {
                    result_builder.append_value(result);
                } else {
                    result_builder.append_null();
                }
            }

            Ok(Arc::new(result_builder.finish()))
        }
        DataType::Utf8View => {
            let string_array = match string_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>() {
                Some(arr) => arr,
                None => return datafusion_common::exec_err!("Expected StringViewArray"),
            };

            let start_array = as_int64_array(start_array)?;
            let length_array = if let Some(arr) = length_array {
                Some(as_int64_array(arr)?)
            } else {
                None
            };

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

                let string_val = string_array.value(i);
                let start_val = start_array.value(i);
                let length_val = length_array.as_ref().map(|arr| arr.value(i));

                if let Some(length_val) = length_val {
                    if length_val < 0 {
                        return exec_err!(
                            "negative substring length not allowed: substr(<str>, {start_val}, {length_val})"
                        );
                    }
                }

                let length_u64 = length_val.map(|l| l as u64);

                if let Some(result) = compute_snowflake_substr(string_val, start_val, length_u64) {
                    result_builder.append_value(result);
                } else {
                    result_builder.append_null();
                }
            }

            Ok(Arc::new(result_builder.finish()))
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function substr, expected Utf8 or LargeUtf8."
        ),
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
}
