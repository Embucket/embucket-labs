use datafusion::arrow::array::{ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::error::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::StringArray;
use datafusion_common::cast::as_generic_string_array;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

// strtok_to_array SQL function
// Tokenizes the given string using the given set of delimiters and returns the tokens as an ARRAY value.
// Syntax: STRTOK_TO_ARRAY( <string> [ , <delimiter> ] )
// Arguments:
// Required: <string> Text to be tokenized.
// Optional: <delimiter> Set of delimiters. Default: A single space character.
// Note: `strtok_to_array` returns
// This function returns a value of type ARRAY or NULL.
// The function returns an empty array if tokenization produces no tokens.
// If either argument is a NULL or JSON null value, the function returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StrtokToArrayFunc {
    signature: Signature,
}

impl Default for StrtokToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StrtokToArrayFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(1), TypeSignature::String(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StrtokToArrayFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "strtok_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let lhs = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let rhs = if args.len() == 2 {
            match &args[1] {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(v) => &v.to_array()?,
            }
        } else {
            &ScalarValue::Utf8(Some(String::from(" "))).to_array()?
        };

        let strs: &StringArray = as_generic_string_array(&lhs)?;
        let delms: &StringArray = as_generic_string_array(&rhs)?;

        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder);

        for (string, delimiter) in strs.iter().zip(delms.iter()) {
            if string.is_none() || delimiter.is_none() {
                list_builder.append_null();
                continue;
            }

            let string = string.unwrap();
            let delimiter = delimiter.unwrap();

            if is_json_null(string) || is_json_null(delimiter) {
                list_builder.append(false);
                continue;
            }

            let delimiter_set: HashSet<char> = delimiter.chars().collect();
            let mut last_split_index: usize = 0;
            for (i, ch) in string.char_indices() {
                if delimiter_set.contains(&ch) {
                    let value = &string[last_split_index..i];
                    if !value.is_empty() {
                        list_builder.values().append_value(value);
                    }
                    last_split_index = i + ch.len_utf8();
                }
            }

            let tail = &string[last_split_index..];
            if !tail.is_empty() {
                list_builder.values().append_value(tail);
            }

            list_builder.append(true);
        }

        Ok(ColumnarValue::Array(Arc::new(list_builder.finish())))
    }
}

fn is_json_null(s: &str) -> bool {
    matches!(s.trim().to_lowercase().as_str(), "null" | "[ null ]")
}

crate::macros::make_udf_function!(StrtokToArrayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semi_structured::parse_json::ParseJsonFunc;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, assert_batches_eq};
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StrtokToArrayFunc::new()));
        ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new(false)));

        let q = "SELECT STRTOK_TO_ARRAY('a.b.c', '.') AS string_to_array;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------+",
                "| string_to_array |",
                "+-----------------+",
                "| [a, b, c]       |",
                "+-----------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK_TO_ARRAY('user@snowflake.com', '.@') AS multiple_delimiters;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------------+",
                "| multiple_delimiters    |",
                "+------------------------+",
                "| [user, snowflake, com] |",
                "+------------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK_TO_ARRAY('a b c') AS default_delimeter;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------+",
                "| default_delimeter |",
                "+-------------------+",
                "| [a, b, c]         |",
                "+-------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK_TO_ARRAY(NULL, '.') AS null_result;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------+",
                "| null_result |",
                "+-------------+",
                "|             |",
                "+-------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK_TO_ARRAY(PARSE_JSON('[ null ]'), '.') AS json_null;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| json_null |",
                "+-----------+",
                "| [[null]]  |",
                "+-----------+",
            ],
            &result
        );

        let q = "SELECT STRTOK_TO_ARRAY('abc', NULL) AS json_null;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------+",
                "| json_null |",
                "+-----------+",
                "|           |",
                "+-----------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_no_tokens_returns_empty_array() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StrtokToArrayFunc::new()));

        let q = "SELECT STRTOK_TO_ARRAY('', '.') AS empty_string;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------+",
                "| empty_string |",
                "+--------------+",
                "| []           |",
                "+--------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK_TO_ARRAY('...', '.') AS only_delimiters;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------+",
                "| only_delimiters |",
                "+-----------------+",
                "| []              |",
                "+-----------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_argument_length_fails() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StrtokToArrayFunc::new()));

        // Zero arguments
        let q = "SELECT STRTOK_TO_ARRAY();";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => assert!(
                    matches!(e, DataFusionError::Execution(_)),
                    "Expected Execution error for 0 args, got: {e}",
                ),
                Ok(_) => panic!("Expected error but query with 0 args succeeded"),
            }
        }

        // Too many arguments
        let q = "SELECT STRTOK_TO_ARRAY('a b c', ' ', '!');";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => assert!(
                    matches!(e, DataFusionError::Execution(_)),
                    "Expected Execution error for 3 args, got: {e}",
                ),
                Ok(_) => panic!("Expected error but query with 3 args succeeded"),
            }
        }
    }
}
