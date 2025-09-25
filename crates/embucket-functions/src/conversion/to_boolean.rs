use super::errors as conv_errors;
use crate::array_to_boolean;
use crate::utils::to_string_array;
use datafusion::arrow::array::builder::BooleanBuilder;
use datafusion::arrow::array::cast::as_string_array;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

const TRUE: [&str; 6] = ["true", "t", "yes", "y", "on", "1"];
const FALSE: [&str; 6] = ["false", "f", "no", "n", "off", "0"];

// to_boolean SQL function
// Converts the input text or numeric expression to a BOOLEAN value.
// Syntax: TO_BOOLEAN( <string_or_numeric_expr> )
// Example SELECT TO_BOOLEAN('true');
// Note `to_boolean` returns
// Returns a BOOLEAN value or NULL.
// - Returns TRUE if string_or_numeric_expr evaluates to TRUE.
// - Returns FALSE if string_or_numeric_expr evaluates to FALSE.
// If the input is NULL, returns NULL without reporting an error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToBooleanFunc {
    signature: Signature,
    try_: bool,
}

impl Default for ToBooleanFunc {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ToBooleanFunc {
    #[must_use]
    pub fn new(try_: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Numeric(1),
                    TypeSignature::String(1),
                    TypeSignature::Exact(vec![DataType::Boolean]),
                ],
                Volatility::Immutable,
            ),
            try_,
        }
    }
}

impl ScalarUDFImpl for ToBooleanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.try_ {
            "try_to_boolean"
        } else {
            "to_boolean"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let arr = match arr.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                // Normalize any string-like input to Utf8 and parse textual booleans
                let arr: &StringArray = &to_string_array(&arr)?;
                let arr = as_string_array(&arr);
                let mut res = BooleanBuilder::with_capacity(arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        res.append_null();
                    } else {
                        let v = arr.value(i);
                        if TRUE.iter().any(|&s| s.eq_ignore_ascii_case(v)) {
                            res.append_value(true);
                        } else if FALSE.iter().any(|&s| s.eq_ignore_ascii_case(v)) {
                            res.append_value(false);
                        } else if self.try_ {
                            res.append_null();
                        } else {
                            return conv_errors::InvalidBooleanStringSnafu { v }.fail()?;
                        }
                    }
                }
                res.finish()
            }
            _ => array_to_boolean(&arr)?,
        };

        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_boolean() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new(false)));
        let q = "CREATE OR REPLACE TABLE test_boolean(b boolean);";
        ctx.sql(q).await?.collect().await?;

        let q = "INSERT INTO test_boolean VALUES (true), (false), (null);";
        ctx.sql(q).await?.collect().await?;

        let q = "SELECT b, TO_BOOLEAN(b) FROM test_boolean;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+----------------------------+",
                "| b     | to_boolean(test_boolean.b) |",
                "+-------+----------------------------+",
                "| true  | true                       |",
                "| false | false                      |",
                "|       |                            |",
                "+-------+----------------------------+"
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_number() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new(false)));
        let q = "CREATE OR REPLACE TABLE test_boolean(i integer);";
        ctx.sql(q).await?.collect().await?;

        let q = "INSERT INTO test_boolean VALUES (1), (0), (null);";
        ctx.sql(q).await?.collect().await?;

        let q = "SELECT i, TO_BOOLEAN(i) FROM test_boolean;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---+----------------------------+",
                "| i | to_boolean(test_boolean.i) |",
                "+---+----------------------------+",
                "| 1 | true                       |",
                "| 0 | false                      |",
                "|   |                            |",
                "+---+----------------------------+"
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_string() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new(false)));
        let q = "CREATE OR REPLACE TABLE test_boolean(s STRING);";
        ctx.sql(q).await?.collect().await?;

        let q = "INSERT INTO test_boolean VALUES ('yes'), ('no'), (null);";
        ctx.sql(q).await?.collect().await?;

        let q = "SELECT s, TO_BOOLEAN(s) FROM test_boolean;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----+----------------------------+",
                "| s   | to_boolean(test_boolean.s) |",
                "+-----+----------------------------+",
                "| yes | true                       |",
                "| no  | false                      |",
                "|     |                            |",
                "+-----+----------------------------+"
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_to_boolean_scalar() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new(false)));
        let q = "SELECT TO_BOOLEAN(true)";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------+",
                "| to_boolean(Boolean(true)) |",
                "+---------------------------+",
                "| true                      |",
                "+---------------------------+",
            ],
            &result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_try_string() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new(true)));
        let q = "CREATE OR REPLACE TABLE test_boolean(s STRING);";
        ctx.sql(q).await?.collect().await?;

        let q = "INSERT INTO test_boolean VALUES ('invalid');";
        ctx.sql(q).await?.collect().await?;

        let q = "SELECT s, TRY_TO_BOOLEAN(s) FROM test_boolean;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+--------------------------------+",
                "| s       | try_to_boolean(test_boolean.s) |",
                "+---------+--------------------------------+",
                "| invalid |                                |",
                "+---------+--------------------------------+"
            ],
            &result
        );
        Ok(())
    }
}
