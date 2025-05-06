use crate::execution::datafusion::functions::booland::BoolAndFunc;
use arrow_array::builder::BooleanBuilder;
use arrow_array::cast::{as_string_array, downcast_array};
use arrow_array::{Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow_schema::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::cast::{
    as_boolean_array, as_int16_array, as_int32_array, as_int64_array, as_int8_array,
};
use datafusion_common::{downcast_value, DataFusionError};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;
use crate::execution::datafusion::functions::array_to_boolean;

// to_boolean SQL function
// Converts the input text or numeric expression to a BOOLEAN value.
// Syntax: TO_BOOLEAN( <string_or_numeric_expr> )
// Example SELECT TO_BOOLEAN('true');
// Note `to_boolean` returns
// Returns a BOOLEAN value or NULL.
// - Returns TRUE if string_or_numeric_expr evaluates to TRUE.
// - Returns FALSE if string_or_numeric_expr evaluates to FALSE.
// If the input is NULL, returns NULL without reporting an error.
#[derive(Debug)]
pub struct ToBooleanFunc {
    signature: Signature,
}

impl Default for ToBooleanFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToBooleanFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Numeric(1),
                    TypeSignature::String(1),
                    TypeSignature::Exact(vec![DataType::Boolean]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToBooleanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_boolean"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        assert_eq!(args.args.len(), 1);

        let ScalarFunctionArgs { args, .. } = args;

        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let arr = match arr.data_type() {
            DataType::Utf8 => {
                let arr = as_string_array(&arr);
                let mut res = BooleanBuilder::with_capacity(arr.len());
                let true_ = ["true", "t", "yes", "y", "on", "1"];
                let false_ = ["false", "f", "no", "n", "off", "0"];
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        res.append_null();
                    } else {
                        let v = arr.value(i);
                        if true_.iter().any(|&s| s.eq_ignore_ascii_case(v)) {
                            res.append_value(true);
                        } else if false_.iter().any(|&s| s.eq_ignore_ascii_case(v)) {
                            res.append_value(false);
                        } else {
                            return Err(DataFusionError::Internal(format!(
                                "Invalid boolean string: {}",
                                v
                            )));
                        }
                    }
                }
                res.finish()
            }
            _=> {
                array_to_boolean(&arr)?
            }
        };

        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

super::macros::make_udf_function!(ToBooleanFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::print_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_boolean() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new()));
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
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new()));
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
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new()));
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
        ctx.register_udf(ScalarUDF::from(ToBooleanFunc::new()));
        let q = "SELECT TO_BOOLEAN(true)";
        let result = ctx.sql(q).await?.collect().await?;

        print_batches(&result)?;
        Ok(())
    }
}
