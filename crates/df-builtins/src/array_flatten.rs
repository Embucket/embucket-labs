use datafusion::arrow::array::as_string_array;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::array::StringBuilder;
use datafusion_common::{DataFusionError, ScalarValue, exec_err, internal_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::{Map, Value};
use std::any::Any;
use std::sync::Arc;

// array_flatten SQL function
// Transforms a nested ARRAY (an ARRAY of ARRAYs) into a single, flat ARRAY by combining all inner ARRAYs into one continuous sequence.
//
// Syntax: ARRAY_FLATTEN( <array_expr> )
//
// Arguments:
// - <array_expr>
//   The ARRAY of ARRAYs to flatten.
//   If any element of array is not an ARRAY, the function reports an error.
//
// Returns:
// This function produces a single ARRAY by joining together all the ARRAYs within the input array. If the input array is NULL or contains any NULL elements, the function returns NULL.
#[derive(Debug)]
pub struct ArrayFlattenFunc {
    signature: Signature,
}

impl Default for ArrayFlattenFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFlattenFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayFlattenFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let mut res = StringBuilder::with_capacity(arr.len(), 1024);

                let arr = as_string_array(arr);
                for v in arr {
                    if let Some(v) = v {
                        res.append_option(flatten(v)?);
                    } else {
                        res.append_null();
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(res.finish())))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(if let Some(v) = v { flatten(v)? } else { None }),
            )),
            ColumnarValue::Scalar(_) => internal_err!("array_flatten function requires a string"),
        }
    }
}

fn flatten_inner(v: Value) -> DFResult<Value> {
    Ok(match v {
        Value::Null => Value::String("undefined".to_string()),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => v,
        Value::Array(arr) => {
            let mut res = vec![];

            for item in arr {
                res.push(flatten_inner(item)?);
            }

            Value::Array(res)
        }
        Value::Object(m) => {
            let mut res: Map<String, Value> = Map::with_capacity(m.len());
            for (k, v) in m {
                res.insert(k, flatten_inner(v)?);
            }

            Value::Object(res)
        }
    })
}

fn flatten(v: &str) -> DFResult<Option<String>> {
    let v = v.to_lowercase(); // normalize to lowercase so NULL become null
    let v = serde_json::from_str::<Value>(&v).map_err(|err| {
        DataFusionError::Execution(format!("failed to deserialize JSON: {err:?}"))
    })?;

    let v = match v {
        Value::Array(arr) => {
            let mut result = Vec::new();
            for item in arr {
                match item {
                    Value::Null => return Ok(None),
                    Value::Array(inner) => {
                        for subitem in inner {
                            result.push(flatten_inner(subitem)?);
                        }
                    }
                    _ => {
                        return exec_err!(
                            "Not an array: 'Input argument to ARRAY_FLATTEN is not an array of arrays'"
                        );
                    }
                }
            }
            Value::Array(result)
        }
        _ => {
            return exec_err!(
                "Not an array: 'Input argument to ARRAY_FLATTEN is not an array of arrays'"
            );
        }
    };

    Ok(Some(serde_json::to_string(&v).map_err(|err| {
        DataFusionError::Execution(format!("failed to serialize JSON: {err:?}"))
    })?))
}

super::macros::make_udf_function!(ArrayFlattenFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_scalar() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ArrayFlattenFunc::new()));
        let q = "SELECT ARRAY_FLATTEN('[ [ [1, 2], [3] ], [ [4], [5] ] ]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| v                   |",
                "+---------------------+",
                "| [[1,2],[3],[4],[5]] |",
                "+---------------------+",
            ],
            &result
        );

        let q = "SELECT ARRAY_FLATTEN('[[1, 2, 3], [4], [5, 6]]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------+",
                "| v             |",
                "+---------------+",
                "| [1,2,3,4,5,6] |",
                "+---------------+",
            ],
            &result
        );

        let q = "SELECT ARRAY_FLATTEN('[[[1, 2], [3]], [[4], [5]]]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| v                   |",
                "+---------------------+",
                "| [[1,2],[3],[4],[5]] |",
                "+---------------------+",
            ],
            &result
        );

        let q = "SELECT ARRAY_FLATTEN('[[1, 2, 3], 4, [5, 6]]') as v;";
        assert!(ctx.sql(q).await?.collect().await.is_err());

        let q = "SELECT ARRAY_FLATTEN('[[1, 2, 3], null, [5, 6]]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        let q = "SELECT ARRAY_FLATTEN('[[1, 2, 3], [NULL], [5, 6]]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------+",
                "| v                       |",
                "+-------------------------+",
                "| [1,2,3,\"undefined\",5,6] |",
                "+-------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(ArrayFlattenFunc::new()));
        ctx.sql("CREATE TABLE t(v STRING)").await?;
        ctx.sql("INSERT INTO t (v) VALUES('[ [ [1, 2], [3] ], [ [4], [5] ] ]')")
            .await?
            .collect()
            .await?;
        let q = "SELECT ARRAY_FLATTEN(v) from t;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------+",
                "| array_flatten(t.v)  |",
                "+---------------------+",
                "| [[1,2],[3],[4],[5]] |",
                "+---------------------+",
            ],
            &result
        );

        Ok(())
    }
}
