use crate::json;
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::exec_err;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use regex::Regex;
use serde_json::{Map, Value};
use snafu::ResultExt;
use std::any::Any;
use std::sync::{Arc, LazyLock};

#[allow(clippy::unwrap_used)]
static RE_NULL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bnull\b").unwrap());

/// `array_flatten` SQL function
/// Transforms a nested ARRAY (an ARRAY of ARRAYs) into a single, flat ARRAY by combining all inner ARRAYs into one continuous sequence.
///
/// Syntax: `ARRAY_FLATTEN`( <`array_expr`> )
///
/// Arguments:
/// - <`array_expr`>
///   The ARRAY of ARRAYs to flatten.
///   If any element of array is not an ARRAY, the function reports an error.
///
/// Returns:
/// This function produces a single ARRAY by joining together all the ARRAYs within the input array. If the input array is NULL or contains any NULL elements, the function returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayFlattenFunc {
    signature: Signature,
}

impl Default for ArrayFlattenFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFlattenFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
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
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let arr = args[0].clone().into_array(number_rows)?;
        let json_array = json::encode_array(arr)?;

        let mut rows: Vec<Value> = Vec::with_capacity(number_rows);
        if let Value::Array(v) = &json_array {
            for item in v.iter().take(number_rows) {
                let mut row = item.clone();
                match &row {
                    Value::String(s) => {
                        // normalize to lowercase so NULL become null
                        let s = RE_NULL.replace_all(s, "null").into_owned();
                        if let Ok(parsed) = serde_json::from_str::<Value>(&s) {
                            row = parsed;
                        }
                        rows.push(flatten_inner(row)?);
                    }
                    Value::Null => rows.push(Value::Null),
                    _ => rows.push(flatten_inner(row)?),
                }
            }
        }

        let mut col_values: Vec<Option<String>> = Vec::with_capacity(number_rows);
        for val in rows {
            if val == Value::Null {
                col_values.push(None);
            } else {
                col_values.push(Some(
                    serde_json::to_string(&val).context(errors::FailedToSerializeValueSnafu)?,
                ));
            }
        }
        let array: StringArray = StringArray::from(col_values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

fn flatten_inner(v: Value) -> DFResult<Value> {
    Ok(match v {
        Value::Array(arr) => {
            let mut res = Vec::new();
            for item in arr {
                match item {
                    Value::Array(inner) => res.extend(inner),
                    Value::Null => return Ok(Value::Null),
                    _ => {
                        return exec_err!(
                            "Not an array: 'Input argument to ARRAY_FLATTEN is not an array of arrays'"
                        );
                    }
                }
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
        _ => v,
    })
}

make_udf_function!(ArrayFlattenFunc);

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
                "+------------------+",
                "| v                |",
                "+------------------+",
                "| [1,2,3,null,5,6] |",
                "+------------------+",
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
