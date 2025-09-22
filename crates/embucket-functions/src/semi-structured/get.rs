use crate::semi_structured::errors;
use datafusion::arrow::array::{Array, StringBuilder, as_string_array};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::types::{logical_int64, logical_string};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{Coercion, ScalarFunctionArgs, ScalarUDFImpl, TypeSignatureClass};
use serde_json::Value;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

// get SQL function
// Retrieves a value from a JSON object or array based on the provided key or index.
// Syntax: GET(<json>, <key_or_index>)
// The function returns NULL if either of the arguments is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetFunc {
    signature: Signature,
    ignore_case: bool,
}

impl Default for GetFunc {
    fn default() -> Self {
        Self::new(false)
    }
}

impl GetFunc {
    #[must_use]
    pub fn new(ignore_case: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                ],
                Volatility::Immutable,
            ),
            ignore_case,
        }
    }
}

impl ScalarUDFImpl for GetFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.ignore_case {
            "get_ignore_case"
        } else {
            "get"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arr = match args[0].clone() {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => v.cast_to(&DataType::Utf8)?.to_array()?,
        };
        let ColumnarValue::Scalar(path) = args[1].clone() else {
            return exec_err!("function requires the second argument to be a scalar");
        };

        let arr = as_string_array(&arr);
        let mut res = StringBuilder::new();

        for v in arr {
            let Some(input) = v else {
                res.append_null();
                continue;
            };

            if path.is_null() {
                res.append_null();
                continue;
            }

            let json_input: Value =
                serde_json::from_str(input).context(errors::FailedToDeserializeJsonSnafu)?;

            match &path {
                ScalarValue::Utf8(Some(key)) => {
                    let Value::Object(map) = json_input else {
                        res.append_null();
                        continue;
                    };

                    let value = if self.ignore_case {
                        let mut found = None;
                        for (k, v) in map {
                            if k.eq_ignore_ascii_case(key) {
                                found = Some(v);
                                break;
                            }
                        }

                        if let Some(value) = found {
                            value
                        } else {
                            res.append_null();
                            continue;
                        }
                    } else if let Some(value) = map.get(key) {
                        value.to_owned()
                    } else {
                        res.append_null();
                        continue;
                    };

                    res.append_value(
                        serde_json::to_string(&value)
                            .context(errors::FailedToSerializeValueSnafu)?,
                    );
                }
                ScalarValue::Int64(Some(key)) => {
                    let Value::Array(arr) = json_input else {
                        res.append_null();
                        continue;
                    };

                    let Some(value) = arr.get(key.as_usize()) else {
                        res.append_null();
                        continue;
                    };

                    res.append_value(
                        serde_json::to_string(value)
                            .context(errors::FailedToSerializeValueSnafu)?,
                    );
                }
                _ => {
                    return exec_err!(
                        "get function requires the second argument to be a scalar string or integer"
                    );
                }
            }
        }

        let res = res.finish();
        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_indexing() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(GetFunc::new(false)));

        let sql = "SELECT get('[1, {\"a\":4}]',0) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ],
            &result
        );

        let sql = "SELECT get('[1, {\"a\":4}]',1) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+",
                "| value   |",
                "+---------+",
                "| {\"a\":4} |",
                "+---------+",
            ],
            &result
        );

        let sql = "SELECT get('[1, {\"a\":4}]',2) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "|       |",
                "+-------+",
            ],
            &result
        );

        let sql = "SELECT get('[1, {\"a\":4}]',null) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "|       |",
                "+-------+",
            ],
            &result
        );

        let sql = "SELECT get(null,2) AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "|       |",
                "+-------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_indexing() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(GetFunc::new(false)));

        let sql = "SELECT get('{\"a\":1}','a') AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ],
            &result
        );

        let sql = "SELECT get('{\"a\":1}','b') AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "|       |",
                "+-------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_indexing_ignore_case() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(GetFunc::new(true)));

        let sql = "SELECT get_ignore_case('{\"a\":1}','A') AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ],
            &result
        );

        let sql = "SELECT get_ignore_case('{\"a\":1}','b') AS value;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------+",
                "| value |",
                "+-------+",
                "|       |",
                "+-------+",
            ],
            &result
        );

        Ok(())
    }
}
