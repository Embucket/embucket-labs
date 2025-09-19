use datafusion::arrow::array::{Array, as_string_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::StringBuilder;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

/// typeof SQL function
/// Returns the type of a value stored in a VARIANT column.
/// Syntax: TYPEOF( <`variant_expr`> )
/// Arguments:
/// - `variant_expr`
///   An expression that evaluates to a value of type VARIANT.
///   Example SELECT TYPEOF('{"a":1}') as v;
///   Returns a STRING value or NULL.
#[derive(Debug)]
pub struct TypeofFunc {
    signature: Signature,
}

impl Default for TypeofFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeofFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TypeofFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "typeof"
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
            ColumnarValue::Scalar(v) => v.to_array()?,
        };

        let mut b = StringBuilder::with_capacity(arr.len(), 1024);

        match arr.data_type() {
            v if v.is_integer() => append_all(&mut b, arr.len(), "INTEGER"),
            v if v.is_floating() => append_all(&mut b, arr.len(), "DOUBLE"),
            DataType::Boolean => append_all(&mut b, arr.len(), "BOOLEAN"),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                append_all(&mut b, arr.len(), "DECIMAL");
            }
            v if v.is_nested() => append_all(&mut b, arr.len(), "ARRAY"),
            _ => {
                let input = as_string_array(&arr);
                for v in input {
                    let Some(v) = v else {
                        b.append_null();
                        continue;
                    };
                    match serde_json::from_str::<Value>(&v.to_ascii_lowercase()) {
                        Ok(v) => match v {
                            Value::Null => b.append_value("NULL_VALUE"),
                            Value::Bool(_) => b.append_value("BOOLEAN"),
                            Value::Number(n) => match n {
                                n if n.is_i64() => b.append_value("INTEGER"),
                                n if n.is_f64() => b.append_value("DOUBLE"),
                                _ => b.append_value("NUMBER"),
                            },
                            Value::String(_) => b.append_value("VARCHAR"),
                            Value::Array(_) => b.append_value("ARRAY"),
                            Value::Object(_) => b.append_value("OBJECT"),
                        },
                        Err(_) => b.append_value("VARCHAR"),
                    }
                }
            }
        }

        let res = b.finish();
        Ok(if arr.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }
}

fn append_all(b: &mut StringBuilder, len: usize, v: &str) {
    for _ in 0..len {
        b.append_value(v);
    }
}

crate::macros::make_udf_function!(TypeofFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_typeof() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(TypeofFunc::new()));

        ctx.sql("CREATE OR REPLACE TABLE vartab (n INTEGER, v STRING);")
            .await?
            .collect()
            .await?;
        ctx.sql(
            r#"INSERT INTO vartab
  SELECT column1 AS n, column2 AS v
    FROM VALUES (1, 'null'), 
                (2, null), 
                (3, 'true'),
                (4, '-17'), 
                (5, '123.12'), 
                (6, '1.912e2'),
                (7, '"Om ara pa ca na dhih"  '), 
                (8, '[-1, 12, 289, 2188, false]'), 
                (9, '{ "x" : "abc", "y" : false, "z": 10} '),
                (10, 'Om ara pa ca na dhih')
       AS vals;"#,
        )
        .await?
        .collect()
        .await?;

        let result = ctx
            .sql("SELECT typeof(v) AS type FROM vartab ORDER BY n")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+------------+",
                "| type       |",
                "+------------+",
                "| NULL_VALUE |",
                "|            |",
                "| BOOLEAN    |",
                "| INTEGER    |",
                "| DOUBLE     |",
                "| DOUBLE     |",
                "| VARCHAR    |",
                "| ARRAY      |",
                "| OBJECT     |",
                "| VARCHAR    |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }
}
