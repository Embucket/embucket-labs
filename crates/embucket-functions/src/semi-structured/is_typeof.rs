use datafusion::arrow::array::{Array, as_string_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::arrow::array::BooleanBuilder;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub enum Kind {
    Null,
    Boolean,
    Double,
    Decimal,
    Real,
    Integer,
    String,
    Array,
    Object,
}

/// `is_null_value`, `is_boolean`, `is_decimal`, `is_double`, `is_integer`, `is_varchar`,
/// `is_array`, `is_object`, `is_real` SQL functions.
///
/// These functions check if the value in a VARIANT column is of a specific type.
/// Syntax: IS_<type>(<`variant_expr`>)
/// Arguments:
/// - `variant_expr`
///   An expression that evaluates to a value of type VARIANT.
///   Example: `SELECT is_integer('123') AS is_int, is_null_value(NULL) AS is_null`;
///   Returns a BOOLEAN value indicating whether the value is of the specified type or NULL if the input is NULL.
#[derive(Debug)]
pub struct IsTypeofFunc {
    signature: Signature,
    kind: Kind,
}

impl Default for IsTypeofFunc {
    fn default() -> Self {
        Self::new(Kind::Null)
    }
}

impl IsTypeofFunc {
    #[must_use]
    pub fn new(kind: Kind) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            kind,
        }
    }
}

impl ScalarUDFImpl for IsTypeofFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Null => "is_null_value",
            Kind::Boolean => "is_boolean",
            Kind::Double => "is_double",
            Kind::Decimal => "is_decimal",
            Kind::Integer => "is_integer",
            Kind::String => "is_varchar",
            Kind::Array => "is_array",
            Kind::Object => "is_object",
            Kind::Real => "is_real",
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

        let mut b = BooleanBuilder::new();

        match arr.data_type() {
            DataType::Null => append_all(&mut b, arr.len(), matches!(self.kind, Kind::Null)),
            DataType::Boolean => append_all(&mut b, arr.len(), matches!(self.kind, Kind::Boolean)),
            v if v.is_integer() => {
                // If the kind is decimal, we need to check if the integer can be cast to decimal
                if matches!(self.kind, Kind::Decimal)
                    && cast(&arr, &DataType::Decimal128(38, 0)).is_ok()
                {
                    append_all(&mut b, arr.len(), matches!(self.kind, Kind::Decimal));
                } else {
                    append_all(&mut b, arr.len(), matches!(self.kind, Kind::Integer));
                }
            }
            v if v.is_floating() => {
                append_all(
                    &mut b,
                    arr.len(),
                    matches!(self.kind, Kind::Double | Kind::Real),
                );
            }
            v if v.is_nested() => append_all(&mut b, arr.len(), matches!(self.kind, Kind::Array)),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                append_all(&mut b, arr.len(), matches!(self.kind, Kind::Decimal));
            }
            _ => {
                let input = as_string_array(&arr);
                for v in input {
                    let Some(v) = v else {
                        b.append_null();
                        continue;
                    };
                    match serde_json::from_str::<Value>(v) {
                        Ok(v) => match self.kind {
                            Kind::Null => b.append_value(v.is_null()),
                            Kind::Boolean => b.append_value(v.is_boolean()),
                            Kind::Double | Kind::Real | Kind::Decimal => {
                                b.append_value(v.is_number());
                            }
                            Kind::Integer => b.append_value(v.is_i64()),
                            Kind::String => b.append_value(v.is_string()),
                            Kind::Array => b.append_value(v.is_array()),
                            Kind::Object => b.append_value(v.is_object()),
                        },
                        Err(e) => exec_err!("Failed to parse JSON string: {e}")?,
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

fn append_all(b: &mut BooleanBuilder, len: usize, v: bool) {
    for _ in 0..len {
        b.append_value(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_is_integer() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IsTypeofFunc::new(Kind::Integer)));

        let sql = "SELECT is_integer(123), is_integer(NULL)";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------+------------------+",
                "| is_integer(123) | is_integer(NULL) |",
                "+-------------------------+------------------+",
                "| true                    |                  |",
                "+-------------------------+------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_is_double() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(IsTypeofFunc::new(Kind::Double)));

        let sql = "SELECT is_double('123'), is_double('3.14')";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------------+-------------------------+",
                "| is_double(Utf8(\"123\")) | is_double(Utf8(\"3.14\")) |",
                "+------------------------+-------------------------+",
                "| true                   | true                    |",
                "+------------------------+-------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
