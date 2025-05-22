use super::super::macros::make_udf_function;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::{Value, to_string};

#[derive(Debug, Clone)]
pub struct ArrayGenerateRangeUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayGenerateRangeUDF {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                ]),
                volatility: Volatility::Immutable,
            },
            aliases: vec!["generate_range".to_string()],
        }
    }
}

impl Default for ArrayGenerateRangeUDF {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
impl ScalarUDFImpl for ArrayGenerateRangeUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_generate_range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(clippy::cast_sign_loss)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        if args.len() < 2 || args.len() > 3 {
            return Err(datafusion_common::error::DataFusionError::Internal(
                "array_generate_range requires 2 or 3 arguments".to_string(),
            ));
        }

        let mut args = args;
        let step = if args.len() == 3 {
            args.pop()
                .ok_or(datafusion_common::error::DataFusionError::Internal(
                    "Expected step argument".to_string(),
                ))?
                .into_array(number_rows)?
        } else {
            // Default step is 1
            let default_step = ScalarValue::Int64(Some(1));
            default_step.to_array_of_size(number_rows)?
        };
        let stop = args
            .pop()
            .ok_or(datafusion_common::error::DataFusionError::Internal(
                "Expected stop argument".to_string(),
            ))?
            .into_array(number_rows)?;
        let start = args
            .pop()
            .ok_or(datafusion_common::error::DataFusionError::Internal(
                "Expected start argument".to_string(),
            ))?
            .into_array(number_rows)?;

        let mut results = Vec::new();

        for i in 0..number_rows {
            let start_val = if start.is_null(i) {
                continue;
            } else {
                start
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or(datafusion_common::error::DataFusionError::Internal(
                        "Expected start argument to be an Int64Array".to_string(),
                    ))?
                    .value(i)
            };

            let stop_val = if stop.is_null(i) {
                continue;
            } else {
                stop.as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or(datafusion_common::error::DataFusionError::Internal(
                        "Expected stop argument to be an Int64Array".to_string(),
                    ))?
                    .value(i)
            };

            let step_val = if step.is_null(i) {
                continue;
            } else {
                step.as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or(datafusion_common::error::DataFusionError::Internal(
                        "Expected step argument to be an Int64Array".to_string(),
                    ))?
                    .value(i)
            };

            for i in (start_val..stop_val).step_by(step_val as usize) {
                results.push(Value::Number(i.into()));
            }
        }

        let json_str = to_string(&Value::Array(results)).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to serialize JSON: {e}",
            ))
        })?;

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str))))
    }
}

make_udf_function!(ArrayGenerateRangeUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_generate_range() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(ArrayGenerateRangeUDF::new()));

        // Test basic range
        let sql = "SELECT array_generate_range(2, 5) as range1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| range1  |",
                "+---------+",
                "| [2,3,4] |",
                "+---------+",
            ],
            &result
        );

        // Test with step
        let sql = "SELECT array_generate_range(5, 25, 10) as range2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------+",
                "| range2 |",
                "+--------+",
                "| [5,15] |",
                "+--------+",
            ],
            &result
        );

        // Test empty range
        let sql = "SELECT array_generate_range(5, 2) as range3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+--------+",
                "| range3 |",
                "+--------+",
                "| []     |",
                "+--------+"
            ],
            &result
        );

        Ok(())
    }
}
