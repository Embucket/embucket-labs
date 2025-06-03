use crate::json;
use crate::macros::make_udf_function;
use datafusion::arrow::{array::AsArray, datatypes::DataType};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ArrayConstructUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayConstructUDF {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::VariadicAny,
                    TypeSignature::Nullary,
                ]),
                volatility: Volatility::Immutable,
            },
            aliases: vec!["make_array".to_string(), "make_list".to_string()],
        }
    }
}

impl Default for ArrayConstructUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrayConstructUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "array_construct"
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let mut results = Vec::new();

        for arg in args {
            let arg_array = arg.into_array(number_rows)?;
            for i in 0..arg_array.len() {
                if arg_array.is_null(i) {
                    results.push(Value::Null);
                } else if let Some(str_array) = arg_array.as_string_opt::<i32>() {
                    for istr in str_array {
                        match istr {
                            Some(istr) => {
                                if let Ok(json_obj) = serde_json::from_str(istr) {
                                    results.push(json_obj);
                                } else {
                                    results.push(Value::String(istr.to_string()));
                                }
                            }
                            None => {
                                results.push(Value::Null);
                            }
                        }
                    }
                } else {
                    let object = json::encode_array(arg_array.clone())?;
                    results.push(object);
                }
            }
        }

        for result in &mut results {
            if let Value::Array(arr) = result {
                if arr.len() == 1 {
                    *result = arr[0].clone();
                }
            }
        }

        let arr = serde_json::Value::Array(results);
        let json_str = serde_json::to_string(&arr).map_err(|e| {
            datafusion_common::error::DataFusionError::Internal(format!(
                "Failed to serialize JSON: {e}",
            ))
        })?;
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str))))
    }
}

make_udf_function!(ArrayConstructUDF);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_array_construct() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));

        // Test basic array construction
        let sql = "SELECT array_construct(1, 2, 3) as arr1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------+",
                "| arr1    |",
                "+---------+",
                "| [1,2,3] |",
                "+---------+"
            ],
            &result
        );

        // Test mixed types
        let sql = "SELECT array_construct(1, 'hello', 2.5) as arr2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------------+",
                "| arr2            |",
                "+-----------------+",
                "| [1,\"hello\",2.5] |",
                "+-----------------+",
            ],
            &result
        );

        // Test empty array
        let sql = "SELECT array_construct() as arr4";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            ["+------+", "| arr4 |", "+------+", "| []   |", "+------+"],
            &result
        );

        // Test with null values
        let sql = "SELECT array_construct(1, NULL, 3) as arr5";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| arr5       |",
                "+------------+",
                "| [1,null,3] |",
                "+------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_array_construct_nested() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));

        // Test basic array construction
        let sql =
            "SELECT array_construct(array_construct(1, 2, 3), array_construct(4, 5, 6)) as arr1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------------+",
                "| arr1              |",
                "+-------------------+",
                "| [[1,2,3],[4,5,6]] |",
                "+-------------------+",
            ],
            &result
        );

        Ok(())
    }
}
