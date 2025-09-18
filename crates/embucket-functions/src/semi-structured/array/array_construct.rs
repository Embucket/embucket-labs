use crate::json;
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::Result as DFResult;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use serde_json::Value;
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
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

        let arrays: Vec<_> = args
            .into_iter()
            .map(|arg| arg.into_array(number_rows))
            .collect::<Result<_, _>>()?;
        let json_arrays: Vec<Value> = arrays
            .into_iter()
            .map(|array| json::encode_array(array.clone()))
            .collect::<Result<_, _>>()?;

        let mut rows: Vec<Value> = Vec::with_capacity(number_rows);
        for row_index in 0..number_rows {
            let mut row: Vec<Value> = Vec::with_capacity(json_arrays.len());
            for arr in &json_arrays {
                if let Value::Array(a) = arr {
                    let mut elem = a[row_index].clone();
                    match &elem {
                        Value::String(s) => {
                            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                                elem = parsed;
                            }
                            row.push(elem);
                        }
                        Value::Null => row.push(Value::Null),
                        _ => row.push(elem),
                    }
                } else {
                    row.push(Value::Null);
                }
            }
            rows.push(Value::Array(row));
        }
        let mut col_values: Vec<Option<String>> = Vec::with_capacity(number_rows);
        for val in rows {
            col_values.push(Some(
                serde_json::to_string(&val).context(errors::FailedToSerializeValueSnafu)?,
            ));
        }
        let array: StringArray = StringArray::from(col_values);
        Ok(ColumnarValue::Array(Arc::new(array)))
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
