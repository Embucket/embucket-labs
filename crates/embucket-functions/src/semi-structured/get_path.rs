use crate::json;
use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::{Array, StringBuilder, as_string_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetPathFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for GetPathFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GetPathFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
            aliases: vec!["json_extract_path_text".to_string()],
        }
    }
}

impl ScalarUDFImpl for GetPathFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "get_path"
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
        let ColumnarValue::Scalar(path_scalar) = args[1].clone() else {
            return exec_err!(
                "get_path function requires the second argument to be a scalar string"
            );
        };
        let ScalarValue::Utf8(Some(path)) = path_scalar.cast_to(&DataType::Utf8)? else {
            return exec_err!(
                "get_path function requires the second argument to be a scalar string"
            );
        };

        // Normalize to Utf8 to support Utf8View/LargeUtf8
        let arr = cast(&arr, &DataType::Utf8)?;
        let input = as_string_array(&arr);
        let mut res = StringBuilder::new();
        for v in input {
            let Some(v) = v else {
                res.append_null();
                continue;
            };

            match serde_json::from_str::<Value>(v) {
                Ok(json_value) => {
                    let Some(value) = json::tokenize_path(&path)
                        .and_then(|v| json::get_json_value(&json_value, &v))
                    else {
                        res.append_null();
                        continue;
                    };

                    res.append_value(
                        serde_json::to_string_pretty(&value)
                            .context(errors::FailedToSerializeValueSnafu)?,
                    );
                }
                Err(_) => res.append_null(),
            }
        }
        let res = res.finish();
        Ok(if res.len() == 1 {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&res, 0)?)
        } else {
            ColumnarValue::Array(Arc::new(res))
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(GetPathFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;
    #[tokio::test]
    async fn test_array_indexing() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(GetPathFunc::new()));

        let sql = "CREATE OR REPLACE TABLE get_path_demo(id INTEGER, v  STRING);";
        ctx.sql(sql).await?.collect().await?;
        ctx.sql(
            r#"
INSERT INTO get_path_demo (id, v)
  SELECT 1,
         '{
           "array1" : [
             {"id1": "value_a1", "id2": "value_a2", "id3": "value_a3"}
           ],
           "array2" : [
             {"id1": "value_b1", "id2": "value_b2", "id3": "value_b3"}
           ],
           "object_outer_key1" : {
             "object_inner_key1a": "object_x1",
             "object_inner_key1b": "object_x2"
           }
         }';"#,
        )
        .await?
        .collect()
        .await?;

        ctx.sql(
            r#"INSERT INTO get_path_demo (id, v)
  SELECT 2,
         '{
           "array1" : [
             {"id1": "value_c1", "id2": "value_c2", "id3": "value_c3"}
           ],
           "array2" : [
             {"id1": "value_d1", "id2": "value_d2", "id3": "value_d3"}
           ],
           "object_outer_key1" : {
             "object_inner_key1a": "object_y1",
             "object_inner_key1b": "object_y2"
           }
         }';"#,
        )
        .await?
        .collect()
        .await?;

        let sql = "SELECT id, GET_PATH(v, 'array2[0].id3') AS id3_in_array2 FROM get_path_demo;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----+---------------+",
                "| id | id3_in_array2 |",
                "+----+---------------+",
                "| 1  | \"value_b3\"    |",
                "| 2  | \"value_d3\"    |",
                "+----+---------------+",
            ],
            &result
        );

        let sql = "SELECT id, GET_PATH(v, 'object_outer_key1:object_inner_key1a') AS object_inner_key1A_values FROM get_path_demo;";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+----+---------------------------+",
                "| id | object_inner_key1a_values |",
                "+----+---------------------------+",
                "| 1  | \"object_x1\"               |",
                "| 2  | \"object_y1\"               |",
                "+----+---------------------------+",
            ],
            &result
        );
        Ok(())
    }
}
