use crate::macros::make_udf_function;
use crate::semi_structured::errors;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::builder::StringBuilder;
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{
    Result as DFResult, ScalarValue,
    types::{
        NativeType, logical_binary, logical_boolean, logical_int8, logical_int16, logical_int32,
        logical_int64, logical_string, logical_uint8, logical_uint16, logical_uint32,
    },
};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VariantArrayElementUDF {
    signature: Signature,
    aliases: Vec<String>,
}

impl VariantArrayElementUDF {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_string()),
                            vec![TypeSignatureClass::Native(logical_binary())],
                            NativeType::String,
                        ),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_string()),
                            vec![TypeSignatureClass::Native(logical_binary())],
                            NativeType::String,
                        ),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_boolean())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_string()),
                            vec![TypeSignatureClass::Native(logical_binary())],
                            NativeType::String,
                        ),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_string()),
                            vec![
                                TypeSignatureClass::Native(logical_binary()),
                                TypeSignatureClass::Native(logical_int8()),
                                TypeSignatureClass::Native(logical_int16()),
                                TypeSignatureClass::Native(logical_int32()),
                                TypeSignatureClass::Native(logical_int64()),
                                TypeSignatureClass::Native(logical_uint8()),
                                TypeSignatureClass::Native(logical_uint16()),
                                TypeSignatureClass::Native(logical_uint32()),
                            ],
                            NativeType::String,
                        ),
                    ]),
                ]),
                volatility: Volatility::Immutable,
            },
            aliases: vec!["array_element".to_string()],
        }
    }
}

impl Default for VariantArrayElementUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for VariantArrayElementUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "variant_element"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    #[allow(clippy::too_many_lines, clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { mut args, .. } = args;
        let (flatten, index, array_str) = if args.len() == 3 {
            (args.pop(), args.pop().unwrap(), args.pop().unwrap())
        } else if args.len() == 2 {
            (None, args.pop().unwrap(), args.pop().unwrap())
        } else {
            return errors::InvalidNumberOfArgumentsSnafu.fail()?;
        };
        match (array_str, index) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(index_value)) => {
                let ScalarValue::Utf8(Some(index)) = index_value else {
                    return errors::ExpectedJsonPathValueForIndexSnafu.fail()?;
                };

                let flatten =
                    if let Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(b)))) = flatten {
                        b
                    } else {
                        false
                    };

                let string_array = array.as_string::<i32>();
                let mut builder = StringBuilder::new();

                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let value: Option<Vec<Value>> =
                            jsonpath_lib::select_as(string_array.value(i), &index).ok();
                        match value {
                            Some(s) => {
                                if s.is_empty() {
                                    builder.append_null();
                                } else if flatten {
                                    if s.len() == 1 {
                                        builder.append_value(s[0].to_string());
                                    } else {
                                        builder.append_value(Value::Array(s).to_string());
                                    }
                                } else {
                                    builder.append_value(Value::Array(s).to_string());
                                }
                            }
                            None => builder.append_null(),
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (ColumnarValue::Scalar(array_value), ColumnarValue::Scalar(index_value)) => {
                let ScalarValue::Utf8(Some(index)) = index_value else {
                    return errors::ExpectedJsonPathValueForIndexSnafu.fail()?;
                };

                let ScalarValue::Utf8(Some(array_str)) = array_value else {
                    return errors::ExpectedStringArraySnafu.fail()?;
                };

                let flatten =
                    if let Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(b)))) = flatten {
                        b
                    } else {
                        false
                    };
                let value: Option<Vec<Value>> = jsonpath_lib::select_as(&array_str, &index).ok();
                match value {
                    Some(s) => {
                        if s.is_empty() {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                        } else if flatten {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                s[0].to_string(),
                            ))))
                        } else {
                            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                Value::Array(s).to_string(),
                            ))))
                        }
                    }
                    None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
            _ => errors::InvalidArgumentTypesSnafu.fail()?,
        }
    }
}

make_udf_function!(VariantArrayElementUDF);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use crate::semi_structured::variant::visitors::variant_element;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::Statement;
    use datafusion_expr::ScalarUDF;
    #[tokio::test]
    async fn test_array_indexing() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register both UDFs
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));
        ctx.register_udf(ScalarUDF::from(VariantArrayElementUDF::new()));

        // Create a table with ID and arrvar columns
        let sql = "CREATE TABLE test_table (id INT, arrvar STRING)";
        ctx.sql(sql).await?.collect().await?;

        // Insert some test data
        let sql = "INSERT INTO test_table VALUES (1, array_construct(1, 2, 3)), (2, array_construct('a', 'b', 'c'))";
        ctx.sql(sql).await?.collect().await?;

        // Test basic array indexing
        let sql = "SELECT arrvar[0] as first, \
                         arrvar[1] as second, \
                         arrvar[2] as third \
                  FROM test_table WHERE id = 1";

        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            variant_element::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------+--------+-------+",
                "| first | second | third |",
                "+-------+--------+-------+",
                "| 1     | 2      | 3     |",
                "+-------+--------+-------+"
            ],
            &result
        );

        // Test out of bounds indexing
        let sql = "SELECT arrvar[5] as out_of_bounds FROM test_table WHERE id = 1";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            variant_element::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| out_of_bounds |",
                "+---------------+",
                "|               |",
                "+---------------+",
            ],
            &result
        );

        // Test mixed type array
        let sql = "SELECT arrvar[1] as str_element FROM test_table WHERE id = 2";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            variant_element::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| str_element |",
                "+-------------+",
                "| \"b\"         |",
                "+-------------+"
            ],
            &result
        );

        // Test empty array
        let sql = "SELECT array_construct()[0] as empty_array";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            variant_element::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+",
                "| empty_array |",
                "+-------------+",
                "|             |",
                "+-------------+"
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_variant_object_path() -> DFResult<()> {
        let ctx = SessionContext::new();

        // Register UDFs
        ctx.register_udf(ScalarUDF::from(VariantArrayElementUDF::new()));

        // Create a table with JSON data
        let sql = "CREATE TABLE json_table (id INT, json_col STRING)";
        ctx.sql(sql).await?.collect().await?;

        // Insert test JSON data
        let sql = "INSERT INTO json_table VALUES 
            (1, '{\"a\": {\"b\": [1,2,3]}}'),
            (2, '{\"a\": {\"b\": [\"x\",\"y\",\"z\"]}}')";
        ctx.sql(sql).await?.collect().await?;

        // Test JSON path access
        let sql = "SELECT json_col:a.b[0] as first_elem FROM json_table";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            variant_element::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------+",
                "| first_elem |",
                "+------------+",
                "| 1          |",
                "| \"x\"        |",
                "+------------+"
            ],
            &result
        );

        // Test nested JSON path access with array flattening
        let sql = "SELECT json_col:a.b as array_elem FROM json_table";
        let mut statement = ctx.state().sql_to_statement(sql, "snowflake")?;
        if let Statement::Statement(ref mut stmt) = statement {
            variant_element::visit(stmt);
        }
        let plan = ctx.state().statement_to_plan(statement).await?;
        let result = ctx.execute_logical_plan(plan).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| array_elem    |",
                "+---------------+",
                "| [1,2,3]       |",
                "| [\"x\",\"y\",\"z\"] |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }
}
