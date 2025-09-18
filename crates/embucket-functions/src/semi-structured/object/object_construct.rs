use crate::semi_structured::errors;
use datafusion::arrow::array::as_boolean_array;
use datafusion::arrow::{array::AsArray, datatypes::DataType};
use datafusion_common::cast::{as_float64_array, as_int64_array};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use indexmap::IndexMap;
use serde_json::{Number, Value};
use snafu::ResultExt;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ObjectConstructUDF {
    signature: Signature,
    aliases: Vec<String>,
    keep_null: bool,
}

impl ObjectConstructUDF {
    #[must_use]
    pub fn new(keep_null: bool) -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::VariadicAny,
                    TypeSignature::Nullary,
                ]),
                volatility: Volatility::Volatile,
            },
            aliases: vec!["make_object".to_string()],
            keep_null,
        }
    }
}

impl Default for ObjectConstructUDF {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ScalarUDFImpl for ObjectConstructUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.keep_null {
            "object_construct_keep_null"
        } else {
            "object_construct"
        }
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

        if args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "{}".to_string(),
            ))));
        }

        if args.len() % 2 != 0 {
            return errors::ObjectConstructRequiresEvenNumberOfArgumentsSnafu.fail()?;
        }

        let mut object = IndexMap::new();

        for chunk in args.chunks(2) {
            let key_array = chunk[0].clone().into_array(number_rows)?;
            let value_array = chunk[1].clone().into_array(number_rows)?;

            for i in 0..key_array.len() {
                if key_array.is_null(i) {
                    return errors::ObjectConstructKeyCannotBeNullSnafu.fail()?;
                }

                let key = if let Some(str_array) = key_array.as_string_opt::<i32>() {
                    str_array.value(i).to_string()
                } else {
                    return errors::ObjectConstructKeyMustBeAStringSnafu.fail()?;
                };

                let value = if value_array.is_null(i) {
                    Value::Null
                } else if let Some(str_array) = value_array.as_string_opt::<i32>() {
                    let istr = str_array.value(i);
                    if let Ok(json_obj) = serde_json::from_str(istr) {
                        json_obj
                    } else {
                        Value::String(istr.to_string())
                    }
                } else {
                    match value_array.data_type() {
                        DataType::Int64 => {
                            Value::Number(Number::from(as_int64_array(&value_array)?.value(i)))
                        }
                        DataType::Float64 => Value::Number(
                            Number::from_f64(as_float64_array(&value_array)?.value(i)).ok_or_else(
                                || errors::ObjectConstructValueMustBeANumberSnafu.build(),
                            )?,
                        ),
                        DataType::Boolean => Value::Bool(as_boolean_array(&value_array).value(i)),
                        DataType::Null => {
                            if self.keep_null {
                                Value::Null
                            } else {
                                continue; // Skip null values if keep_null is false
                            }
                        }
                        _ => {
                            return errors::ObjectConstructValueMustBeAStringNumberOrBooleanSnafu
                                .fail()?;
                        }
                    }
                };

                object.insert(key, value);
            }
        }

        let json_str = serde_json::to_string(&Value::Object(serde_json::Map::from_iter(object)))
            .context(errors::FailedToSerializeValueSnafu)?;

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str))))
    }
}

// This is no longer possible to use due to the deprecation of the Wildcard expr
// This will require fixing the SQLparser crate to support the new wildcard syntax inside a function call
// #[derive(Debug)]
// pub struct ObjectConstructAnalyzerRule;

// impl AnalyzerRule for ObjectConstructAnalyzerRule {
//     fn name(&self) -> &'static str {
//         "object_construct"
//     }

//     fn analyze(
//         &self,
//         plan: LogicalPlan,
//         _config: &ConfigOptions,
//     ) -> DFResult<LogicalPlan> {
//         plan.transform_up(analyze_object_construct_wildcard)
//             .map(|transformed|transformed.data)
//     }
// }

// fn analyze_object_construct_wildcard(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
//     let transformed_plan = plan.map_subqueries(|plan| plan.transform_up(analyze_object_construct_wildcard))?;

//     let transformed_plan = transformed_plan.transform_data(|plan| {

//         match &plan {
//             LogicalPlan::Projection(projection) => {
//                 let has_construct_schema = projection.expr.iter().any(|expr| {
//                     match expr {
//                         Expr::ScalarFunction (func ) => {
//                             func.name() == "object_construct" && func.args.iter().any(|arg| matches!(arg, Expr::Wildcard { .. }))
//                         }
//                         _ => false,
//                     }
//                 });
//                 //dbg!(&projection);
//                 Ok(Transformed::yes(plan.clone()))
//             }
//             _ => Ok(Transformed::no(plan)),
//         }
//     })?;

//     Ok(transformed_plan)
// }

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::semi_structured::array::array_construct::ArrayConstructUDF;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_object_construct() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(ObjectConstructUDF::new(false)));

        // Test basic object construction
        let sql = "SELECT object_construct('a', 1, 'b', 'hello', 'c', 2.5, 'd', null) as obj1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----------------------------+",
                "| obj1                        |",
                "+-----------------------------+",
                "| {\"a\":1,\"b\":\"hello\",\"c\":2.5} |",
                "+-----------------------------+",
            ],
            &result
        );

        // Test empty object
        let sql = "SELECT object_construct() as obj2";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            ["+------+", "| obj2 |", "+------+", "| {}   |", "+------+"],
            &result
        );

        // Test with null values
        let sql = "SELECT object_construct('a', 1, 'b', NULL, 'c', 3) as obj3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------+",
                "| obj3          |",
                "+---------------+",
                "| {\"a\":1,\"c\":3} |",
                "+---------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_construct_keep_null() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(ObjectConstructUDF::new(true)));

        // Test with null values
        let sql = "SELECT object_construct_keep_null('a', 1, 'b', NULL, 'c', 3) as obj3";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+------------------------+",
                "| obj3                   |",
                "+------------------------+",
                "| {\"a\":1,\"b\":null,\"c\":3} |",
                "+------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_construct_nested() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udf(ScalarUDF::from(ObjectConstructUDF::new(false)));
        ctx.register_udf(ScalarUDF::from(ArrayConstructUDF::new()));

        // Test nested object construction
        let sql = "SELECT object_construct('a', object_construct('x', 1, 'y', 2), 'b', array_construct(1, 2, 3)) as obj1";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------------------+",
                "| obj1                            |",
                "+---------------------------------+",
                "| {\"a\":{\"x\":1,\"y\":2},\"b\":[1,2,3]} |",
                "+---------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    // #[tokio::test]
    // async fn test_object_construct_wildcard() -> DFResult<()> {
    //     let mut ctx = SessionContext::new();
    //     register_udf(&mut ctx);

    //     // Create and populate the test table
    //     let sql = "CREATE TABLE demo_table_1 (province VARCHAR, created_date DATE);";
    //     ctx.sql(sql).await?;

    //     let sql = "INSERT INTO demo_table_1 (province, created_date) VALUES
    //             ('Manitoba', '2024-01-18'::DATE),
    //             ('Alberta', '2024-01-19'::DATE);
    //     ";
    //     ctx.sql(sql).await?;

    //     // Test object_construct with wildcard
    //     let sql = "SELECT object_construct(*) AS oc FROM demo_table_1 ORDER BY oc['PROVINCE']";
    //     let result = ctx.sql(sql).await?.collect().await?;

    //     assert_batches_eq!(
    //         [
    //             "+---------------------------------+",
    //             "| oc                              |",
    //             "+---------------------------------+",
    //             "| {                               |",
    //             "|   \"CREATED_DATE\": \"2024-01-19\", |",
    //             "|   \"PROVINCE\": \"Alberta\"         |",
    //             "| }                               |",
    //             "| {                               |",
    //             "|   \"CREATED_DATE\": \"2024-01-18\", |",
    //             "|   \"PROVINCE\": \"Manitoba\"        |",
    //             "| }                               |",
    //             "+---------------------------------+",
    //         ],
    //         &result
    //     );

    //     Ok(())
    // }
}
