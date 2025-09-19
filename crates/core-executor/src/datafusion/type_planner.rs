use arrow_schema::{DECIMAL128_MAX_PRECISION, Field, Fields};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::DataType as SQLDataType;
use datafusion::sql::utils::make_decimal_type;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, TableReference, not_impl_err, plan_err};
use datafusion_expr::planner::ContextProvider;
use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use sqlparser::ast::ArrayElemTypeDef;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct CustomTypePlanner {
    options: ConfigOptions,
}

impl TypePlanner for CustomTypePlanner {
    fn plan_type(&self, sql_type: &ast::DataType) -> Result<Option<DataType>> {
        match sql_type {
            SQLDataType::Array(ArrayElemTypeDef::None) => {
                Ok(Some(DataType::new_list(DataType::Utf8, true)))
            }
            SQLDataType::Int32 => Ok(Some(DataType::Int32)),
            SQLDataType::Int64 | SQLDataType::Int(_) | SQLDataType::Integer(_) => {
                Ok(Some(DataType::Int64))
            }
            SQLDataType::UInt32 => Ok(Some(DataType::UInt32)),
            SQLDataType::Float(_) | SQLDataType::Float32 => Ok(Some(DataType::Float32)),
            SQLDataType::Float64 => Ok(Some(DataType::Float64)),
            SQLDataType::Blob(_) | SQLDataType::Binary(_) | SQLDataType::Varbinary(_) => {
                Ok(Some(DataType::Binary))
            }
            // https://github.com/apache/datafusion/issues/12644 for JSON
            SQLDataType::JSON | SQLDataType::Character(_) | SQLDataType::CharacterVarying(_) => {
                Ok(Some(DataType::Utf8))
            }
            SQLDataType::Datetime(precision) => {
                let time_unit = parse_timestamp_precision(*precision)?;
                Ok(Some(DataType::Timestamp(time_unit, None)))
            }
            SQLDataType::Custom(a, b) => match a.to_string().to_ascii_uppercase().as_str() {
                "VARIANT" => Ok(Some(DataType::Utf8)),
                "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                    let parsed_b: Option<u64> = b.iter().next().and_then(|s| s.parse().ok());
                    let time_unit = parse_timestamp_precision(parsed_b)?;
                    Ok(Some(DataType::Timestamp(time_unit, None)))
                }
                "OBJECT" => {
                    let mut fields = vec![];
                    let mut i = 0;

                    while i + 1 < b.len() {
                        let field_name = &b[i].to_ascii_lowercase();
                        let type_str = b[i + 1].to_ascii_uppercase();
                        let mut nullable = true;
                        i += 2;

                        if i < b.len()
                            && b[i].eq_ignore_ascii_case("NOT")
                            && i + 1 < b.len()
                            && b[i + 1].eq_ignore_ascii_case("NULL")
                        {
                            nullable = false;
                            i += 2;
                        }

                        // Convert type_str to SQLDataType
                        let sql_type = parse_type_from_tokens(&type_str)?;
                        let data_type = match self.plan_type(&sql_type)? {
                            Some(dt) => dt,
                            // Fallback to SqlToRel for unsupported types
                            None => SqlToRel::new(self).convert_data_type(&sql_type)?,
                        };
                        fields.push(Field::new(field_name, data_type, nullable));
                    }
                    Ok(Some(DataType::Struct(Fields::from(fields))))
                }
                "NUMBER" => {
                    let (precision, scale) = match b.len() {
                        0 => (Some(u64::from(DECIMAL128_MAX_PRECISION)), None),
                        1 => {
                            let precision = b[0].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid precision: {}", b[0]))
                            })?;
                            (Some(precision), None)
                        }
                        2 => {
                            let precision = b[0].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid precision: {}", b[0]))
                            })?;
                            let scale = b[1].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid scale: {}", b[1]))
                            })?;
                            (Some(precision), Some(scale))
                        }
                        _ => {
                            return Err(DataFusionError::Plan(format!(
                                "Invalid NUMBER type format: {b:?}"
                            )));
                        }
                    };
                    make_decimal_type(precision, scale).map(Some)
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

fn parse_timestamp_precision(precision: Option<u64>) -> Result<TimeUnit> {
    match precision {
        Some(0) => Ok(TimeUnit::Second),
        Some(3) => Ok(TimeUnit::Millisecond),
        // We coerce nanoseconds to microseconds as Apache Iceberg v2 doesn't support nanosecond precision
        None | Some(6 | 9) => Ok(TimeUnit::Microsecond),
        _ => not_impl_err!("Unsupported SQL precision {precision:?}"),
    }
}

fn parse_type_from_tokens(type_str: &str) -> Result<SQLDataType> {
    let dialect = GenericDialect;
    let tokens = Tokenizer::new(&dialect, type_str)
        .tokenize()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    parser
        .parse_data_type()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

impl ContextProvider for CustomTypePlanner {
    fn get_table_source(&self, _name: TableReference) -> Result<Arc<dyn TableSource>> {
        plan_err!("Not implemented")
    }
    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }
    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }
    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }
    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        &self.options
    }
    fn udf_names(&self) -> Vec<String> {
        vec![]
    }
    fn udaf_names(&self) -> Vec<String> {
        vec![]
    }
    fn udwf_names(&self) -> Vec<String> {
        vec![]
    }
}
