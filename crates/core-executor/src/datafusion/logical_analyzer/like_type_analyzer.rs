use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, ExprSchemable, Like, ScalarUDF};
use embucket_functions::conversion::ToVarcharFunc;
use std::fmt::Debug;
use std::sync::Arc;

/// TODO DOCS
#[derive(Debug, Default)]
pub struct LikeTypeAnalyzer;

impl LikeTypeAnalyzer {
    fn varchar_expr_from(expr: Expr) -> Box<Expr> {
        Box::new(Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::from(ToVarcharFunc::new(false))),
            args: vec![expr],
        }))
    }
}

impl LikeTypeAnalyzer {
    fn analyze_internal(plan: &LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(plan);
        let new_plan = plan.clone().map_expressions(|expr| {
            let original_name = name_preserver.save(&expr);
            let transformed_expr = expr.transform_up(|e| {
                match &e {
                    Expr::Like(Like {
                        negated,
                        expr,
                        pattern,
                        escape_char,
                        case_insensitive,
                    }) => {
                        let expr_type = expr.get_type(plan.schema());
                        let pattern_type = pattern.get_type(plan.schema());
                        match (expr_type, pattern_type) {
                            //No need to coerce if both types are a string
                            (
                                Ok(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8),
                                Ok(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8),
                            ) => Ok(Transformed::no(e)),
                            (_, Ok(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)) => {
                                let udf = Self::varchar_expr_from(*expr.clone());
                                Ok(Transformed::yes(Expr::Like(Like {
                                    negated: *negated,
                                    expr: udf,
                                    pattern: pattern.clone(),
                                    escape_char: *escape_char,
                                    case_insensitive: *case_insensitive,
                                })))
                            }
                            (Ok(DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8), _) => {
                                let udf = Self::varchar_expr_from(*pattern.clone());
                                Ok(Transformed::yes(Expr::Like(Like {
                                    negated: *negated,
                                    expr: expr.clone(),
                                    pattern: udf,
                                    escape_char: *escape_char,
                                    case_insensitive: *case_insensitive,
                                })))
                            }
                            //Not Utf8 type or `Err(...)`, call to_char just in case
                            (_, _) => {
                                let udf1 = Self::varchar_expr_from(*expr.clone());
                                let udf2 = Self::varchar_expr_from(*pattern.clone());
                                Ok(Transformed::yes(Expr::Like(Like {
                                    negated: *negated,
                                    expr: udf1,
                                    pattern: udf2,
                                    escape_char: *escape_char,
                                    case_insensitive: *case_insensitive,
                                })))
                            }
                        }
                    }
                    _ => Ok(Transformed::no(e)),
                }
            })?;

            Ok(transformed_expr.update_data(|data| original_name.restore(data)))
        })?;
        Ok(new_plan)
    }
}

impl AnalyzerRule for LikeTypeAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| Self::analyze_internal(&plan))
            .data()?
            .recompute_schema()
    }
    fn name(&self) -> &'static str {
        "LikeTypeAnalyzer"
    }
}
