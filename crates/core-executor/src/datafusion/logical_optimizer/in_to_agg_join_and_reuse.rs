use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Aggregate, BinaryExpr, Expr, Join, LogicalPlan, Operator};
use std::sync::Arc;
use datafusion_common::{JoinConstraint, NullEquality};
use datafusion_expr::expr::InSubquery;

#[derive(Debug, Default)]
pub struct InToAggJoinAndReuse {}

impl InToAggJoinAndReuse {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for InToAggJoinAndReuse {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _cfg: &dyn OptimizerConfig,
    ) -> datafusion_common::Result<Transformed<LogicalPlan>> {
        plan.clone().transform_up(|node| match node {
            LogicalPlan::Filter(filter) => {
                // ищем InSubquery(expr, subquery)
                if let Expr::InSubquery(InSubquery{
                    expr,
                    subquery,
                    negated: false, ..
                }) = &filter.predicate
                {
                    if let LogicalPlan::Aggregate(Aggregate {
                        input,
                        group_expr,
                        aggr_expr,
                        ..
                    }) = subquery.subquery.as_ref()
                    {
                        // создаём имя поля агрегата (например, sum_qty)
                        let sum_field_name = "sum_qty".to_string();
                        let new_aggr = LogicalPlan::Aggregate(Aggregate {
                            input: input.clone(),
                            group_expr: group_expr.clone(),
                            aggr_expr: aggr_expr.clone(),
                            schema: None,
                        });

                        // join: left=filter.input, right=new_aggr
                        let join_cond = Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(expr.as_ref().clone()),
                            Operator::Eq,
                            Box::new(group_expr[0].clone()),
                        ));

                        let mut new_join = LogicalPlan::Join(Join {
                            left: Arc::new(*filter.input.clone()),
                            right: Arc::new(new_aggr),
                            on: vec![(expr_name(expr.as_ref())?, expr_name(&group_expr[0])?)],
                            join_type: JoinType::Inner,
                            filter: None,
                            schema: None,
                            join_constraint: JoinConstraint::On,
                            null_equality: NullEquality::NullEqualsNothing,
                        });

                        // переносим HAVING (если был) как Filter после Join
                        let having_expr = extract_having(subquery.as_ref());
                        if let Some(having) = having_expr {
                            new_join = LogicalPlan::Filter(Filter {
                                input: Arc::new(new_join),
                                predicate: having,
                            });
                        }

                        return Ok(Transformed::yes(new_join));
                    }
                }
                Ok(Transformed::no(node.clone()))
            }
            _ => Ok(Transformed::no(node.clone())),
        })
    }
}
