use datafusion::logical_expr::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::{Expr, Query, SetExpr, Statement, Value, VisitorMut};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct FetchToLimit {}

impl VisitorMut for FetchToLimit {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(fetch) = query.fetch.take() {
            // Default quantity is 1 when not specified
            let quantity = fetch
                .quantity
                .unwrap_or_else(|| Expr::Value(Value::Number("1".to_string(), false).into()));
            // Ignore PERCENT and WITH TIES for now
            query.limit = Some(quantity);
        }
        // process WITH clauses recursively
        /*if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                let _ = self.pre_visit_query(&mut cte.query);
            }
        }
        if let SetExpr::Select(select) = query.body.as_mut() {
            for table_with_joins in &mut select.from {
                if let Some(subquery) = table_with_joins.relation.get_subquery_mut() {
                    let _ = self.pre_visit_query(subquery);
                }
            }
        }*/
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut FetchToLimit {});
}
