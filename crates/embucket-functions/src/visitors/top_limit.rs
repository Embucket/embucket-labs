use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, LimitClause, Query, SetExpr, Statement, TableFactor, TopQuantity, Value, VisitorMut,
};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct TopLimitVisitor;

impl TopLimitVisitor {
    fn ensure_limit_clause(limit_clause: &mut Option<LimitClause>) -> &mut Option<Expr> {
        if limit_clause.is_none() {
            *limit_clause = Some(LimitClause::LimitOffset {
                limit: None,
                offset: None,
                limit_by: vec![],
            });
        }
        match limit_clause.as_mut().unwrap() {
            LimitClause::LimitOffset { limit, .. } => limit,
            LimitClause::OffsetCommaLimit { .. } => unreachable!(
                "OffsetCommaLimit should not be constructed by this visitor"
            ),
        }
    }

    fn process_set_expr(&mut self, set_expr: &mut SetExpr, outer_limit_clause: &mut Option<LimitClause>) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    if let TableFactor::Derived { subquery, .. } = &mut table_with_joins.relation {
                        self.process_query(subquery);
                    }
                }

                if let Some(top) = select.top.take() {
                    if !top.percent && !top.with_ties {
                        let out_limit_ref = Self::ensure_limit_clause(outer_limit_clause);
                        if out_limit_ref.is_none() {
                            let maybe_expr = top.quantity.as_ref().map(|q| match q {
                                TopQuantity::Expr(expr) => expr.clone(),
                                TopQuantity::Constant(n) => Expr::Value(
                                    Value::Number(n.to_string(), false).with_empty_span(),
                                ),
                            });
                            if let Some(expr) = maybe_expr {
                                *out_limit_ref = Some(expr);
                            } else {
                                select.top = Some(top);
                            }
                        } else {
                            select.top = Some(top);
                        }
                    } else {
                        select.top = Some(top);
                    }
                }
            }
            SetExpr::Query(q) => self.process_query(q),
            SetExpr::SetOperation { left, right, .. } => {
                self.process_set_expr(left, outer_limit_clause);
                self.process_set_expr(right, outer_limit_clause);
            }
            _ => {}
        }
    }

    fn process_query(&mut self, query: &mut Query) {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                self.process_query(&mut cte.query);
            }
        }

        self.process_set_expr(&mut query.body, &mut query.limit_clause);
    }
}

impl VisitorMut for TopLimitVisitor {
    type Break = ();

    fn pre_visit_statement(&mut self, stmt: &mut Statement) -> ControlFlow<Self::Break> {
        if let Statement::Query(query) = stmt {
            self.process_query(query);
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut TopLimitVisitor);
}
