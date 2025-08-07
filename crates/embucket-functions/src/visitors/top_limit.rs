use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, Query, SetExpr, Statement, TableFactor, TopQuantity, Value, VisitorMut,
};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct TopLimitVisitor;

impl TopLimitVisitor {
    fn process_set_expr(&mut self, set_expr: &mut SetExpr, outer_limit: &mut Option<Expr>) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    if let TableFactor::Derived { subquery, .. } = &mut table_with_joins.relation {
                        self.process_query(subquery);
                    }
                }

                if let Some(top) = select.top.take() {
                    if !top.percent && !top.with_ties {
                        if outer_limit.is_none()
                            && let Some(expr) = top.quantity.map(|q| match q {
                                TopQuantity::Expr(expr) => expr,
                                TopQuantity::Constant(n) => Expr::Value(
                                    Value::Number(n.to_string(), false).with_empty_span(),
                                ),
                            })
                        {
                            *outer_limit = Some(expr);
                        }
                    } else {
                        select.top = Some(top);
                    }
                }
            }
            SetExpr::Query(q) => self.process_query(q),
            SetExpr::SetOperation { left, right, .. } => {
                self.process_set_expr(left, outer_limit);
                self.process_set_expr(right, outer_limit);
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

        self.process_set_expr(&mut query.body, &mut query.limit);
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
