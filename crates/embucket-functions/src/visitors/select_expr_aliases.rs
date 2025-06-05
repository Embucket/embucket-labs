use datafusion::logical_expr::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::{Ident, Query, SelectItem, SetExpr, Statement, VisitorMut};
use std::collections::HashSet;
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct AddAliasesToSelectExpressions {}

impl VisitorMut for AddAliasesToSelectExpressions {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = &mut *query.body {
            let mut seen = HashSet::new();
            let mut counter = 0;

            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let expr_str = expr.to_string();

                        if !seen.insert(expr_str.clone()) {
                            let alias = format!("expr_{counter}");
                            *item = SelectItem::ExprWithAlias {
                                expr: expr.clone(),
                                alias: Ident::new(alias),
                            };
                            counter += 1;
                        }
                    }
                    SelectItem::ExprWithAlias { alias, .. } => {
                        seen.insert(alias.value.clone());
                    }
                    _ => {}
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut AddAliasesToSelectExpressions {});
}
