use datafusion::logical_expr::sqlparser::ast::{Expr, VisitMut};
use datafusion::sql::sqlparser::ast::{
    Query, SelectItem, SetExpr, Statement, VisitorMut, visit_expressions_mut,
};
use std::collections::HashMap;
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct InlineAliasesInSelect {}

impl VisitorMut for InlineAliasesInSelect {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = &mut *query.body {
            let mut alias_expr_map = HashMap::new();

            for item in &select.projection {
                if let SelectItem::ExprWithAlias { expr, alias } = item {
                    alias_expr_map.insert(alias.value.clone(), expr.clone());
                }
            }

            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        visit_expressions_mut(expr, &mut |e: &mut Expr| {
                            if let Expr::Identifier(ident) = e {
                                if let Some(original) = alias_expr_map.get(&ident.value) {
                                    *e = original.clone();
                                }
                            }
                            ControlFlow::<()>::Continue(())
                        });
                    }
                    _ => {}
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut InlineAliasesInSelect {});
}
