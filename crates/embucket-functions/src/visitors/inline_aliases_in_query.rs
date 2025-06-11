use datafusion::logical_expr::sqlparser::ast::{Expr, VisitMut};
use datafusion::sql::sqlparser::ast::{
    Query, SelectItem, SetExpr, Statement, VisitorMut, visit_expressions_mut,
};
use std::collections::HashMap;
use std::ops::ControlFlow;

/// A visitor that inlines alias expressions in the `SELECT` clause of a SQL query.
///
/// # Purpose
/// This visitor traverses a SQL query and replaces references to aliases
/// defined in the `SELECT` projection with their original expressions.
/// This transformation is useful when further processing (e.g., optimization,
/// rewriting, or serialization) needs access to the full expression rather
/// than a symbolic alias reference.
///
/// # How It Works
/// 1. It first collects a mapping from alias names to their corresponding expressions.
///    For example, given `SELECT a + b AS sum_ab`, it stores `sum_ab -> a + b`.
///
/// 2. It then walks through each projection item and looks for identifier references
///    to aliases. If it finds one, it replaces the identifier with the full original
///    expression (e.g., replacing `sum_ab` with `a + b`).
///
/// 3. **Important**: This inlining is *not* performed inside **window functions**, since:
///    - Many SQL engines do not support alias references inside window functions,
///    - Inlining in such contexts could lead to semantic errors or redundant nesting
///      (e.g., `last_value(sum_ab)` → `last_value(a + b)` could be incorrect or unwanted).
///
/// # Example
/// Input:
/// ```sql
/// SELECT a + b AS sum_ab, sum_ab * 2 FROM my_table
/// ```
/// Output (after inlining):
/// ```sql
/// SELECT a + b AS sum_ab, (a + b) * 2 FROM my_table
/// ```
///
/// # Notes
/// - Does not descend into subqueries (handled elsewhere or explicitly avoided).
/// - Only replaces identifiers that match known aliases in the same SELECT block.
/// - Designed to be conservative and safe for common SQL transformations.
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
                        if matches!(expr, Expr::Function(func) if func.over.is_some()) {
                            continue;
                        }

                        let _ = visit_expressions_mut(expr, &mut |e: &mut Expr| {
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
