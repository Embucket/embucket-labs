use datafusion::logical_expr::sqlparser::ast::{Expr, Function, VisitMut};
use datafusion::sql::sqlparser::ast::{
    Query, SelectItem, SetExpr, Statement, VisitorMut, visit_expressions_mut,
};
use std::collections::HashMap;
use std::ops::ControlFlow;

/// A visitor that performs **safe alias inlining** inside the `SELECT` projection of a SQL query.
///
/// # Purpose
/// This visitor rewrites SQL `SELECT` statements by replacing references to column aliases
/// (defined within the same projection list) with their corresponding full expressions.
/// This is useful for:
/// - SQL rewrites
/// - Expression optimizations
/// - Normalization before query analysis or serialization
///
/// # Behavior
/// - Processes:
///   - `SELECT` projection
///   - `WHERE`
///   - `QUALIFY`
/// - Aliases are only substituted **within the same query block** (i.e., not across subqueries or CTE boundaries).
/// - Subqueries have independent alias scopes.
/// - Self-references are protected to avoid infinite recursion.
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
#[derive(Debug, Default)]
pub struct InlineAliasesInSelect {}

impl VisitorMut for InlineAliasesInSelect {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = &mut *query.body {
            let mut alias_expr_map = HashMap::new();

            for item in &mut select.projection {
                match item {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        substitute_aliases(expr, &alias_expr_map, Some(&alias.value), None);
                        //NOTE: if other aggregate functions happen (without over) - we have no way of knowing,
                        // like just calling last_value with an alias,
                        // perhaps this will need to be extended in the logical planning phase later
                        alias_expr_map.insert(alias.value.clone(), expr.clone());
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        substitute_aliases(expr, &alias_expr_map, None, None);
                    }
                    _ => {}
                }
            }

            // Rewrite WHERE
            if let Some(selection) = select.selection.as_mut() {
                substitute_aliases(
                    selection,
                    &alias_expr_map,
                    None,
                    Some(&|e| matches!(e, Expr::Function(Function { over: Some(_), .. }))),
                );
            }

            // Rewrite QUALIFY
            if let Some(qualify) = select.qualify.as_mut() {
                substitute_aliases(qualify, &alias_expr_map, None, None);
            }
        }

        // Recursively process CTEs (WITH clauses)
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                let _ = self.pre_visit_query(&mut cte.query);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Substitute aliases inside arbitrary expressions, recursively
fn substitute_aliases(
    expr: &mut Expr,
    alias_map: &HashMap<String, Expr>,
    forbidden_alias: Option<&str>,
    forbidden_predicate: Option<&dyn Fn(&Expr) -> bool>,
) {
    let _ = visit_expressions_mut(expr, &mut |e: &mut Expr| {
        match e {
            Expr::Identifier(ident) => {
                if Some(ident.value.as_str()) == forbidden_alias {
                    return ControlFlow::<()>::Continue(());
                }
                if let Some(subst) = alias_map.get(&ident.value) {
                    if let Some(pred) = forbidden_predicate
                        && pred(subst)
                    {
                        return ControlFlow::<()>::Continue(());
                    }
                    *e = subst.clone();
                }
            }
            Expr::Subquery(subquery) => {
                let _ = InlineAliasesInSelect::default().pre_visit_query(subquery);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut InlineAliasesInSelect {});
}
