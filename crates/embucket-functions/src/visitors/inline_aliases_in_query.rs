use datafusion::logical_expr::sqlparser::ast::{Expr, VisitMut};
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
/// - It processes only the `SELECT` projection (i.e. the expressions in the `SELECT ...` list).
/// - For each alias defined via `AS`, it stores the original expression.
/// - While processing other projection expressions, any reference to an alias is replaced with its full expression.
/// - Aliases are only substituted **within the same query block** (i.e., not across subqueries or CTE boundaries).
///
/// # Subqueries
/// - Subqueries inside projections are processed recursively.
/// - Each subquery has its own independent alias scope.
///
/// # Self-reference protection
/// - The alias being defined is protected from self-replacement during its own expansion to avoid infinite recursion.
/// - Example: `SELECT a AS x, x + 1` → `SELECT a AS x, a + 1`.
///
/// # Limitations (By Design)
/// - Only processes projection (`SELECT ...`) — does not modify `WHERE`, `HAVING`, `ORDER BY`, `JOIN`, `GROUP BY`, etc.
/// - Window functions are not treated specially — all expressions inside projections are uniformly processed.
/// - No alias expansion is performed for `USING` or `NATURAL JOIN` clauses.
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

            for item in &select.projection {
                if let SelectItem::ExprWithAlias { expr, alias } = item {
                    alias_expr_map.insert(alias.value.clone(), expr.clone());
                }
            }

            for item in &mut select.projection {
                match item {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        substitute_aliases(expr, &alias_expr_map, Some(&alias.value));
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        substitute_aliases(expr, &alias_expr_map, None);
                    }
                    _ => {}
                }
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
) {
    let _ = visit_expressions_mut(expr, &mut |e: &mut Expr| {
        match e {
            Expr::Identifier(ident) => {
                if Some(ident.value.as_str()) == forbidden_alias {
                    return ControlFlow::<()>::Continue(());
                }
                if let Some(subst) = alias_map.get(&ident.value) {
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
