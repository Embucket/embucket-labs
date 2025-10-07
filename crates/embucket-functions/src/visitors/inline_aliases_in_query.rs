use datafusion::logical_expr::sqlparser::ast::{Expr, Function, SetOperator, VisitMut};
use datafusion::sql::sqlparser::ast::{
    Query, SelectItem, SetExpr, Statement, TableFactor, VisitorMut, visit_expressions_mut,
};
use std::collections::{HashMap, HashSet};
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
/// ```sql
/// SELECT 'test' as name, length(name) FROM (SELECT name FROM VALUES ('test'))
/// ```
/// Output (after inlining, doesn't change anything):
/// ```sql
/// SELECT 'test' as name, length(name) FROM (SELECT name FROM VALUES ('test'))
/// ```
#[derive(Debug, Default)]
pub struct InlineAliasesInSelect {}

impl VisitorMut for InlineAliasesInSelect {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = &mut *query.body {
            let mut alias_expr_map = HashMap::new();

            let mut subquery_idents = HashSet::new();

            for table in &mut select.from {
                //Here we go over all parenthesized subqueries in the FROM clause. Ex: SELECT * FROM `(SELECT * FROM table)`, `(SELECT * FROM table)`
                if let TableFactor::Derived { subquery, .. } = &mut table.relation {
                    //Here we go over all SELECTS & UNIONs in the parentheses. Ex: SELECT * FROM `(SELECT * FROM table UNION ALL SELECT * FROM table)`
                    traverse_set_expr(&mut subquery_idents, &subquery.body);
                }
            }

            for item in &mut select.projection {
                match item {
                    SelectItem::ExprWithAlias { expr, alias } => {
                        //Don't substitute aliases for the same alias & subquery idents
                        substitute_aliases(
                            expr,
                            &alias_expr_map,
                            Some(&alias.value),
                            Some(&|e| contains_ident_value(&subquery_idents, e)),
                        );
                        //Don't add to a substitution map if the alias is the same as the subquery ident
                        if !subquery_idents.contains(&alias.value) {
                            alias_expr_map.insert(alias.value.clone(), expr.clone());
                        }
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        //Don't substitute subquery idents
                        substitute_aliases(
                            expr,
                            &alias_expr_map,
                            None,
                            Some(&|e| contains_ident_value(&subquery_idents, e)),
                        );
                    }
                    _ => {}
                }
            }

            // Rewrite WHERE
            if let Some(selection) = select.selection.as_mut() {
                //NOTE: if other aggregate functions happen (without over) - we have no way of knowing,
                // like just calling last_value with an alias,
                // perhaps this will need to be extended in the logical planning phase later
                substitute_aliases(
                    selection,
                    &alias_expr_map,
                    None,
                    //Just a precation, not sure if we need to check with teh subquery here
                    Some(&|e| {
                        matches!(e, Expr::Function(Function { over: Some(_), .. }))
                            || contains_ident_value(&subquery_idents, e)
                    }),
                );
            }

            // Rewrite QUALIFY
            if let Some(qualify) = select.qualify.as_mut() {
                //Just a precation, not sure if we need to check with teh subquery here
                substitute_aliases(
                    qualify,
                    &alias_expr_map,
                    None,
                    Some(&|e| contains_ident_value(&subquery_idents, e)),
                );
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

fn contains_ident_value(subquery_idents: &HashSet<String>, expr: &Expr) -> bool {
    if let Expr::Identifier(ident) = expr {
        subquery_idents.contains(&ident.value)
    } else {
        false
    }
}

/// Recursively traverses the subquery to find all identifiers
fn traverse_set_expr(subquery_idents: &mut HashSet<String>, set_expr: &SetExpr) {
    //Recursion shouldn't be an issue, since we only traverse one level of the subquery (one level of parentheses)
    match set_expr {
        SetExpr::Select(select) => {
            select.projection.iter().for_each(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    subquery_idents.insert(alias.value.clone());
                }
                SelectItem::UnnamedExpr(expr) => {
                    if let Expr::Identifier(ident) = expr {
                        subquery_idents.insert(ident.value.clone());
                    }
                }
                _ => {}
            });
        }
        SetExpr::SetOperation {
            op, left, right, ..
        } if op == &SetOperator::Union => {
            let () = traverse_set_expr(subquery_idents, left);
            let () = traverse_set_expr(subquery_idents, right);
        }
        SetExpr::Query(query) => {
            let () = traverse_set_expr(subquery_idents, &query.body);
        }
        _ => {}
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut InlineAliasesInSelect {});
}
