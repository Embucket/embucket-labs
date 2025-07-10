use crate::visitors::{query_with_body, select_with_body};
use datafusion::logical_expr::sqlparser::ast::{Ident, Select, SelectItem, TableFactor};
use datafusion::sql::sqlparser::ast::{Expr, Query, SetExpr};
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{Statement, VisitorMut};
use std::ops::ControlFlow;

/// A visitor that rewrites SQL queries containing `QUALIFY` clauses into standard
/// SQL constructs that are compatible with engines lacking native `QUALIFY` support.
///
/// # Behavior
/// This visitor walks through the SQL AST (`Query`) and transforms any `SELECT` statement
/// containing a `QUALIFY` clause into an equivalent query using subqueries with
/// `ROW_NUMBER()` exposed as a named projection and filtered via `WHERE`.
///
/// ## Transformation Rules:
/// - If a `SELECT` contains both `QUALIFY` and `WHERE`, the entire `SELECT` is wrapped
///   into a subquery (excluding the `QUALIFY`), and then the `QUALIFY` filter is applied
///   on the outer query.
/// - If a `SELECT` contains only `QUALIFY`, it is directly transformed by:
///   - removing the `QUALIFY` clause,
///   - adding the expression (e.g. `ROW_NUMBER() OVER ...`) to the projection as `qualify_alias`,
///   - wrapping the `SELECT` into a subquery,
///   - applying a `WHERE qualify_alias <op> <value>` filter in the outer query.
///
/// ## Example:
/// ```sql
/// -- Input:
/// SELECT * FROM table
/// WHERE some_condition
/// QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1
///
/// -- Output:
/// SELECT * FROM (
///     SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS qualify_alias
///     FROM table
///     WHERE some_condition
/// ) WHERE qualify_alias = 1
/// ```
///
/// This transformation makes it possible to execute `QUALIFY`-style logic in SQL engines
/// that support window functions but do not support `QUALIFY`.
#[derive(Debug, Default)]
pub struct QualifyInQuery;

impl QualifyInQuery {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl VisitorMut for QualifyInQuery {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(with) = query.with.as_mut() {
            for cte in &mut with.cte_tables {
                match self.pre_visit_query(&mut cte.query) {
                    ControlFlow::Break(b) => return ControlFlow::Break(b),
                    ControlFlow::Continue(()) => {}
                }
            }
        }

        match query.body.as_mut() {
            SetExpr::Select(select) => {
                if let Some(Expr::BinaryOp { left, op, right }) = select.qualify.as_ref() {
                    let mut inner_select = if select.selection.is_some() {
                        Box::from(wrap_select_in_subquery(select, None))
                    } else {
                        select.clone()
                    };
                    inner_select.qualify = None;
                    inner_select.projection.push(SelectItem::ExprWithAlias {
                        expr: *(left.clone()),
                        alias: Ident::new("qualify_alias".to_string()),
                    });
                    let outer_selection = Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("qualify_alias"))),
                        op: op.clone(),
                        right: Box::new(*right.clone()),
                    });
                    let outer_select =
                        Box::new(wrap_select_in_subquery(&inner_select, outer_selection));
                    *query.body = SetExpr::Select(outer_select);
                }
            }
            SetExpr::Query(inner_query) => match self.pre_visit_query(inner_query) {
                ControlFlow::Break(b) => return ControlFlow::Break(b),
                ControlFlow::Continue(()) => {}
            },
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut QualifyInQuery::new());
}

#[must_use]
pub fn wrap_select_in_subquery(select: &Select, selection: Option<Expr>) -> Select {
    let mut inner_select = select.clone();
    inner_select.qualify = None;

    let subquery = query_with_body(inner_select);
    let projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("*")))];
    let relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(subquery),
        alias: None,
    };
    select_with_body(projection, relation, selection)
}
