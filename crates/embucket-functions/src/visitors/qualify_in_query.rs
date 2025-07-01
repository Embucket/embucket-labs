use datafusion::logical_expr::sqlparser;
use datafusion::logical_expr::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::logical_expr::sqlparser::ast::{
    GroupByExpr, Ident, Select, SelectItem, TableFactor, TableWithJoins,
};
use datafusion::sql::sqlparser::ast::{Expr, Query, SetExpr};
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{Statement, VisitorMut};
use std::ops::ControlFlow;

/// Visitor that converts BINARY and VARBINARY types to BYTEA in CREATE TABLE statements
///
/// This allows `DataFusion` to support standard SQL BINARY types by converting them to
/// the supported BYTEA type, which maps to `DataType::Binary` internally.
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

    let subquery = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(inner_select))),
        order_by: None,
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    };

    Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection: vec![SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(
            Ident::new("*"),
        ))],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(subquery),
                alias: None,
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        prewhere: None,
        selection,
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: None,
        flavor: sqlparser::ast::SelectFlavor::Standard,
    }
}
