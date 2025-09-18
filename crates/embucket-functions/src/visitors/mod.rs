use datafusion::logical_expr::sqlparser;
use datafusion::logical_expr::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::logical_expr::sqlparser::ast::{
    Expr, GroupByExpr, Query, SelectItem, SetExpr, TableFactor, TableWithJoins,
};
use datafusion::sql::sqlparser::ast::Select;

pub mod copy_into_identifiers;
pub mod fetch_to_limit;
pub mod functions_rewriter;
pub mod inline_aliases_in_query;
pub mod json_element;
pub mod like_ilike_any;
pub mod rlike_regexp_expr_rewriter;
pub mod select_expr_aliases;
pub mod table_functions;
pub mod table_functions_cte_relation;
pub mod timestamp;
pub mod top_limit;
pub mod unimplemented;

#[must_use]
pub fn query_with_body(inner_select: Select) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(inner_select))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }
}

#[must_use]
pub fn select_with_body(
    projection: Vec<SelectItem>,
    relation: TableFactor,
    selection: Option<Expr>,
) -> Select {
    Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection,
        exclude: None,
        into: None,
        from: vec![TableWithJoins {
            relation,
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
