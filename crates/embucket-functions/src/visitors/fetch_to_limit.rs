use datafusion::logical_expr::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::{
    LimitClause, Offset, OffsetRows, Query, Statement, VisitorMut,
};
use datafusion::sql::sqlparser::parser::ParserError;
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct FetchToLimit {}

/// A visitor that changes FETCH behavior to LIMIT in Snowflake SQL.
/// This visitor rewrites Snowflake SQL `SELECT` statements by replacing the `FETCH` statement with the `LIMIT` statement.
/// They are equivalent in Snowflake SQL.
impl VisitorMut for FetchToLimit {
    type Break = ParserError;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(fetch) = query.fetch.take() {
            if let Some(quantity) = fetch.quantity {
                // Preserve existing OFFSET (and LIMIT BY) if present
                let (existing_offset, existing_limit_by) = match &query.limit_clause {
                    Some(LimitClause::LimitOffset { offset, limit_by, .. }) => {
                        (offset.clone(), limit_by.clone())
                    }
                    Some(LimitClause::OffsetCommaLimit { offset, .. }) => {
                        (
                            Some(Offset {
                                value: offset.clone(),
                                rows: OffsetRows::None,
                            }),
                            vec![],
                        )
                    }
                    None => (None, vec![]),
                };

                query.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(quantity),
                    offset: existing_offset,
                    limit_by: existing_limit_by,
                });
            } else {
                return ControlFlow::Break(ParserError::ParserError(
                    "FETCH requires a quantity to be specified. The number of rows returned must be a non-negative integer constant.".to_string(),
                ));
            }
        }

        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) -> Result<(), ParserError> {
    match stmt.visit(&mut FetchToLimit {}) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}
