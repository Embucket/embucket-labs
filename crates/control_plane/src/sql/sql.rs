use crate::sql::context::CustomContextProvider;
use crate::sql::planner::ExtendedSqlToRel;
use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::Statement;
use std::collections::HashMap;

pub async fn sql_query(ctx: SessionContext, query: &String) -> Result<Vec<RecordBatch>> {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect.as_str();
    let statement = state.sql_to_statement(query, dialect)?;

    if let DFStatement::Statement(s) = statement {
        if let Statement::CreateTable { .. } = *s {
            let ctx_provider = CustomContextProvider {
                state: &state,
                tables: HashMap::new(),
            };
            let planner = ExtendedSqlToRel::new(&ctx_provider);
            let plan = planner.sql_statement_to_plan(*s);
            let plan = plan?;

            return ctx.execute_logical_plan(plan).await?.collect().await;
        }
    }

    ctx.sql(query).await?.collect().await
}
