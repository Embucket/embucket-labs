use datafusion::logical_expr::sqlparser::ast::{Expr, TableFactor, VisitMut};
use datafusion::sql::sqlparser::ast::{
    Function, FunctionArguments, Query, SetExpr, Statement, VisitorMut,
};
use std::ops::ControlFlow;

/// A SQL AST visitor that rewrites `TABLE(<FUNCTION>(...))` table functions
/// into `<FUNCTION>(...)` by removing the unnecessary `TABLE(...)` wrapper.
///
/// This transformation is useful because in many SQL dialects, especially Snowflake-like syntax,
/// queries such as:
///
/// ```sql
/// SELECT * FROM TABLE(<FUNCTION>(LAST_QUERY_ID())) WHERE value > 1;
/// ```
///
/// are semantically equivalent to:
///
/// ```sql
/// SELECT * FROM <FUNCTION>(LAST_QUERY_ID()) WHERE value > 1;
/// ```
///
/// However, the presence of the `TABLE(...)` wrapper can complicate query parsing
/// or downstream analysis in some tools, such as logical planners or optimizers.
/// This visitor simplifies the AST by stripping the redundant `TABLE(...)`
/// call when it wraps a single `<FUNCTION>(...)` function call.
///
/// # How it works:
/// - It traverses SQL `Query` nodes in the AST.
/// - For each `FROM` clause entry that is a `TableFactor::TableFunction`, it checks whether the expression is:
///     - A function call named `TABLE`,
///     - With exactly one argument,
///     - And that argument is a function call named `<FUNCTION>`.
/// - If all conditions are met, it replaces the outer `TABLE(...)` function expression
///   with the inner `<FUNCTION>(...)` function directly.
///
/// This transformation is performed in-place using the `VisitorMut` trait.
#[derive(Debug, Default)]
pub struct TableFunctionVisitor {}

impl VisitorMut for TableFunctionVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for item in &mut select.from {
                if let TableFactor::TableFunction {
                    expr:
                        Expr::Function(Function {
                            name,
                            args: FunctionArguments::List(args),
                            ..
                        }),
                    alias,
                } = &mut item.relation
                {
                    let func_name = name.to_string();
                    if matches!(func_name.to_lowercase().as_str(), "flatten") {
                        item.relation = TableFactor::Function {
                            name: name.clone(),
                            args: args.args.clone(),
                            alias: alias.clone(),
                            lateral: false,
                        };
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut TableFunctionVisitor {});
}
