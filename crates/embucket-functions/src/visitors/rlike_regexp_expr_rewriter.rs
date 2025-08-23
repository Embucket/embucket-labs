use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    Statement, VisitorMut,
};
use datafusion_expr::sqlparser::ast::{Function, VisitMut};
use std::ops::ControlFlow;

/// Rewrites `[ NOT ] RLIKE` & `[ NOT ] REGEXP` expressions to `[ NOT ] regexp_like` function calls.
///
/// ## Behavior
/// `[ NOT ] RLIKE` & `[ NOT ] REGEXP` AST Node (it is one) are not supported by the Logical plan, meaning there are no `REGEXP` or `RLIKE` logical nodes,
/// or at least anything with those AST nodes can't produce a logical plan.
///
/// ## Transformation
/// When we get this AST node both of which are the same node, just with the `regexp` parament set to `true` or `false` respectively,
/// we transform it to a `regexp_like` function call (with the unary `[ NOT ]` operation if needed, ti will be made apparent further as to why)
/// same parameters, which are:
/// - `expr` (the inner expr) on which `RLIKE` operates, a column, another function call, a scalar, etc.
/// - `negated` which is if this node has the `NOT` before it (it's not an unary operation here, it's part of the node itself).
/// - `pattern` is the regex pattern on which we try to find the likeness from the `expr`
/// - `regexp` if it is `RLIKE` or is it `REGEXP`, here it doesn't change anything, as it should.
///
/// ## Example
/// `SELECT column1 FROM VALUES
/// ('San Francisco'),
/// ('San Jose'),
/// ('Santa Clara'),
/// ('Sacramento') WHERE column1 RLIKE 'San* [fF].*'`
/// Turns in to
/// `SELECT column1 FROM VALUES
/// ('San Francisco'),
/// ('San Jose'),
/// ('Santa Clara'),
/// ('Sacramento') WHERE regexp_like(column1, 'San* [fF].*')`
/// And the result of both are the same.
#[derive(Debug, Default)]
pub struct RLikeRegexpExprRewriter;

impl VisitorMut for RLikeRegexpExprRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::RLike {
            expr: inner_expr,
            negated,
            pattern,
            ..
        } = expr
        {
            let function = Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new("regexp_like")]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*inner_expr.clone())),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(*pattern.clone())),
                    ],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });
            if *negated {
                *expr = Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(function),
                };
            } else {
                *expr = function;
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut RLikeRegexpExprRewriter {});
}
