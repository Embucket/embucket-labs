use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{BinaryOperator, Expr, Statement, VisitorMut};
use std::ops::{ControlFlow, Deref};

#[derive(Debug, Default)]
pub struct LikeAny;

impl VisitorMut for LikeAny {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Like {
            negated,
            any,
            expr: inner_expr,
            pattern,
            escape_char,
        } = expr
            && *any
        {
            if let Expr::Tuple(patterns) = pattern.deref().deref() {
                let mut new_expr = Expr::Like {
                    negated: *negated,
                    any: false,
                    expr: inner_expr.clone(),
                    pattern: Box::new(patterns[patterns.len() - 1].clone()),
                    escape_char: escape_char.clone(),
                };
                //For even number of patterns in `any` its: patterns / 2 = binary expr of like or
                //For ood number of patterns in `any` its: (patterns / 2) + 1 = binary expr of like or
                patterns.iter().rev().skip(1).for_each(|expr| {
                    new_expr = Expr::BinaryOp {
                        left: Box::new(Expr::Like {
                            negated: *negated,
                            any: false,
                            expr: inner_expr.clone(),
                            pattern: Box::new(expr.clone()),
                            escape_char: escape_char.clone(),
                        }),
                        op: BinaryOperator::Or,
                        right: Box::new(new_expr.clone()),
                    };
                });
                *expr = new_expr;
            } else if let Expr::Nested(pattern) = pattern.deref().deref() {
                let new_expr = Expr::Like {
                    negated: *negated,
                    any: false,
                    expr: inner_expr.clone(),
                    pattern: pattern.clone(),
                    escape_char: escape_char.clone(),
                };
                *expr = new_expr;
            }
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut LikeAny {});
}
