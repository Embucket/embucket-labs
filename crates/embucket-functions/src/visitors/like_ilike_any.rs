use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{BinaryOperator, Expr, Statement, Value, VisitorMut};
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct LikeILikeAny;

impl LikeILikeAny {
    fn new_expr(
        negated: bool,
        inner_expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<Value>,
        case_sensitive: bool,
    ) -> Expr {
        if let Expr::Tuple(patterns) = &*pattern {
            if case_sensitive {
                let mut new_expr = Expr::ILike {
                    negated,
                    any: false,
                    expr: inner_expr.clone(),
                    pattern: Box::new(patterns[patterns.len() - 1].clone()),
                    escape_char: escape_char.clone(),
                };
                //For even number of patterns in `any` its: patterns / 2 = binary expr of like or
                //For ood number of patterns in `any` its: (patterns / 2) + 1 = binary expr of like or
                patterns.iter().rev().skip(1).for_each(|pattern| {
                    new_expr = Expr::BinaryOp {
                        left: Box::new(Expr::ILike {
                            negated,
                            any: false,
                            expr: inner_expr.clone(),
                            pattern: Box::new(pattern.clone()),
                            escape_char: escape_char.clone(),
                        }),
                        op: BinaryOperator::Or,
                        right: Box::new(new_expr.clone()),
                    };
                });
                new_expr
            } else {
                let mut new_expr = Expr::Like {
                    negated,
                    any: false,
                    expr: inner_expr.clone(),
                    pattern: Box::new(patterns[patterns.len() - 1].clone()),
                    escape_char: escape_char.clone(),
                };
                //For even number of patterns in `any` its: patterns / 2 = binary expr of like or
                //For ood number of patterns in `any` its: (patterns / 2) + 1 = binary expr of like or
                patterns.iter().rev().skip(1).for_each(|pattern| {
                    new_expr = Expr::BinaryOp {
                        left: Box::new(Expr::Like {
                            negated,
                            any: false,
                            expr: inner_expr.clone(),
                            pattern: Box::new(pattern.clone()),
                            escape_char: escape_char.clone(),
                        }),
                        op: BinaryOperator::Or,
                        right: Box::new(new_expr.clone()),
                    };
                });
                new_expr
            }
        } else if let Expr::Nested(pattern) = &*pattern {
            if case_sensitive {
                Expr::ILike {
                    negated,
                    any: false,
                    expr: inner_expr,
                    pattern: pattern.clone(),
                    escape_char,
                }
            } else {
                Expr::Like {
                    negated,
                    any: false,
                    expr: inner_expr,
                    pattern: pattern.clone(),
                    escape_char,
                }
            }
        } else {
            *pattern
        }
    }
}

impl VisitorMut for LikeILikeAny {
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
            *expr = Self::new_expr(
                *negated,
                inner_expr.clone(),
                pattern.clone(),
                escape_char.clone(),
                false,
            );
        } else if let Expr::ILike {
            negated,
            any,
            expr: inner_expr,
            pattern,
            escape_char,
        } = expr
            && *any
        {
            *expr = Self::new_expr(
                *negated,
                inner_expr.clone(),
                pattern.clone(),
                escape_char.clone(),
                true,
            );
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut LikeILikeAny {});
}
