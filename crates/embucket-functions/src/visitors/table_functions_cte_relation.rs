use crate::visitors::{query_with_body, select_with_body};
use datafusion::logical_expr::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, SelectItem, SetExpr, TableFactor,
};
use datafusion::sql::sqlparser::ast::{Query, TableAlias};
use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{Statement, VisitorMut};
use std::collections::HashMap;
use std::ops::ControlFlow;

#[derive(Debug, Default)]
pub struct TableFuncInlineCte {
    ctes: HashMap<String, Query>,
    current_from_tables: Vec<TableFactor>,
}

impl VisitorMut for TableFuncInlineCte {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if let Some(with) = &mut query.with {
            for cte in &with.cte_tables {
                self.ctes
                    .insert(cte.alias.name.value.clone(), (*cte.query).clone());
            }
        }
        if let SetExpr::Select(select) = &*query.body {
            self.current_from_tables = select.from.iter().map(|f| f.relation.clone()).collect();
        } else {
            self.current_from_tables.clear();
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<()> {
        match table_factor {
            TableFactor::Function { name, args, .. }
                if name.to_string().eq_ignore_ascii_case("flatten") =>
            {
                *args = self.replace_flatten_args(args);
            }
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl TableFuncInlineCte {
    pub fn replace_flatten_args(&mut self, args: &mut [FunctionArg]) -> Vec<FunctionArg> {
        args.iter()
            .map(|arg| match arg {
                FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(expr),
                    operator,
                } if name.to_string().eq_ignore_ascii_case("input") => {
                    let new_expr = self.replace_expr(expr.clone());
                    FunctionArg::Named {
                        name: name.clone(),
                        arg: FunctionArgExpr::Expr(new_expr),
                        operator: operator.clone(),
                    }
                }
                other => other.clone(), // Unnamed or non-Expr args
            })
            .collect()
    }
    fn replace_expr(&self, expr: Expr) -> Expr {
        match expr {
            Expr::Function(mut func) => {
                func.args = match func.args {
                    FunctionArguments::List(mut arg_list) => {
                        arg_list.args = arg_list
                            .args
                            .into_iter()
                            .map(|arg| match arg {
                                FunctionArg::Named {
                                    name,
                                    arg: FunctionArgExpr::Expr(inner),
                                    operator,
                                } => FunctionArg::Named {
                                    name,
                                    arg: FunctionArgExpr::Expr(self.replace_expr(inner)),
                                    operator,
                                },
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        self.replace_expr(inner),
                                    ))
                                }
                                other => other,
                            })
                            .collect();
                        FunctionArguments::List(arg_list)
                    }
                    other => other,
                };
                Expr::Function(func)
            }
            Expr::Identifier(ident) => {
                let column = &ident.value;

                if let Some((alias, query)) = self.extract_projected_columns(column) {
                    let relation = TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(query.clone()),
                        alias: Some(TableAlias {
                            name: Ident::new(alias.clone()),
                            columns: vec![],
                        }),
                    };
                    let projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
                        column.clone(),
                    )))];
                    let subquery = select_with_body(projection, relation, None);
                    return Expr::Subquery(Box::new(query_with_body(subquery)));
                }
                Expr::Identifier(ident)
            }
            other => other,
        }
    }

    fn extract_projected_columns(&self, column: &String) -> Option<(&String, &Query)> {
        // Search for CTEs that are defined on the same level as the current FROM tables
        let cte_names_on_same_level: Vec<String> = self
            .current_from_tables
            .iter()
            .filter_map(|tf| match tf {
                TableFactor::Table { name, .. } | TableFactor::Function { name, .. } => {
                    let table_name = name.to_string();
                    if self.ctes.contains_key(&table_name) {
                        Some(table_name)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

        // Check if the column is a part of any CTE's projection on the same level
        for cte in cte_names_on_same_level.clone() {
            if let Some((alias, query)) = self.ctes.get_key_value(&cte) {
                if let SetExpr::Select(select) = &*query.body {
                    let mut columns = vec![];
                    for item in &select.projection {
                        match item {
                            SelectItem::ExprWithAlias { alias, .. } => {
                                columns.push(alias.value.clone());
                            }
                            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                                columns.push(ident.value.clone());
                            }
                            _ => {}
                        }
                    }
                    if columns.iter().any(|c| c == column) {
                        return Some((alias, query));
                    }
                }
            }
        }
        if !cte_names_on_same_level.is_empty() {
            return self.ctes.get_key_value(&cte_names_on_same_level[0]);
        }
        None
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut TableFuncInlineCte::default());
}
