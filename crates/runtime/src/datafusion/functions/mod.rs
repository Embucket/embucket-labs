// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::{common::Result, execution::FunctionRegistry, logical_expr::ScalarUDF};
use sqlparser::ast::Value::SingleQuotedString;
use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};

mod convert_timezone;
mod date_add;
mod date_diff;
mod date_from_parts;
mod parse_json;
mod time_from_parts;
mod timestamp_from_parts;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        convert_timezone::get_udf(),
        date_add::get_udf(),
        parse_json::get_udf(),
        date_diff::get_udf(),
        timestamp_from_parts::get_udf(),
        time_from_parts::get_udf(),
        date_from_parts::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}

mod macros {
    macro_rules! make_udf_function {
    ($udf_type:ty) => {
        paste::paste! {
            static [< STATIC_ $udf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
                std::sync::OnceLock::new();

            pub fn get_udf() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
                [< STATIC_ $udf_type:upper >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                            <$udf_type>::default(),
                        ))
                    })
                    .clone()
            }
        }
    }
}

    pub(crate) use make_udf_function;
}

pub fn visit_functions_expressions(func: &mut Function) {
    let func_name_string = func.name.clone().to_string().to_lowercase();
    let func_name = func_name_string.as_str();
    let args = &mut func.args;
    let name = match func_name {
        "year" | "day" | "dayofmonth" | "dayofweek" | "dayofweekiso" | "dayofyear" | "week"
        | "weekofyear" | "weekiso" | "month" | "quarter" | "hour" | "minute" | "second" => {
            if let FunctionArguments::List(arg_list) = args {
                let arg = match func_name {
                    "year" | "quarter" | "month" | "week" | "day" | "hour" | "minute"
                    | "second" => func_name,
                    "dayofyear" => "doy",
                    "dayofweek" => "dow",
                    "dayofmonth" => "day",
                    "weekofyear" => "week",
                    _ => "unknown",
                };
                arg_list.args.insert(
                    0,
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(SingleQuotedString(
                        arg.to_string(),
                    )))),
                );
            };
            "date_part"
        }
        _ => func_name,
    };
    func.name = sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new(name)]);
}
