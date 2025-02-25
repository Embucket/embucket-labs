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

mod convert_timezone;
mod date_add;
mod date_diff;
mod parse_json;
mod timestamp_from_parts;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        convert_timezone::get_udf(),
        date_add::get_udf(),
        parse_json::get_udf(),
        date_diff::get_udf(),
        timestamp_from_parts::get_udf(),
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
