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

use datafusion_expr::AggregateUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod any_value;
pub mod array_unique_agg;
pub mod booland_agg;
pub mod boolor_agg;
pub mod boolxor_agg;
pub mod percentile_cont;

pub fn register_udafs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let aggregate_functions: Vec<Arc<AggregateUDF>> = vec![
        any_value::get_udaf(),
        booland_agg::get_udaf(),
        boolor_agg::get_udaf(),
        boolxor_agg::get_udaf(),
        percentile_cont::get_udaf(),
        array_unique_agg::get_udaf(),
    ];

    for func in aggregate_functions {
        registry.register_udaf(func)?;
    }

    Ok(())
}

mod macros {
    macro_rules! make_udaf_function {
        ($udaf_type:ty) => {
        paste::paste! {
            static [< STATIC_ $udaf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> =
                std::sync::OnceLock::new();

            pub fn get_udaf() -> std::sync::Arc<datafusion::logical_expr::AggregateUDF> {
                [< STATIC_ $udaf_type:upper >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::AggregateUDF::new_from_impl(
                            <$udaf_type>::default(),
                        ))
                    })
                    .clone()
            }
        }
    }
    }

    pub(crate) use make_udaf_function;
}
