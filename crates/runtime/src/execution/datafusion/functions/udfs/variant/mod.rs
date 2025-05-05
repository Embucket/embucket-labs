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

mod json;
pub mod array_append;
pub mod array_cat;
pub mod array_compact;
pub mod array_construct;
pub mod array_contains;
pub mod array_distinct;
pub mod array_except;
pub mod array_generate_range;
pub mod array_insert;
pub mod array_intersection;
pub mod array_max;
pub mod array_min;
pub mod array_position;
pub mod array_prepend;
pub mod array_remove;
pub mod array_remove_at;
pub mod array_reverse;
pub mod array_size;
pub mod array_slice;
pub mod array_sort;
pub mod arrays_overlap;
pub mod arrays_to_object;
pub mod arrays_zip;
pub mod variant_element;

use std::sync::Arc;
use datafusion::{common::Result, execution::FunctionRegistry, logical_expr::ScalarUDF};

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        array_append::get_udf(),
        array_cat::get_udf(),
        array_compact::get_udf(),
        array_construct::get_udf(),
        array_contains::get_udf(),
        array_distinct::get_udf(),
        array_except::get_udf(),
        array_generate_range::get_udf(),
        array_insert::get_udf(),
        array_intersection::get_udf(),
        array_max::get_udf(),
        array_min::get_udf(),
        array_position::get_udf(),
        array_prepend::get_udf(),
        array_remove::get_udf(),
        array_remove_at::get_udf(),
        array_reverse::get_udf(),
        array_size::get_udf(),
        array_slice::get_udf(),
        array_sort::get_udf(),
        arrays_overlap::get_udf(),
        arrays_to_object::get_udf(),
        arrays_zip::get_udf(),
        variant_element::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
