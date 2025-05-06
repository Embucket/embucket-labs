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
mod json;
//pub mod object_construct;
pub mod object_delete;
pub mod object_insert;
pub mod object_pick;
pub mod variant_element;

use datafusion::{common::Result, prelude::SessionContext};

pub fn register_udfs(ctx: &mut SessionContext) -> Result<()> {
    array_append::register_udf(ctx);
    array_cat::register_udf(ctx);
    array_compact::register_udf(ctx);
    array_construct::register_udf(ctx);
    array_contains::register_udf(ctx);
    array_distinct::register_udf(ctx);
    array_except::register_udf(ctx);
    array_generate_range::register_udf(ctx);
    array_insert::register_udf(ctx);
    array_intersection::register_udf(ctx);
    array_max::register_udf(ctx);
    array_min::register_udf(ctx);
    array_position::register_udf(ctx);
    array_prepend::register_udf(ctx);
    array_remove::register_udf(ctx);
    array_remove_at::register_udf(ctx);
    array_reverse::register_udf(ctx);
    array_size::register_udf(ctx);
    array_slice::register_udf(ctx);
    array_sort::register_udf(ctx);
    arrays_overlap::register_udf(ctx);
    arrays_to_object::register_udf(ctx);
    arrays_zip::register_udf(ctx);
    variant_element::register_udf(ctx);
    object_delete::register_udf(ctx);
    object_insert::register_udf(ctx);
    object_pick::register_udf(ctx);
    Ok(())
}
