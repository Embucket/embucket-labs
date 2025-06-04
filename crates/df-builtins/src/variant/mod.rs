pub mod array_append;
pub mod array_cat;
pub mod array_compact;
pub mod array_construct;
pub mod array_contains;
pub mod array_distinct;
pub mod array_except;
pub mod array_flatten;
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
pub mod array_to_string;
pub mod arrays_overlap;
pub mod arrays_to_object;
pub mod arrays_zip;
pub mod as_func;
pub mod object_construct;
pub mod object_delete;
pub mod object_insert;
pub mod object_pick;
pub mod to_array;
pub mod variant_element;
pub mod visitors;

use datafusion::common::Result;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

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
        object_delete::get_udf(),
        object_insert::get_udf(),
        object_pick::get_udf(),
        object_construct::get_udf(),
        array_flatten::get_udf(),
        array_to_string::get_udf(),
        as_func::get_udf(),
        to_array::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
