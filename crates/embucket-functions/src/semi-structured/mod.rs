pub mod array;
pub mod get;
pub mod get_path;
pub mod json;
pub mod object;
pub mod variant;

use crate::semi_structured::array::*;
use crate::semi_structured::json::*;
use crate::semi_structured::object::*;
use crate::semi_structured::variant::*;
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
        array_flatten::get_udf(),
        array_to_string::get_udf(),
        Arc::new(ScalarUDF::from(ObjectConstructUDF::new(true))),
        Arc::new(ScalarUDF::from(ObjectConstructUDF::new(false))),
        as_func::get_udf(),
        to_array::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
