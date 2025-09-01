pub mod array;
pub mod errors;
pub mod get;
pub mod get_path;
pub mod object;
pub mod variant;
pub use errors::Error;

pub mod typeof_func;

mod conversion;
pub mod is_typeof;
pub mod parse_json;

use crate::semi_structured::array::{
    array_append, array_cat, array_compact, array_construct, array_contains, array_distinct,
    array_except, array_flatten, array_generate_range, array_insert, array_intersection, array_max,
    array_min, array_position, array_prepend, array_remove, array_remove_at, array_reverse,
    array_size, array_slice, array_sort, array_to_string, arrays_overlap, arrays_to_object,
    arrays_zip,
};
use crate::semi_structured::get::GetFunc;
use crate::semi_structured::is_typeof::IsTypeofFunc;
use crate::semi_structured::object::object_construct::ObjectConstructUDF;
use crate::semi_structured::object::{object_delete, object_insert, object_pick};
use crate::semi_structured::parse_json::ParseJsonFunc;
use crate::semi_structured::variant::variant_element;
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
        array_flatten::get_udf(),
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
        array_to_string::get_udf(),
        arrays_overlap::get_udf(),
        arrays_to_object::get_udf(),
        arrays_zip::get_udf(),
        conversion::as_func::get_udf(),
        get_path::get_udf(),
        array::is_array::get_udf(),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Array))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Boolean))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Double))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Decimal))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Integer))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Null))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Object))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::Real))),
        Arc::new(ScalarUDF::from(IsTypeofFunc::new(is_typeof::Kind::String))),
        object::is_object::get_udf(),
        Arc::new(ScalarUDF::from(ObjectConstructUDF::new(false))),
        Arc::new(ScalarUDF::from(ObjectConstructUDF::new(true))),
        object_delete::get_udf(),
        object_insert::get_udf(),
        object::object_keys::get_udf(),
        object_pick::get_udf(),
        array::strtok_to_array::get_udf(),
        Arc::new(ScalarUDF::from(ParseJsonFunc::new(false))),
        Arc::new(ScalarUDF::from(ParseJsonFunc::new(true))),
        Arc::new(ScalarUDF::from(GetFunc::new(false))),
        Arc::new(ScalarUDF::from(GetFunc::new(true))),
        typeof_func::get_udf(),
        variant_element::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
