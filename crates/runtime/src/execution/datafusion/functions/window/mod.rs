use std::sync::Arc;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{WindowUDF};

pub mod any_value;


pub fn register_udwfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let window_functions: Vec<Arc<WindowUDF>> = vec![
        any_value::get_udwf(),
    ];

    for func in window_functions {
        registry.register_udwf(func)?;
    }

    Ok(())
}

mod macros {
    macro_rules! make_udwf_function {
    ($udwf_type:ty) => {
        paste::paste! {
            static [< STATIC_ $udwf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::WindowUDF>> =
                std::sync::OnceLock::new();

            pub fn get_udwf() -> std::sync::Arc<datafusion::logical_expr::WindowUDF> {
                [< STATIC_ $udwf_type:upper >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::WindowUDF::new_from_impl(
                            <$udwf_type>::default(),
                        ))
                    })
                    .clone()
            }
        }
    }
}

    pub(crate) use make_udwf_function;
}