use datafusion_expr::WindowUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod conditional_true_event;

pub fn register_udwfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let window_functions: Vec<Arc<WindowUDF>> = vec![conditional_true_event::get_udwf()];
    for func in window_functions {
        registry.register_udwf(func)?;
    }
    Ok(())
}

mod macros {
    macro_rules! make_udwf_function {
        ($udwf_type:ty) => {
            paste::paste! {
                static [< STATIC_ $udwf_type:upper >]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::WindowUDF>> = std::sync::OnceLock::new();

                pub fn get_udwf() -> std::sync::Arc<datafusion_expr::WindowUDF> {
                    [< STATIC_ $udwf_type:upper >]
                        .get_or_init(|| {
                            std::sync::Arc::new(datafusion_expr::WindowUDF::new_from_impl(<$udwf_type>::default()))
                        })
                        .clone()
                }
            }
        };
    }
    pub(crate) use make_udwf_function;
}
