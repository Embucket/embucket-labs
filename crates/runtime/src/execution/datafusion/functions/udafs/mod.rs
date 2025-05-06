use datafusion::prelude::SessionContext;

pub mod any_value;
pub mod object_agg;

pub fn register_udafs(ctx: &mut SessionContext) -> datafusion_common::Result<()> {
    any_value::register_udaf(ctx);
    object_agg::register_udaf(ctx);
    Ok(())
}

mod macros {
    macro_rules! make_udaf_function {
        ($udaf_type:ty) => {
            paste::paste! {
                pub fn register_udaf(ctx: &mut datafusion::prelude::SessionContext) {
                    ctx.register_udaf(datafusion_expr::AggregateUDF::from(<$udaf_type>::default()));
                }
            }
        };
    }

    pub(crate) use make_udaf_function;
}
