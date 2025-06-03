mod area;
mod contains;
mod distance;
mod within;

use datafusion::prelude::SessionContext;

pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(area::Area::new().into());
    ctx.register_udf(distance::Distance::new().into());
    ctx.register_udf(contains::Contains::new().into());
    ctx.register_udf(within::Within::new().into());
}
