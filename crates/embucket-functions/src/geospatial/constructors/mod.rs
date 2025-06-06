pub mod line_string;
pub mod polygon;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for constructing geometries
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(line_string::MakeLine::new().into());
    ctx.register_udf(polygon::MakePolygon::new().into());
}
