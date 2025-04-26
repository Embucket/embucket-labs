mod dim;
mod geometry;
mod line_string;
mod point;
mod srid;

use datafusion::prelude::SessionContext;

/// Register all provided [geo] functions for constructing geometries
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(dim::GeomDimension::new().into());
    ctx.register_udf(line_string::StartPoint::new().into());
    ctx.register_udf(line_string::EndPoint::new().into());
    ctx.register_udf(line_string::PointN::new().into());
    ctx.register_udf(srid::Srid::new().into());
    ctx.register_udf(point::PointX::new().into());
    ctx.register_udf(point::PointY::new().into());
    ctx.register_udf(geometry::MinX::new().into());
    ctx.register_udf(geometry::MinY::new().into());
    ctx.register_udf(geometry::MaxX::new().into());
    ctx.register_udf(geometry::MaxY::new().into());
}
