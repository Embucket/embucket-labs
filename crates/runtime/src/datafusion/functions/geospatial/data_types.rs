use std::sync::Arc;

use crate::datafusion::functions::geospatial::error::{
    self as geo_error, GeoDataFusionError, GeoDataFusionResult,
};
use arrow_array::ArrayRef;
use datafusion::error::DataFusionError;
use geoarrow::array::{CoordType, GeometryArray, LineStringArray, PointArray, RectArray};
use geoarrow::datatypes::{Dimension, NativeType};
use geoarrow::NativeArray;
use snafu::ResultExt;

pub const POINT2D_TYPE: NativeType = NativeType::Point(CoordType::Separated, Dimension::XY);
pub const POINT3D_TYPE: NativeType = NativeType::Point(CoordType::Separated, Dimension::XYZ);
pub const BOX2D_TYPE: NativeType = NativeType::Rect(Dimension::XY);
pub const BOX3D_TYPE: NativeType = NativeType::Rect(Dimension::XYZ);
pub const GEOMETRY_TYPE: NativeType = NativeType::Geometry(CoordType::Separated);
pub const LINE_STRING_TYPE: NativeType =
    NativeType::LineString(CoordType::Separated, Dimension::XY);

/// This will not cast a `PointArray` to a `GeometryArray`
pub fn parse_to_native_array(array: &ArrayRef) -> GeoDataFusionResult<Arc<dyn NativeArray>> {
    let data_type = array.data_type();
    if data_type.equals_datatype(&POINT2D_TYPE.into()) {
        let point_array = PointArray::try_from((array.as_ref(), Dimension::XY))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&POINT3D_TYPE.into()) {
        let point_array = PointArray::try_from((array.as_ref(), Dimension::XYZ))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&LINE_STRING_TYPE.into()) {
        let point_array = LineStringArray::try_from((array.as_ref(), Dimension::XY))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(point_array))
    } else if data_type.equals_datatype(&BOX2D_TYPE.into()) {
        let rect_array = RectArray::try_from((array.as_ref(), Dimension::XY))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&BOX3D_TYPE.into()) {
        let rect_array = RectArray::try_from((array.as_ref(), Dimension::XYZ))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&GEOMETRY_TYPE.into()) {
        Ok(Arc::new(
            GeometryArray::try_from(array.as_ref()).context(geo_error::GeoArrowSnafu)?,
        ))
    } else {
        Err(GeoDataFusionError::DataFusion {
            source: DataFusionError::Execution(format!("Unexpected input data type: {data_type}")),
        })
    }
}
