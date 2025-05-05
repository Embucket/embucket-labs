use std::sync::Arc;

use crate::execution::datafusion::functions::geospatial::error::{
    self as geo_error, GeoDataFusionError, GeoDataFusionResult,
};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow::array::{
    GeometryArray, GeometryCollectionArray, LineStringArray, PointArray, PolygonArray,
    RectArray,
};
use geoarrow::datatypes::{NativeType};
use geoarrow_schema::{CoordType, Dimension};
use geoarrow::NativeArray;
use snafu::ResultExt;

pub const POINT2D_TYPE: NativeType = NativeType::Point(CoordType::Separated, Dimension::XY);
pub const POINT3D_TYPE: NativeType = NativeType::Point(CoordType::Separated, Dimension::XYZ);
pub const BOX2D_TYPE: NativeType = NativeType::Rect(Dimension::XY);
pub const BOX3D_TYPE: NativeType = NativeType::Rect(Dimension::XYZ);
pub const GEOMETRY_TYPE: NativeType = NativeType::Geometry(CoordType::Separated);
pub const GEOMETRY_COLLECTION_TYPE: NativeType =
    NativeType::GeometryCollection(CoordType::Separated, Dimension::XY);

pub const LINE_STRING_TYPE: NativeType =
    NativeType::LineString(CoordType::Separated, Dimension::XY);
pub const POLYGON_2D_TYPE: NativeType = NativeType::Polygon(CoordType::Separated, Dimension::XY);

#[must_use]
pub fn any_single_geometry_type_input() -> Signature {
    Signature::uniform(1, geo_types(), Volatility::Immutable)
}

#[must_use]
pub fn geo_types() -> Vec<DataType> {
    vec![
        POINT2D_TYPE.into(),
        POINT3D_TYPE.into(),
        BOX2D_TYPE.into(),
        BOX3D_TYPE.into(),
        LINE_STRING_TYPE.into(),
        POLYGON_2D_TYPE.into(),
        GEOMETRY_TYPE.into(),
        GEOMETRY_COLLECTION_TYPE.into(),
    ]
}

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
        let line_array = LineStringArray::try_from((array.as_ref(), Dimension::XY))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(line_array))
    } else if data_type.equals_datatype(&BOX2D_TYPE.into()) {
        let rect_array = RectArray::try_from((array.as_ref(), Dimension::XY))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&BOX3D_TYPE.into()) {
        let rect_array = RectArray::try_from((array.as_ref(), Dimension::XYZ))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&POLYGON_2D_TYPE.into()) {
        let rect_array = PolygonArray::try_from((array.as_ref(), Dimension::XY))
            .context(geo_error::GeoArrowSnafu)?;
        Ok(Arc::new(rect_array))
    } else if data_type.equals_datatype(&GEOMETRY_TYPE.into()) {
        Ok(Arc::new(
            GeometryArray::try_from(array.as_ref()).context(geo_error::GeoArrowSnafu)?,
        ))
    } else if data_type.equals_datatype(&GEOMETRY_COLLECTION_TYPE.into()) {
        Ok(Arc::new(
            GeometryCollectionArray::try_from((array.as_ref(), Dimension::XY))
                .context(geo_error::GeoArrowSnafu)?,
        ))
    } else {
        Err(GeoDataFusionError::DataFusion {
            source: DataFusionError::Execution(format!("Unexpected input data type: {data_type}")),
        })
    }
}
