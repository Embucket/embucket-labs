use std::any::Any;
use std::sync::{Arc, OnceLock};

use crate::execution::datafusion::functions::geospatial::data_types::{
    any_single_geometry_type_input, parse_to_native_array,
};
use datafusion::arrow::array::builder::Int32Builder;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ScalarFunctionArgs;
use geoarrow::array::AsNativeArray;
use geoarrow::datatypes::NativeType;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use geozero::GeozeroGeometry;

#[derive(Debug)]
pub struct Srid {
    signature: Signature,
}

/// Currently, for any value of the geometry type, only SRID 4326 is supported and is returned.
impl Srid {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Srid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_srid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        dim_impl(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the SRID (spatial reference system identifier) of a geometry",
                "ST_SRID(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

macro_rules! build_output_array {
    ($arr:expr) => {{
        let mut output_array = Int32Builder::with_capacity($arr.len());
        for geom in $arr.iter() {
            if let Some(p) = geom {
                output_array.append_value(p.srid().unwrap_or(4326));
            } else {
                output_array.append_null();
            }
        }
        Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
    }};
}

fn dim_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            DataFusionError::Execution("Expected only one argument in ST_SRID".to_string())
        })?;

    let native_array = parse_to_native_array(&array)?;
    let native_array_ref = native_array.as_ref();

    match native_array.data_type() {
        NativeType::Point(_, _) => build_output_array!(native_array_ref.as_point()),
        NativeType::MultiPoint(_, _) => build_output_array!(native_array_ref.as_multi_point()),
        NativeType::LineString(_, _) => build_output_array!(native_array_ref.as_line_string()),
        NativeType::MultiLineString(_, _) => {
            build_output_array!(native_array_ref.as_multi_line_string())
        }
        NativeType::Polygon(_, _) => build_output_array!(native_array_ref.as_polygon()),
        NativeType::MultiPolygon(_, _) => {
            build_output_array!(native_array_ref.as_multi_polygon())
        }
        NativeType::Geometry(_) => build_output_array!(native_array_ref.as_geometry()),
        NativeType::GeometryCollection(_, _) => {
            build_output_array!(native_array_ref.as_geometry_collection())
        }
        NativeType::Rect(_) => Err(DataFusionError::Execution(
            "Unsupported geometry type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::cast::AsArray;
    use datafusion::arrow::array::types::Int32Type;
    use datafusion::arrow::array::ArrayRef;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_expr::ScalarFunctionArgs;
    use geo_types::{line_string, point, polygon};
    use geoarrow::array::LineStringBuilder;
    use geoarrow::array::{CoordType, PointBuilder, PolygonBuilder};
    use geoarrow::datatypes::Dimension;
    use geoarrow::ArrayBase;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_srid() {
        let dim = Dimension::XY;
        let ct = CoordType::Separated;

        let args: [(ArrayRef, i32); 3] = [
            (
                {
                    let data = vec![
                        line_string![(x: 1., y: 2.), (x: 1., y: 0.), (x: 1., y: 1.), (x: 0., y: 1.), (x: 0., y: 0.)],
                        line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.), (x: 2., y: 3.), (x: 2., y: 2.)],
                    ];
                    let array =
                        LineStringBuilder::from_line_strings(&data, dim, ct, Arc::default())
                            .finish();
                    array.to_array_ref()
                },
                4326,
            ),
            (
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 1., y: 1.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                4326,
            ),
            (
                {
                    let data = vec![
                        polygon![(x: 3.4, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)],
                        polygon![(x: 3.3, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)],
                    ];
                    let array =
                        PolygonBuilder::from_polygons(&data, dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                4326,
            ),
        ];

        for (array, exp) in args {
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                number_rows: 2,
                return_type: &DataType::Null,
            };
            let srid_fn = Srid::new();
            let result = srid_fn.invoke_with_args(args).unwrap().to_array(2).unwrap();
            let result = result.as_primitive::<Int32Type>();
            assert_eq!(result.value(0), exp);
        }
    }
}
