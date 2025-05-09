use crate::execution::datafusion::functions::geospatial::data_types::{
    parse_to_native_array, POINT2D_TYPE,
};
use crate::execution::datafusion::functions::geospatial::error as geo_error;
use datafusion::arrow::array::builder::Float64Builder;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Float64;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ScalarFunctionArgs;
use geo_traits::{CoordTrait, PointTrait};
use geoarrow::array::AsNativeArray;
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use snafu::ResultExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
macro_rules! create_point_udf {
    ($name:ident, $func_name:expr, $index:expr, $doc:expr, $syntax:expr) => {
        #[derive(Debug)]
        pub struct $name {
            signature: Signature,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::exact(vec![POINT2D_TYPE.into()], Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &'static str {
                $func_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(Float64)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                get_coord(&args.args, $index)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some(DOCUMENTATION.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $doc, $syntax)
                        .with_argument("g1", "geometry")
                        .with_related_udf("st_x")
                        .with_related_udf("st_y")
                        .build()
                }))
            }
        }
    };
}

create_point_udf!(
    PointX,
    "st_x",
    0,
    "Returns the longitude (X coordinate) of a Point represented by geometry.",
    "ST_X(geom)"
);

create_point_udf!(
    PointY,
    "st_y",
    1,
    "Returns the latitude (Y coordinate) of a Point represented by geometry.",
    "ST_Y(geom)"
);

fn get_coord(args: &[ColumnarValue], n: i64) -> Result<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Execution("Expected at least one argument".to_string()))?;

    let native_array = parse_to_native_array(&array)?;
    let native_array_ref = native_array.as_ref();
    let points_array = native_array_ref
        .as_point_opt()
        .ok_or(GeoArrowError::General(
            "Expected Point-typed array".to_string(),
        ))
        .context(geo_error::GeoArrowSnafu)?;

    let mut output_builder = Float64Builder::with_capacity(points_array.len());

    for line in points_array.iter() {
        if let Some(point) = line {
            let coord = point
                .coord()
                .ok_or_else(|| DataFusionError::Execution("Coordinate is None".to_string()))?;
            let value = match n {
                0 => coord.x(),
                1 => coord.y(),
                _ => {
                    return Err(DataFusionError::Execution(
                        "Index out of bounds".to_string(),
                    ))
                }
            };
            output_builder.append_value(value);
        } else {
            output_builder.append_null();
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output_builder.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::cast::AsArray;
    use datafusion::arrow::array::types::Float64Type;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::point;
    use geoarrow::array::{CoordType, PointBuilder};
    use geoarrow::datatypes::Dimension;

    #[test]
    #[allow(clippy::unwrap_used, clippy::float_cmp)]
    fn test_points() {
        let pa = PointBuilder::from_points(
            [
                point! {x: 4., y: 2.},
                point! {x: 1., y: 2.},
                point! {x: 2., y: 3.},
            ]
            .iter(),
            Dimension::XY,
            CoordType::Separated,
            Arc::default(),
        )
        .finish()
        .to_array_ref();

        let results: [[f64; 3]; 2] = [[4., 1., 2.], [2., 2., 3.]];
        let udfs: Vec<Box<dyn ScalarUDFImpl>> =
            vec![Box::new(PointX::new()), Box::new(PointY::new())];

        for (idx, udf) in udfs.iter().enumerate() {
            let result = udf
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(pa.clone())],
                    number_rows: 3,
                    return_type: &DataType::Null,
                })
                .unwrap();
            let result = result.to_array(3).unwrap();
            let result = result.as_primitive::<Float64Type>();
            assert_eq!(result.value(0), results[idx][0]);
            assert_eq!(result.value(1), results[idx][1]);
            assert_eq!(result.value(2), results[idx][2]);
        }
    }
}
