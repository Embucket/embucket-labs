use crate::execution::datafusion::functions::geospatial::data_types::{
    parse_to_native_array, LINE_STRING_TYPE,
};
use crate::execution::datafusion::functions::geospatial::error as geo_error;
use crate::execution::datafusion::functions::macros::make_udf_function;
use arrow_schema::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_doc::Documentation;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use geo_traits::{LineStringTrait, MultiPointTrait, PointTrait};
use geoarrow::array::{
    AsNativeArray, LineStringArray, LineStringBuilder, MultiPointArray, PointArray,
};
use geoarrow_schema::{CoordType, Dimension};
use geoarrow::datatypes::{NativeType};
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use geozero::GeomProcessor;
use snafu::ResultExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct MakeLine {
    signature: Signature,
}

impl Default for MakeLine {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeLine {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Variadic(vec![
                    NativeType::Point(CoordType::Separated, Dimension::XY).to_data_type(),
                    NativeType::MultiPoint(CoordType::Separated, Dimension::XY).to_data_type(),
                    NativeType::LineString(CoordType::Separated, Dimension::XY).to_data_type(),
                    NativeType::Geometry(CoordType::Separated).to_data_type(),
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeLine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_makeline"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(LINE_STRING_TYPE.into())
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        unsafe { make_line(args) }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry that represents a line connecting the points in the input objects.",
                "ST_MakeLine(ST_POINT(-71.104, 42.315), ST_POINT(-71.103, 42.312))",
            )
                .with_related_udf("st_makeline")
                .build()
        }))
    }
}

unsafe fn make_line(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let parsed_arrays = ColumnarValue::values_to_arrays(args)?;
    let array_size = parsed_arrays[0].len();

    // Convert all input arrays to LineStringArray if possible
    let parsed_arrays = parsed_arrays
        .into_iter()
        .map(|arg| {
            let native_array = parse_to_native_array(&arg)?;
            let native_array_ref = native_array.as_ref();

            let point_array = match native_array.data_type() {
                NativeType::Point(_, _) => {
                    let points = native_array_ref
                        .as_point_opt()
                        .ok_or(GeoArrowError::General(
                            "Expected Point-typed array in ST_Makeline".to_string(),
                        ))
                        .context(geo_error::GeoArrowSnafu)?;
                    points_to_line(points)?
                }
                NativeType::MultiPoint(_, _) => {
                    let multi_points = native_array_ref
                        .as_multi_point_opt()
                        .ok_or(GeoArrowError::General(
                            "Expected MultiPoint-typed array in ST_Makeline".to_string(),
                        ))
                        .context(geo_error::GeoArrowSnafu)?;
                    multi_points_to_line(multi_points)?
                }
                NativeType::LineString(_, _) => native_array_ref
                    .as_line_string_opt()
                    .ok_or(GeoArrowError::General(
                        "Expected LineString-typed array in ST_Makeline".to_string(),
                    ))
                    .context(geo_error::GeoArrowSnafu)?
                    .clone(),
                _ => {
                    return Err(DataFusionError::Execution(
                        "Expected Point, LineString, or MultiPoint in ST_Makeline".to_string(),
                    ));
                }
            };
            Ok(point_array)
        })
        .collect::<Result<Vec<LineStringArray>>>()?;
    let merged_lines_array = merge_lines(&parsed_arrays, array_size)?;
    Ok(ColumnarValue::from(merged_lines_array.to_array_ref()))
}

fn multi_points_to_line(multi_point_array: &MultiPointArray) -> Result<LineStringArray> {
    let mut builder =
        LineStringBuilder::new_with_options(Dimension::XY, CoordType::Separated, Arc::default());

    for (idx, mp) in multi_point_array.iter_geo_values().enumerate() {
        builder
            .linestring_begin(true, mp.len(), idx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to start linestring: {e}")))?;
        for point in mp.points() {
            unsafe {
                if let Some(coord) = point.coord() {
                    builder
                        .push_coord(coord)
                        .context(geo_error::GeoArrowSnafu)?;
                }
            }
        }
        builder
            .linestring_end(true, idx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to end linestring: {e}")))?;
    }

    Ok(builder.finish())
}

fn points_to_line(points: &PointArray) -> Result<LineStringArray> {
    let mut builder =
        LineStringBuilder::new_with_options(Dimension::XY, CoordType::Separated, Arc::default());

    for (idx, point) in points.iter_geo_values().enumerate() {
        builder
            .linestring_begin(true, 1, idx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to start linestring: {e}")))?;
        unsafe {
            if let Some(coord) = point.coord() {
                builder
                    .push_coord(coord)
                    .context(geo_error::GeoArrowSnafu)?;
            }
        }
        builder
            .linestring_end(true, idx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to end linestring: {e}")))?;
    }
    Ok(builder.finish())
}

unsafe fn merge_lines(
    line_arrays: &[LineStringArray],
    array_size: usize,
) -> Result<LineStringArray> {
    let mut builder =
        LineStringBuilder::new_with_options(Dimension::XY, CoordType::Separated, Arc::default());

    for idx in 0..array_size {
        let lines = line_arrays
            .iter()
            .map(|line_array| line_array.get(idx))
            .collect::<Vec<_>>();

        let mut coords = Vec::new();
        for line in lines.into_iter().flatten() {
            line.coords().for_each(|coord| {
                coords.push(coord);
            });
        }
        builder
            .linestring_begin(true, coords.len(), idx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to start linestring: {e}")))?;
        for coord in coords {
            builder
                .push_coord(&coord)
                .context(geo_error::GeoArrowSnafu)?;
        }
        builder
            .linestring_end(true, idx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to end linestring: {e}")))?;
    }
    Ok(builder.finish())
}

make_udf_function!(MakeLine);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datafusion::functions::geospatial::data_types::LINE_STRING_TYPE;
    use arrow_array::Array;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::{line_string, point};
    use geoarrow::array::{LineStringArray, LineStringBuilder, PointBuilder};
    use geoarrow::datatypes::Dimension;
    use geoarrow::trait_::ArrayAccessor;
    use geozero::ToWkt;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_make_line() {
        let pa = PointBuilder::from_points(
            [
                point! {x: 0., y: 0.},
                point! {x: 1., y: 1.},
                point! {x: 2., y: 2.},
            ]
            .iter(),
            Dimension::XY,
            CoordType::Separated,
            Arc::default(),
        )
        .finish()
        .to_array_ref();

        let pa2 = PointBuilder::from_points(
            [
                point! {x: 3., y: 3.},
                point! {x: 4., y: 4.},
                point! {x: 5., y: 5.},
            ]
            .iter(),
            Dimension::XY,
            CoordType::Separated,
            Arc::default(),
        )
        .finish()
        .to_array_ref();

        let lines_array = LineStringBuilder::from_line_strings(
            &[
                line_string![(x: 0., y: 1.), (x: 2., y: 1.)],
                line_string![],
                line_string![(x: 2., y: 3.), (x: 3., y: 4.)],
            ],
            Dimension::XY,
            CoordType::Separated,
            Arc::default(),
        )
        .finish()
        .to_array_ref();

        let args = vec![
            ColumnarValue::Array(pa),
            ColumnarValue::Array(pa2),
            ColumnarValue::Array(lines_array),
        ];
        let make_line = MakeLine::new();
        let result = make_line.invoke_batch(&args, 4).unwrap();
        let result = result.to_array(3).unwrap();

        assert_eq!(result.data_type(), &LINE_STRING_TYPE.into());
        let result = LineStringArray::try_from((result.as_ref(), Dimension::XY)).unwrap();
        assert_eq!(
            result.get(0).unwrap().to_wkt().unwrap(),
            "LINESTRING(0 0,3 3,0 1,2 1)"
        );
        assert_eq!(
            result.get(1).unwrap().to_wkt().unwrap(),
            "LINESTRING(1 1,4 4)"
        );
        assert_eq!(
            result.get(2).unwrap().to_wkt().unwrap(),
            "LINESTRING(2 2,5 5,2 3,3 4)"
        );
    }
}
