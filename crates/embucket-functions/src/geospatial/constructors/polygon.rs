use crate::execution::datafusion::functions::geospatial::data_types::{
    parse_to_native_array, LINE_STRING_TYPE, POLYGON_2D_TYPE,
};
use crate::execution::datafusion::functions::geospatial::error as geo_error;
use crate::execution::datafusion::functions::macros::make_udf_function;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::{DataFusionError, Result};
use datafusion_doc::Documentation;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use geo_traits::LineStringTrait;
use geoarrow::array::{AsNativeArray, PolygonBuilder};
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use geoarrow_schema::{CoordType, Dimension};
use snafu::ResultExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct MakePolygon {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for MakePolygon {
    fn default() -> Self {
        Self::new()
    }
}

impl MakePolygon {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![LINE_STRING_TYPE.to_data_type()],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("st_polygon")],
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePolygon {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_makepolygon"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(POLYGON_2D_TYPE.into())
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        unsafe { make_polygon(args) }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry that represents a Polygon without holes.",
                "ST_MakePolygon(ST_POINT(-71.104, 42.315), ST_POINT(-71.103, 42.312))",
            )
            .with_related_udf("st_polygon")
            .build()
        }))
    }
}

unsafe fn make_polygon(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let parsed_arrays = ColumnarValue::values_to_arrays(args)?;

    if parsed_arrays.len() > 1 {
        return Err(DataFusionError::Execution(
            "Expected only one argument in ST_MakePolygon".to_string(),
        ));
    }

    let line_string_array = parse_to_native_array(&parsed_arrays[0])?;
    let line_string_array_ref = line_string_array.as_ref();
    let line_string_array = line_string_array_ref
        .as_line_string_opt()
        .ok_or(GeoArrowError::General(
            "Expected LineString-typed array in ST_MakePolygon".to_string(),
        ))
        .context(geo_error::GeoArrowSnafu)?;

    let mut builder =
        PolygonBuilder::new_with_options(Dimension::XY, CoordType::Separated, Arc::default());
    for i in 0..line_string_array.len() {
        if let Some(line) = line_string_array.get(i) {
            builder.try_push_geom_offset(1).map_err(|e| {
                DataFusionError::Execution(format!("failed to push geom offset: {e}"))
            })?;
            builder
                .try_push_ring_offset(line.num_coords())
                .map_err(|e| {
                    DataFusionError::Execution(format!("failed to push geom offset: {e}"))
                })?;
            for coord in line.coords() {
                builder
                    .push_coord(&coord)
                    .map_err(|e| DataFusionError::Execution(format!("failed to add coord: {e}")))?;
            }
        }
    }
    Ok(builder.finish().into_array_ref().into())
}

make_udf_function!(MakePolygon);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::line_string;
    use geoarrow::array::{LineStringBuilder, PolygonArray};
    use geoarrow::datatypes::Dimension;
    use geoarrow::trait_::ArrayAccessor;
    use geozero::ToWkt;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_make_polygon() {
        let data = vec![
            line_string![(x: 0., y: 0.), (x: 1., y: 0.), (x: 1., y: 1.), (x: 0., y: 1.), (x: 0., y: 0.)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.), (x: 2., y: 3.), (x: 2., y: 2.)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.), (x: 2., y: 3.)],
        ];
        let array = LineStringBuilder::from_line_strings(
            &data,
            Dimension::XY,
            CoordType::Separated,
            Arc::default(),
        )
        .finish();

        let data = array.to_array_ref();
        let args = vec![ColumnarValue::Array(data)];
        let make_polygon = MakePolygon::new();
        let result = make_polygon.invoke_batch(&args, 3).unwrap();
        let result = result.to_array(3).unwrap();

        assert_eq!(result.data_type(), &POLYGON_2D_TYPE.into());

        let result = PolygonArray::try_from((result.as_ref(), Dimension::XY)).unwrap();
        assert_eq!(
            result.get(0).unwrap().to_wkt().unwrap(),
            "POLYGON((0 0,1 0,1 1,0 1,0 0))"
        );
        assert_eq!(
            result.get(1).unwrap().to_wkt().unwrap(),
            "POLYGON((2 2,3 2,3 3,2 3,2 2))"
        );
        assert_eq!(
            result.get(2).unwrap().to_wkt().unwrap(),
            "POLYGON((2 2,3 2,3 3,2 3))"
        );
    }
}
