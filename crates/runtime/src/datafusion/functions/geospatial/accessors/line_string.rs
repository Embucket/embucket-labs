// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::datafusion::functions::geospatial::data_types::{
    parse_to_native_array, BOX2D_TYPE, BOX3D_TYPE, GEOMETRY_TYPE, LINE_STRING_TYPE, POINT2D_TYPE,
    POINT3D_TYPE, POLYGON_2D_TYPE,
};
use crate::datafusion::functions::geospatial::error as geo_error;
use crate::datafusion::functions::timestamp_from_parts::to_primitive_array;
use arrow_array::types::Int64Type;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use geo_traits::LineStringTrait;
use geoarrow::array::{AsNativeArray, CoordType, PointBuilder};
use geoarrow::datatypes::Dimension;
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use snafu::ResultExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct EndPoint {
    signature: Signature,
}

impl EndPoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![LINE_STRING_TYPE.into()], Volatility::Immutable),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for EndPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_endpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(POINT2D_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        get_n_point(args, None)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the last point of a LINESTRING geometry as a POINT. Returns NULL if the input is not a LINESTRING", 
                "ST_EndPoint(line_string)")
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

#[derive(Debug)]
pub struct StartPoint {
    signature: Signature,
}

impl StartPoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![LINE_STRING_TYPE.into()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StartPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_startpoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(POINT2D_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        get_n_point(args, Some(1))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the first point of a LINESTRING geometry as a POINT.",
                "ST_StartPoint(line_string)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct PointN {
    signature: Signature,
}

impl PointN {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![POINT2D_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![POINT3D_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![BOX2D_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![BOX3D_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![LINE_STRING_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![POLYGON_2D_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![POINT2D_TYPE.into(), DataType::Int64]),
                    TypeSignature::Exact(vec![GEOMETRY_TYPE.into(), DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for PointN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_pointn"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(POINT2D_TYPE.into())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() < 2 {
            return Err(DataFusionError::Execution(
                "Expected two arguments in ST_PointN".to_string(),
            ));
        }
        let index = to_primitive_array::<Int64Type>(&args[1])?.value(0);
        get_n_point(args, Some(index))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point at a specified index in a LineString. Returns NULL if the input is not a LINESTRING",
                "ST_PointN(line_string)")
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

fn get_n_point(args: &[ColumnarValue], n: Option<i64>) -> Result<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Execution("Expected at least one argument".to_string()))?;

    let native_array = parse_to_native_array(&array)?;
    let native_array_ref = native_array.as_ref();
    let line_string_array = native_array_ref
        .as_line_string_opt()
        .ok_or(GeoArrowError::General(
            "Expected Geometry-typed array".to_string(),
        ))
        .context(geo_error::GeoArrowSnafu)?;

    let mut output_builder = PointBuilder::with_capacity_and_options(
        Dimension::XY,
        line_string_array.len(),
        CoordType::Separated,
        Arc::default(),
    );

    for line in line_string_array.iter() {
        if let Some(line_string) = line {
            let pos = if let Some(n) = n {
                let index = if n < 0 {
                    line_string.num_coords().try_into().unwrap_or(0) + n
                } else {
                    n - 1
                };
                index
                    .try_into()
                    .map_err(|_| DataFusionError::Execution("Index out of bounds".to_string()))?
            } else {
                line_string.num_coords() - 1
            };
            output_builder.push_coord(line_string.coord(pos).as_ref());
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish().into_array_ref().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::line_string;
    use geoarrow::array::{LineStringBuilder, PointArray};
    use geoarrow::datatypes::Dimension;
    use geoarrow::trait_::ArrayAccessor;
    use geozero::ToWkt;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_start_point() {
        let data = vec![
            line_string![(x: 1., y: 1.), (x: 1., y: 0.), (x: 1., y: 1.)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.)],
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
        let start_point = StartPoint::new();
        let result = start_point.invoke_batch(&args, 3).unwrap();
        let result = result.to_array(3).unwrap();
        assert_eq!(result.data_type(), &POINT2D_TYPE.into());
        let result = PointArray::try_from((result.as_ref(), Dimension::XY)).unwrap();
        assert_eq!(result.get(0).unwrap().to_wkt().unwrap(), "POINT(1 1)");
        assert_eq!(result.get(1).unwrap().to_wkt().unwrap(), "POINT(2 2)");
        assert_eq!(result.get(2).unwrap().to_wkt().unwrap(), "POINT(2 2)");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_end_point() {
        let data = vec![
            line_string![(x: 0., y: 0.), (x: 1., y: 0.), (x: 1., y: 1.)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.)],
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
        let end_point = EndPoint::new();
        let result = end_point.invoke_batch(&args, 3).unwrap();
        let result = result.to_array(3).unwrap();
        assert_eq!(result.data_type(), &POINT2D_TYPE.into());
        let result = PointArray::try_from((result.as_ref(), Dimension::XY)).unwrap();
        assert_eq!(result.get(0).unwrap().to_wkt().unwrap(), "POINT(1 1)");
        assert_eq!(result.get(1).unwrap().to_wkt().unwrap(), "POINT(3 3)");
        assert_eq!(result.get(2).unwrap().to_wkt().unwrap(), "POINT(3 2)");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_point_n() {
        let data = vec![
            line_string![(x: 0., y: 0.), (x: 1., y: 0.), (x: 1., y: 1.), (x: 4.1, y: 4.1)],
            line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.)],
            line_string![(x: 2., y: 2.), (x: 4., y: 2.)],
        ];
        let array = LineStringBuilder::from_line_strings(
            &data,
            Dimension::XY,
            CoordType::Separated,
            Arc::default(),
        )
        .finish();

        let cases: [(i64, bool, [&str; 3]); 5] = [
            (1, true, ["POINT(0 0)", "POINT(2 2)", "POINT(2 2)"]),
            (2, true, ["POINT(1 0)", "POINT(3 2)", "POINT(4 2)"]),
            (-1, true, ["POINT(4.1 4.1)", "POINT(3 3)", "POINT(4 2)"]),
            (-2, true, ["POINT(1 1)", "POINT(3 2)", "POINT(2 2)"]),
            (-10, false, ["", "", ""]),
        ];

        for (index, ok, exp) in cases {
            let data = array.to_array_ref();
            let args = vec![
                ColumnarValue::Array(data),
                ColumnarValue::Scalar(index.into()),
            ];
            let point_n = PointN::new();
            let result = point_n.invoke_batch(&args, 3);

            if ok {
                let result = result.unwrap().to_array(3).unwrap();
                assert_eq!(result.data_type(), &POINT2D_TYPE.into());
                let result = PointArray::try_from((result.as_ref(), Dimension::XY)).unwrap();
                assert_eq!(result.get(0).unwrap().to_wkt().unwrap(), exp[0]);
                assert_eq!(result.get(1).unwrap().to_wkt().unwrap(), exp[1]);
                assert_eq!(result.get(2).unwrap().to_wkt().unwrap(), exp[2]);
            } else {
                assert_eq!(
                    result.err().unwrap().to_string(),
                    "Execution error: Index out of bounds"
                );
            }
        }
    }
}
