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

use std::any::Any;
use std::sync::{Arc, OnceLock};

use crate::datafusion::functions::geospatial::data_types::{
    any_single_geometry_type_input, parse_to_native_array,
};
use arrow::array::UInt8Builder;
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ScalarFunctionArgs;
use geoarrow::array::AsNativeArray;
use geoarrow::datatypes::NativeType;
use geoarrow::scalar::Geometry;
use geoarrow::trait_::ArrayAccessor;

#[derive(Debug)]
pub struct GeomDimension {
    signature: Signature,
}

impl GeomDimension {
    pub fn new() -> Self {
        Self {
            signature: any_single_geometry_type_input(),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomDimension {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_dimension"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        dim_impl(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the coordinate dimension of the geometry value.",
                "ST_Dimension(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

macro_rules! build_output_array {
    ($value:expr, $size:expr) => {{
        let mut output_array = UInt8Builder::with_capacity($size);
        for _ in 0..$size {
            output_array.append_value($value);
        }
        Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
    }};
}

fn dim_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            DataFusionError::Execution("Expected only one argument in ST_Dimension".to_string())
        })?;

    let native_array = parse_to_native_array(&array)?;
    let native_array_ref = native_array.as_ref();
    let array_size = native_array_ref.len();

    match native_array.data_type() {
        NativeType::Point(_, _) | NativeType::MultiPoint(_, _) => {
            build_output_array!(0, array_size)
        }
        NativeType::LineString(_, _) | NativeType::MultiLineString(_, _) => {
            build_output_array!(1, array_size)
        }
        NativeType::Polygon(_, _) | NativeType::MultiPolygon(_, _) | NativeType::Rect(_) => {
            build_output_array!(2, array_size)
        }
        NativeType::Geometry(_) | NativeType::GeometryCollection(_, _) => {
            let array_ref = native_array.as_ref();
            let arr = array_ref.as_geometry();
            let mut output_array = UInt8Builder::with_capacity(native_array.len());
            for geom in arr.iter() {
                let dim = match geom {
                    Some(g) => match g {
                        Geometry::Point(_) | Geometry::MultiPoint(_) => 0,
                        Geometry::LineString(_) | Geometry::MultiLineString(_) => 1,
                        Geometry::Polygon(_) | Geometry::MultiPolygon(_) | Geometry::Rect(_) => 2,
                        Geometry::GeometryCollection(_) => {
                            return Err(DataFusionError::Execution(
                                "Unsupported geometry type".to_string(),
                            ))
                        }
                    },
                    None => {
                        return Err(DataFusionError::Execution(
                            "Null geometry found".to_string(),
                        ))
                    }
                };
                output_array.append_value(dim);
            }
            Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt8Type;
    use arrow_array::ArrayRef;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::{line_string, point, polygon};
    use geoarrow::array::LineStringBuilder;
    use geoarrow::array::{CoordType, PointBuilder, PolygonBuilder};
    use geoarrow::datatypes::Dimension;
    use geoarrow::ArrayBase;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_dim() {
        let dim = Dimension::XY;
        let ct = CoordType::Separated;

        let args: [(ArrayRef, u8); 3] = [
            (
                {
                    let data = vec![
                        line_string![(x: 0., y: 0.), (x: 1., y: 0.), (x: 1., y: 1.), (x: 0., y: 1.), (x: 0., y: 0.)],
                        line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.), (x: 2., y: 3.), (x: 2., y: 2.)],
                    ];
                    let array =
                        LineStringBuilder::from_line_strings(&data, dim, ct, Arc::default())
                            .finish();
                    array.to_array_ref()
                },
                1,
            ),
            (
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 1., y: 1.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                0,
            ),
            (
                {
                    let data = vec![
                        polygon![(x: 3.3, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)],
                        polygon![(x: 3.3, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)],
                    ];
                    let array =
                        PolygonBuilder::from_polygons(&data, dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                2,
            ),
        ];

        for (array, exp) in args {
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                number_rows: 2,
                return_type: &DataType::Null,
            };
            let dim_fn = GeomDimension::new();
            let result = dim_fn.invoke_with_args(args).unwrap().to_array(2).unwrap();
            let result = result.as_primitive::<UInt8Type>();
            assert_eq!(result.value(0), exp);
        }
    }
}
