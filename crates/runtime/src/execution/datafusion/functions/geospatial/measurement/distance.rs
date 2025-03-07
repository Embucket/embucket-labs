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

use crate::execution::datafusion::functions::geospatial::data_types::parse_to_native_array;
use arrow_array::builder::Float64Builder;
use arrow_array::{Array, Float64Array};
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ScalarFunctionArgs;
use geoarrow::algorithm::geo::EuclideanDistance;
use geoarrow::array::AsNativeArray;
use geoarrow::datatypes::NativeType;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct Distance {
    signature: Signature,
}

impl Distance {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Distance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        distance(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the minimum great circle distance between two geometries or the minimum Euclidean distance between two geometries.",
                "ST_Distance(g1, g2)",
            )
            .with_argument("g1", "geometry")
            .with_argument("g2", "geometry")
            .build()
        }))
    }
}

macro_rules! match_point_distance_data_type {
    ($left:expr, $left_method:ident, $rhs:expr) => {
        match $rhs.data_type() {
            NativeType::Point(_, _) => {
                EuclideanDistance::euclidean_distance($left.$left_method(), $rhs.as_point())
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "ST_Distance does not support this right geometry type".to_string(),
                ))
            }
        }
    };
}

macro_rules! match_line_distance_data_type {
    ($left:expr, $left_method:ident, $rhs:expr) => {
        match $rhs.data_type() {
            NativeType::Point(_, _) => {
                EuclideanDistance::euclidean_distance($left.$left_method(), $rhs.as_point())
            }
            NativeType::LineString(_, _) => {
                EuclideanDistance::euclidean_distance($left.$left_method(), $rhs.as_line_string())
            }
            NativeType::Polygon(_, _) => {
                EuclideanDistance::euclidean_distance($left.$left_method(), $rhs.as_polygon())
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "ST_Distance does not support this rhs geometry type".to_string(),
                ))
            }
        }
    };
}
fn distance(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?;
    if array.len() > 2 {
        return Err(DataFusionError::Execution(
            "ST_Contains takes two arguments".to_string(),
        ));
    }

    let left = parse_to_native_array(&array[0])?;
    let left = left.as_ref();
    let right = parse_to_native_array(&array[1])?;
    let right = right.as_ref();

    let result = match left.data_type() {
        NativeType::Point(_, _) => {
            let point_array = left.as_point();
            match right.data_type() {
                NativeType::Point(_, _) => point_array.euclidean_distance(right.as_point()),
                NativeType::LineString(_, _) => {
                    point_array.euclidean_distance(right.as_line_string())
                }
                NativeType::Polygon(_, _) => point_array.euclidean_distance(right.as_polygon()),
                NativeType::MultiPoint(_, _) => {
                    point_array.euclidean_distance(right.as_multi_point())
                }
                NativeType::MultiLineString(_, _) => {
                    point_array.euclidean_distance(right.as_multi_line_string())
                }
                NativeType::MultiPolygon(_, _) => {
                    point_array.euclidean_distance(right.as_multi_polygon())
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "ST_Distance does not support this rhs geometry type".to_string(),
                    ))
                }
            }
        }
        NativeType::LineString(_, _) => match_line_distance_data_type!(left, as_line_string, right),
        NativeType::Polygon(_, _) => match_line_distance_data_type!(left, as_polygon, right),
        NativeType::MultiPoint(_, _) => {
            match_point_distance_data_type!(left, as_multi_point, right)
        }
        NativeType::MultiLineString(_, _) => {
            match_point_distance_data_type!(left, as_multi_line_string, right)
        }
        NativeType::MultiPolygon(_, _) => {
            match_point_distance_data_type!(left, as_multi_polygon, right)
        }
        _ => {
            return Err(DataFusionError::Execution(
                "ST_Distance does not support this left geometry type".to_string(),
            ))
        }
    };
    // Convert to meters
    Ok(ColumnarValue::Array(Arc::new(to_meters(&result))))
}

fn to_meters(array: &Float64Array) -> Float64Array {
    let mut builder = Float64Builder::with_capacity(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let meters = array.value(i).to_radians() * 6_371_000.0;
            builder.append_value(meters);
        }
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::ArrayRef;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::{line_string, point, polygon};
    use geoarrow::array::LineStringBuilder;
    use geoarrow::array::{CoordType, PointBuilder, PolygonBuilder};
    use geoarrow::datatypes::Dimension;
    use geoarrow::ArrayBase;

    #[test]
    #[allow(clippy::unwrap_used, clippy::float_cmp)]
    fn test_distance() {
        let dim = Dimension::XY;
        let ct = CoordType::Separated;
        let args: [(ArrayRef, ArrayRef, [f64; 2]); 3] = [
            (
                {
                    let data = vec![
                        line_string![(x: 1., y: 1.), (x: 1., y: 2.), (x: 1., y: 3.), (x: 0., y: 1.), (x: 0., y: 0.)],
                        line_string![(x: 2., y: 2.), (x: 3., y: 2.), (x: 3., y: 3.), (x: 2., y: 3.), (x: 2., y: 2.)],
                    ];
                    let array =
                        LineStringBuilder::from_line_strings(&data, dim, ct, Arc::default())
                            .finish();
                    array.to_array_ref()
                },
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 1., y: 1.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                [0., 157_253.],
            ),
            (
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 0., y: 0.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                {
                    let data = [point! {x: 2., y: 0.}, point! {x: 2., y: 0.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                [222_390., 222_390.],
            ),
            (
                {
                    let data = vec![
                        polygon![(x: 0., y: 0.), (x: 0., y: 1.0), (x: 1., y: 1.), (x: 1., y: 0.), (x:0., y:0.)],
                        polygon![(x: 3.3, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3, y:30.4)],
                    ];
                    let array =
                        PolygonBuilder::from_polygons(&data, dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                {
                    let data = [point! {x: 7.9, y: 28.4}, point! {x: 0., y: 0.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                [3_141_862., 2_741_919.],
            ),
        ];

        for (left, right, exp) in args {
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(left), ColumnarValue::Array(right)],
                number_rows: 2,
                return_type: &DataType::Null,
            };
            let distance_fn = Distance::new();
            let result = distance_fn
                .invoke_with_args(args)
                .unwrap()
                .to_array(2)
                .unwrap();
            let result = result.as_primitive::<Float64Type>();
            assert_eq!(result.value(0).round(), exp[0]);
            assert_eq!(result.value(1).round(), exp[1]);
        }
    }
}
