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
use arrow_schema::DataType;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ScalarFunctionArgs;
use geoarrow::algorithm::geo::Contains as ContainsTrait;
use geoarrow::array::AsNativeArray;
use geoarrow::datatypes::NativeType;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct Contains {
    signature: Signature,
}

impl Contains {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Contains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        contains(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns TRUE the geometry object is completely inside another object of the same type.",
                "ST_Contains(g1, g2)",
            )
                .with_argument("g1", "geometry")
                .with_argument("g2", "geometry")
                .with_related_udf("st_within")
                .with_related_udf("st_covers")
                .build()
        }))
    }
}

macro_rules! match_rhs_data_type {
    ($left:expr, $left_method:ident, $rhs:expr) => {
        match $rhs.data_type() {
            NativeType::Point(_, _) => {
                ContainsTrait::contains($left.$left_method(), $rhs.as_point())
            }
            NativeType::LineString(_, _) => {
                ContainsTrait::contains($left.$left_method(), $rhs.as_line_string())
            }
            NativeType::Polygon(_, _) => {
                ContainsTrait::contains($left.$left_method(), $rhs.as_polygon())
            }
            NativeType::MultiPoint(_, _) => {
                ContainsTrait::contains($left.$left_method(), $rhs.as_multi_point())
            }
            NativeType::MultiLineString(_, _) => {
                ContainsTrait::contains($left.$left_method(), $rhs.as_multi_line_string())
            }
            NativeType::MultiPolygon(_, _) => {
                ContainsTrait::contains($left.$left_method(), $rhs.as_multi_polygon())
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "ST_Contains does not support this rhs geometry type".to_string(),
                ))
            }
        }
    };
}

fn contains(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(args)?;
    if array.len() > 2 {
        return Err(DataFusionError::Execution(
            "ST_Contains takes two arguments".to_string(),
        ));
    }

    let left = parse_to_native_array(&array[0])?;
    let left = left.as_ref();
    let rhs = parse_to_native_array(&array[1])?;
    let rhs = rhs.as_ref();

    let result = match left.data_type() {
        NativeType::Point(_, _) => match_rhs_data_type!(left, as_point, rhs),
        NativeType::LineString(_, _) => match_rhs_data_type!(left, as_line_string, rhs),
        NativeType::Polygon(_, _) => match_rhs_data_type!(left, as_polygon, rhs),
        NativeType::MultiPoint(_, _) => match_rhs_data_type!(left, as_multi_point, rhs),
        NativeType::MultiLineString(_, _) => match_rhs_data_type!(left, as_multi_line_string, rhs),
        NativeType::MultiPolygon(_, _) => match_rhs_data_type!(left, as_multi_polygon, rhs),
        _ => {
            return Err(DataFusionError::Execution(
                "ST_Contains does not support this left geometry type".to_string(),
            ))
        }
    };
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::ArrayRef;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::{line_string, point, polygon};
    use geoarrow::array::LineStringBuilder;
    use geoarrow::array::{CoordType, PointBuilder, PolygonBuilder};
    use geoarrow::datatypes::Dimension;
    use geoarrow::ArrayBase;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_contains() {
        let dim = Dimension::XY;
        let ct = CoordType::Separated;

        let args: [(ArrayRef, ArrayRef, [bool; 2]); 3] = [
            (
                {
                    let data = vec![
                        line_string![(x: 0., y: 0.), (x: 1., y: 2.), (x: 1., y: 1.), (x: 0., y: 1.), (x: 0., y: 0.)],
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
                [true, false],
            ),
            (
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 1., y: 1.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 0., y: 0.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                [true, false],
            ),
            (
                {
                    let data = vec![
                        polygon![(x: 3.3, y: 30.5), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)],
                        polygon![(x: 3.3, y: 30.4), (x: 1.7, y: 24.6), (x: 13.4, y: 25.1), (x: 14.4, y: 31.0),(x:3.3,y:30.4)],
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
                [true, false],
            ),
        ];

        for (left, rhs, exp) in args {
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(left), ColumnarValue::Array(rhs)],
                number_rows: 2,
                return_type: &DataType::Null,
            };
            let contains = Contains::new();
            let result = contains
                .invoke_with_args(args)
                .unwrap()
                .to_array(2)
                .unwrap();
            let result = result.as_boolean();
            assert_eq!(result.value(0), exp[0]);
            assert_eq!(result.value(1), exp[1]);
        }
    }
}
