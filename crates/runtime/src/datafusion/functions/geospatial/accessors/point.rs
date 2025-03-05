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

use crate::datafusion::functions::geospatial::data_types::{parse_to_native_array, POINT2D_TYPE};
use crate::datafusion::functions::geospatial::error as geo_error;
use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use arrow_schema::DataType::Float64;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use geo_traits::{CoordTrait, PointTrait};
use geoarrow::array::AsNativeArray;
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use snafu::ResultExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct PointX {
    signature: Signature,
}

impl PointX {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![POINT2D_TYPE.into()], Volatility::Immutable),
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointX {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_x"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        get_coord(args, 0)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the longitude (X coordinate) of a Point represented by geometry.",
                "ST_X(geom)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct PointY {
    signature: Signature,
}

impl PointY {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![POINT2D_TYPE.into()], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PointY {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "st_y"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        get_coord(args, 1)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the latitude (Y coordinate) of a Point represented by geometry.",
                "ST_Y(geom)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

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
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::point;
    use geoarrow::array::{CoordType, PointBuilder};
    use geoarrow::datatypes::Dimension;

    #[test]
    #[allow(clippy::unwrap_used, clippy::float_cmp)]
    fn test_x() {
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

        let args = vec![ColumnarValue::Array(pa)];
        let x = PointX::new();
        let result = x.invoke_batch(&args, 3).unwrap();
        let result = result.to_array(3).unwrap();

        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 4.0);
        assert_eq!(result.value(1), 1.0);
        assert_eq!(result.value(2), 2.0);
    }
    #[test]
    #[allow(clippy::unwrap_used, clippy::float_cmp)]
    fn test_y() {
        let pa = PointBuilder::from_points(
            [
                point! {x: 4., y: 0.},
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

        let args = vec![ColumnarValue::Array(pa)];
        let y = PointY::new();
        let result = y.invoke_batch(&args, 3).unwrap();
        let result = result.to_array(3).unwrap();

        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 0.0);
        assert_eq!(result.value(1), 2.0);
        assert_eq!(result.value(2), 3.0);
    }
}
