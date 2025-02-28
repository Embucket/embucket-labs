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
    parse_to_native_array, LINE_STRING_TYPE,
};
use crate::datafusion::functions::geospatial::error as geo_error;
use crate::datafusion::functions::macros::make_udf_function;
use arrow_schema::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{DataFusionError, Result};
use geo_traits::{LineStringTrait, PointTrait};
use geoarrow::array::{
    AsNativeArray, CoordType, LineStringArray, LineStringBuilder, MultiPointArray, PointArray,
};
use geoarrow::datatypes::{Dimension, NativeType};
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::ArrayAccessor;
use geoarrow::ArrayBase;
use geozero::GeomProcessor;
use snafu::ResultExt;
use std::any::Any;
use std::sync::Arc;

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

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        unsafe { make_line(args) }
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
    let merged_point_array = point_arrays_to_linestring(&parsed_arrays, array_size)?;
    Ok(ColumnarValue::from(merged_point_array.to_array_ref()))
}

fn multi_points_to_line(multi_point_array: &MultiPointArray) -> Result<LineStringArray> {
    let mut builder =
        LineStringBuilder::new_with_options(Dimension::XY, CoordType::Separated, Arc::default());
    builder
        .linestring_begin(true, multi_point_array.len(), 0)
        .map_err(|e| DataFusionError::Execution(format!("Failed to start linestring: {e}")))?;
    for p in multi_point_array.iter_geo_values().flatten() {
        unsafe {
            if let Some(coord) = p.coord() {
                builder
                    .push_coord(coord)
                    .context(geo_error::GeoArrowSnafu)?;
            }
        }
    }
    builder
        .linestring_end(true, 0)
        .map_err(|e| DataFusionError::Execution(format!("Failed to end linestring: {e}")))?;
    Ok(builder.finish())
}

fn points_to_line(points: &PointArray) -> Result<LineStringArray> {
    let mut builder =
        LineStringBuilder::new_with_options(Dimension::XY, CoordType::Separated, Arc::default());
    builder
        .linestring_begin(true, points.len(), 0)
        .map_err(|e| DataFusionError::Execution(format!("Failed to start linestring: {e}")))?;

    for p in points.iter_geo_values() {
        unsafe {
            if let Some(coord) = p.coord() {
                builder
                    .push_coord(coord)
                    .context(geo_error::GeoArrowSnafu)?;
            }
        }
    }
    builder
        .linestring_end(true, 0)
        .map_err(|e| DataFusionError::Execution(format!("Failed to end linestring: {e}")))?;
    Ok(builder.finish())
}

unsafe fn point_arrays_to_linestring(
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
