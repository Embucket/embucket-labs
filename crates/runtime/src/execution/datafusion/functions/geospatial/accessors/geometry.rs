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

use crate::execution::datafusion::functions::geospatial::data_types::{
    any_single_geometry_type_input, parse_to_native_array,
};
use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use arrow_schema::DataType::Float64;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{ColumnarValue, Documentation, ScalarUDFImpl, Signature};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ScalarFunctionArgs;
use geo_traits::CoordTrait;
use geo_traits::RectTrait;
use geoarrow::algorithm::geo::BoundingRect;
use geoarrow::trait_::ArrayAccessor;
use std::any::Any;
use std::sync::{Arc, OnceLock};

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

macro_rules! create_extremum_udf {
    ($name:ident, $func_name:expr, $index:expr, $is_max:expr, $doc:expr, $syntax:expr) => {
        #[derive(Debug)]
        pub struct $name {
            signature: Signature,
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    signature: any_single_geometry_type_input(),
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
                get_extremum(&args.args, $index, $is_max)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some(DOCUMENTATION.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $doc, $syntax)
                        .with_argument("g1", "geometry")
                        .with_related_udf("st_xmin")
                        .with_related_udf("st_ymin")
                        .with_related_udf("st_zmin")
                        .with_related_udf("st_xmax")
                        .build()
                }))
            }
        }
    };
}

create_extremum_udf!(
    MinX,
    "st_xmin",
    0,
    false,
    "Returns the minimum longitude (X coordinate) of all points contained in the specified geometry.",
    "ST_XMin(geom)"
);

create_extremum_udf!(
    MinY,
    "st_ymin",
    1,
    false,
    "Returns the minimum latitude (Y coordinate) of all points contained in the specified geometry.",
    "ST_YMin(geom)"
);

create_extremum_udf!(
    MaxX,
    "st_xmax",
    0,
    true,
    "Returns the maximum longitude (X coordinate) of all points contained in the specified geometry.",
    "ST_XMax(geom)"
);

create_extremum_udf!(
    MaxY,
    "st_ymax",
    1,
    true,
    "Returns the maximum latitude (Y coordinate) of all points contained in the specified geometry.",
    "ST_YMax(geom)"
);

fn get_extremum(args: &[ColumnarValue], index: i64, is_max: bool) -> Result<ColumnarValue> {
    let arg = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Execution("Expected only one argument".to_string()))?;

    let array = ColumnarValue::values_to_arrays(args)?
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Execution("Expected at least one argument".to_string()))?;

    let native_array = parse_to_native_array(&array)?;
    let native_array_ref = native_array
        .as_ref()
        .bounding_rect()
        .map_err(|e| DataFusionError::Execution(format!("Error getting bounding rect: {e}")))?;

    let mut output_array = Float64Builder::with_capacity(arg.len());
    for rect in native_array_ref.iter() {
        match (index, is_max) {
            (0, false) => output_array.append_option(rect.map(|r| r.min().x())),
            (1, false) => output_array.append_option(rect.map(|r| r.min().y())),
            (0, true) => output_array.append_option(rect.map(|r| r.max().x())),
            (1, true) => output_array.append_option(rect.map(|r| r.max().y())),
            _ => {
                return Err(DataFusionError::Execution(
                    "Index out of bounds".to_string(),
                ))
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output_array.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{MaxX, MaxY, MinX, MinY};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::ArrayRef;
    use datafusion::logical_expr::ColumnarValue;
    use geo_types::{line_string, point, polygon};
    use geoarrow::array::{CoordType, LineStringBuilder, PointBuilder, PolygonBuilder};
    use geoarrow::datatypes::Dimension;
    use geoarrow::ArrayBase;

    #[test]
    #[allow(clippy::unwrap_used, clippy::float_cmp)]
    fn test_extrema() {
        let dim = Dimension::XY;
        let ct = CoordType::Separated;

        let args: [(ArrayRef, [[f64; 2]; 4]); 3] = [
            (
                {
                    let data = vec![
                        line_string![(x: 0., y: 0.), (x: 1., y: 0.), (x: 1., y: 1.), (x: 0., y: 1.), (x: 0., y: 0.)],
                        line_string![(x: -60., y: -30.), (x: 60., y: -30.)],
                    ];
                    let array =
                        LineStringBuilder::from_line_strings(&data, dim, ct, Arc::default())
                            .finish();
                    array.to_array_ref()
                },
                [[0., -60.], [1., 60.], [0., -30.], [1., -30.]],
            ),
            (
                {
                    let data = [point! {x: 0., y: 0.}, point! {x: 1., y: 1.}];
                    let array =
                        PointBuilder::from_points(data.iter(), dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                [[0., 1.], [0., 1.], [0., 1.], [0., 1.]],
            ),
            (
                {
                    let data = vec![
                        polygon![(x: 3.3, y: 30.2), (x: 4.7, y: 24.6), (x: 13.4, y: 25.1), (x: 24.4, y: 30.0),(x:3.3,y:30.4)],
                        polygon![(x: 3.2, y: 11.1), (x: 4.7, y: 24.6), (x: 13.4, y: 25.1), (x: 19.4, y: 31.0),(x:3.3,y:36.4)],
                    ];
                    let array =
                        PolygonBuilder::from_polygons(&data, dim, ct, Arc::default()).finish();
                    array.to_array_ref()
                },
                [[3.3, 3.2], [24.4, 19.4], [24.6, 11.1], [30.4, 36.4]],
            ),
        ];

        let udfs: Vec<Box<dyn ScalarUDFImpl>> = vec![
            Box::new(MinX::new()),
            Box::new(MaxX::new()),
            Box::new(MinY::new()),
            Box::new(MaxY::new()),
        ];

        for (array, exp) in args {
            for (i, udf) in udfs.iter().enumerate() {
                let res = udf
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(array.clone())],
                        number_rows: 2,
                        return_type: &DataType::Null,
                    })
                    .unwrap()
                    .to_array(2)
                    .unwrap();
                let res = res.as_primitive::<Float64Type>();
                assert_eq!(res.value(0), exp[i][0]);
                assert_eq!(res.value(1), exp[i][1]);
            }
        }
    }
}
