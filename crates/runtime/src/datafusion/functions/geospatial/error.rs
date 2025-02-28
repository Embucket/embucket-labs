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

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;
use geoarrow::error::GeoArrowError;
use geohash::GeohashError;
use snafu::Snafu;
use std::fmt::Debug;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum GeoDataFusionError {
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion { source: DataFusionError },

    #[snafu(display("GeoArrow error: {source}"))]
    GeoArrow { source: GeoArrowError },

    #[snafu(display("GeoHash error: {source}"))]
    GeoHash { source: GeohashError },
}

pub type GeoDataFusionResult<T> = Result<T, GeoDataFusionError>;

impl From<GeoDataFusionError> for DataFusionError {
    fn from(value: GeoDataFusionError) -> Self {
        match value {
            GeoDataFusionError::Arrow { source } => Self::ArrowError(source, None),
            GeoDataFusionError::DataFusion { source } => source,
            GeoDataFusionError::GeoArrow { source } => Self::External(Box::new(source)),
            GeoDataFusionError::GeoHash { source } => Self::External(Box::new(source)),
        }
    }
}
