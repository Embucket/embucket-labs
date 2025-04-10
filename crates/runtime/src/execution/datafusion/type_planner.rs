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

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::sql::sqlparser::ast::DataType as SQLDataType;
use datafusion::sql::utils::make_decimal_type;
use datafusion_common::{not_impl_err, DataFusionError};

#[derive(Debug)]
pub struct IceBucketTypePlanner {}

impl TypePlanner for IceBucketTypePlanner {
    fn plan_type(&self, sql_type: &ast::DataType) -> Result<Option<DataType>> {
        match sql_type {
            SQLDataType::Int32 => Ok(Some(DataType::Int32)),
            SQLDataType::Int64 => Ok(Some(DataType::Int64)),
            SQLDataType::UInt32 => Ok(Some(DataType::UInt32)),
            SQLDataType::Blob(_) => Ok(Some(DataType::Binary)),
            SQLDataType::Float(_) | SQLDataType::Float32 => Ok(Some(DataType::Float32)),
            SQLDataType::Float64 => Ok(Some(DataType::Float64)),

            // https://github.com/apache/datafusion/issues/12644
            SQLDataType::JSON => Ok(Some(DataType::Utf8)),
            SQLDataType::Custom(a, b) => match a.to_string().to_uppercase().as_str() {
                "VARIANT" => Ok(Some(DataType::Utf8)),
                "TIMESTAMP_NTZ" => {
                    let parsed_b: Option<u64> = b.iter().next().and_then(|s| s.parse().ok());
                    match parsed_b {
                        Some(0) => Ok(Some(DataType::Timestamp(TimeUnit::Second, None))),
                        Some(3) => Ok(Some(DataType::Timestamp(TimeUnit::Millisecond, None))),
                        // We coerce nanoseconds to microseconds as Apache Iceberg v2 doesn't support nanosecond precision
                        Some(6 | 9) => Ok(Some(DataType::Timestamp(TimeUnit::Microsecond, None))),
                        _ => not_impl_err!("Unsupported SQL TIMESTAMP_NTZ precision {parsed_b:?}"),
                    }
                }
                "NUMBER" => {
                    let (precision, scale) = match b.len() {
                        0 => (None, None),
                        1 => {
                            let precision = b[0].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid precision: {}", b[0]))
                            })?;
                            (Some(precision), None)
                        }
                        2 => {
                            let precision = b[0].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid precision: {}", b[0]))
                            })?;
                            let scale = b[1].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid scale: {}", b[1]))
                            })?;
                            (Some(precision), Some(scale))
                        }
                        _ => {
                            return Err(DataFusionError::Plan(format!(
                                "Invalid NUMBER type format: {b:?}"
                            )));
                        }
                    };
                    make_decimal_type(precision, scale).map(Some)
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }
}
