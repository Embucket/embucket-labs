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

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub nullable: bool,
    pub r#type: String,
    pub byte_length: Option<i32>,
    pub length: Option<i32>,
    pub scale: Option<i32>,
    pub precision: Option<i32>,
    pub collation: Option<String>,
}

impl ColumnInfo {
    #[must_use]
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("logicalType".to_string(), self.r#type.to_uppercase());
        metadata.insert(
            "precision".to_string(),
            self.precision.unwrap_or(38).to_string(),
        );
        metadata.insert("scale".to_string(), self.scale.unwrap_or(0).to_string());
        metadata.insert(
            "charLength".to_string(),
            self.length.unwrap_or(0).to_string(),
        );
        metadata
    }

    #[must_use]
    pub fn from_batch(records: &[RecordBatch]) -> Vec<Self> {
        let mut column_infos = Vec::new();

        if records.is_empty() {
            return column_infos;
        }
        for field in records[0].schema().fields() {
            column_infos.push(Self::from_field(field));
        }
        column_infos
    }

    #[must_use]
    pub fn from_field(field: &Field) -> Self {
        let mut column_info = Self {
            name: field.name().clone(),
            database: String::new(), // TODO
            schema: String::new(),   // TODO
            table: String::new(),    // TODO
            nullable: field.is_nullable(),
            r#type: field.data_type().to_string(),
            byte_length: None,
            length: None,
            scale: None,
            precision: None,
            collation: None,
        };

        match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                column_info.r#type = "fixed".to_string();
                column_info.precision = Some(38);
                column_info.scale = Some(0);
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                column_info.r#type = "real".to_string();
                column_info.precision = Some(38);
                column_info.scale = Some(16);
            }
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                column_info.r#type = "fixed".to_string();
                column_info.precision = Some(i32::from(*precision));
                column_info.scale = Some(i32::from(*scale));
            }
            DataType::Boolean => {
                column_info.r#type = "boolean".to_string();
            }
            // Varchar, Char, Utf8
            DataType::Utf8 => {
                column_info.r#type = "text".to_string();
                column_info.byte_length = Some(16_777_216);
                column_info.length = Some(16_777_216);
            }
            DataType::Time32(_) | DataType::Time64(_) => {
                column_info.r#type = "time".to_string();
                column_info.precision = Some(0);
                column_info.scale = Some(9);
            }
            DataType::Date32 | DataType::Date64 => {
                column_info.r#type = "date".to_string();
            }
            DataType::Timestamp(unit, _) => {
                column_info.r#type = "timestamp_ntz".to_string();
                column_info.precision = Some(0);
                let scale = match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                column_info.scale = Some(scale);
            }
            DataType::Binary | DataType::BinaryView => {
                column_info.r#type = "binary".to_string();
                column_info.byte_length = Some(8_388_608);
                column_info.length = Some(8_388_608);
            }
            _ => {
                column_info.r#type = "text".to_string();
            }
        }
        column_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_column_info_from_field() {
        let field = Field::new("test_field", DataType::Int8, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert!(!column_info.nullable);

        let field = Field::new("test_field", DataType::Decimal128(1, 2), true);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "fixed");
        assert_eq!(column_info.precision.unwrap(), 1);
        assert_eq!(column_info.scale.unwrap(), 2);
        assert!(column_info.nullable);

        let field = Field::new("test_field", DataType::Boolean, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "boolean");

        let field = Field::new("test_field", DataType::Time32(TimeUnit::Second), false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "time");
        assert_eq!(column_info.precision.unwrap(), 0);
        assert_eq!(column_info.scale.unwrap(), 9);

        let field = Field::new("test_field", DataType::Date32, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "date");

        let units = [
            (TimeUnit::Second, 0),
            (TimeUnit::Millisecond, 3),
            (TimeUnit::Microsecond, 6),
            (TimeUnit::Nanosecond, 9),
        ];
        for (unit, scale) in units {
            let field = Field::new("test_field", DataType::Timestamp(unit, None), false);
            let column_info = ColumnInfo::from_field(&field);
            assert_eq!(column_info.name, "test_field");
            assert_eq!(column_info.r#type, "timestamp_ntz");
            assert_eq!(column_info.precision.unwrap(), 0);
            assert_eq!(column_info.scale.unwrap(), scale);
        }

        let field = Field::new("test_field", DataType::Binary, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "binary");
        assert_eq!(column_info.byte_length.unwrap(), 8_388_608);
        assert_eq!(column_info.length.unwrap(), 8_388_608);

        // Any other type
        let field = Field::new("test_field", DataType::Utf8View, false);
        let column_info = ColumnInfo::from_field(&field);
        assert_eq!(column_info.name, "test_field");
        assert_eq!(column_info.r#type, "text");
        assert_eq!(column_info.byte_length, None);
        assert_eq!(column_info.length, None);

        let floats = [
            (DataType::Float16, 16, true),
            (DataType::Float32, 16, true),
            (DataType::Float64, 16, true),
            (DataType::Float64, 17, false),
        ];
        for (float_datatype, scale, outcome) in floats {
            let field = Field::new("test_field", float_datatype, false);
            let column_info = ColumnInfo::from_field(&field);
            assert_eq!(column_info.name, "test_field");
            assert_eq!(column_info.r#type, "real");
            assert_eq!(column_info.precision.unwrap(), 38);
            if outcome {
                assert_eq!(column_info.scale.unwrap(), scale);
            } else {
                assert_ne!(column_info.scale.unwrap(), scale);
            }
        }
    }

    #[tokio::test]
    async fn test_to_metadata() {
        let column_info = ColumnInfo {
            name: "test_field".to_string(),
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            table: "test_table".to_string(),
            nullable: false,
            r#type: "fixed".to_string(),
            byte_length: Some(8_388_608),
            length: Some(8_388_608),
            scale: Some(0),
            precision: Some(38),
            collation: None,
        };
        let metadata = column_info.to_metadata();
        assert_eq!(metadata.get("logicalType"), Some(&"FIXED".to_string()));
        assert_eq!(metadata.get("precision"), Some(&"38".to_string()));
        assert_eq!(metadata.get("scale"), Some(&"0".to_string()));
        assert_eq!(metadata.get("charLength"), Some(&"8388608".to_string()));
    }
}
