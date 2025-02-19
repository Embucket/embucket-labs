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

use arrow::array::Array;
use arrow::compute::{date_part, DatePart};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_common::{internal_err, types::logical_string};
use std::any::Any;
use std::sync::Arc;
use std::vec;

#[derive(Debug)]
pub struct DateDiffFunc {
    signature: Signature,
    #[allow(dead_code)]
    aliases: Vec<String>,
}

impl Default for DateDiffFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateDiffFunc {
    pub fn new() -> Self {
        Self {
            //TODO: Fix signature, can we diffretite between two differnt types? (ex.: date32 - timestamp)
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Timestamp,
                        TypeSignatureClass::Timestamp,
                    ]),
                    Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Time,
                        TypeSignatureClass::Time,
                    ]),
                    Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Date,
                        TypeSignatureClass::Date,
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("date_diff"),
                String::from("timediff"),
                String::from("time_diff"),
                String::from("timestampdiff"),
                String::from("timestamp_diff"),
            ],
        }
    }
    fn date_diff_func(
        date_or_time_expr1: &Arc<dyn Array>,
        date_or_time_expr2: &Arc<dyn Array>,
        unit_type: DatePart,
    ) -> Result<ColumnarValue> {
        let unit2 = date_part(date_or_time_expr2, unit_type)?;
        let unit1 = date_part(date_or_time_expr1, unit_type)?;
        Ok(ColumnarValue::Scalar(
            ScalarValue::try_from_array(&unit2, 0)?
                .sub(ScalarValue::try_from_array(&unit1, 0)?)?
                .cast_to(&Int64)?,
        ))
    }
}
//TODO: FIX docs
// date_diff SQL function
// Syntax: `DATEADD(<date_or_time_part>, <value>, <date_or_time_expr>)`
// - <date_or_time_part>: This indicates the units of time that you want to add.
// For example if you want to add two days, then specify day. This unit of measure must be one of the values listed in Supported date and time parts.
// - <value>: This is the number of units of time that you want to add.
// For example, if the units of time is day, and you want to add two days, specify 2. If you want to subtract two days, specify -2.
// - <date_or_time_expr>: Must evaluate to a date, time, or timestamp.
// This is the date, time, or timestamp to which you want to add.
// For example, if you want to add two days to August 1, 2024, then specify '2024-08-01'::DATE.
// If the data type is TIME, then the date_or_time_part must be in units of hours or smaller, not days or bigger.
// If the input data type is DATE, and the date_or_time_part is hours or smaller, the input value will not be rejected,
// but instead will be treated as a TIMESTAMP with hours, minutes, seconds, and fractions of a second all initially set to 0 (e.g. midnight on the specified date).
//
// Note: `dateadd` returns
// If date_or_time_expr is a time, then the return data type is a time.
// If date_or_time_expr is a timestamp, then the return data type is a timestamp.
// If date_or_time_expr is a date:
// - If date_or_time_part is day or larger (for example, month, year), the function returns a DATE value.
// - If date_or_time_part is smaller than a day (for example, hour, minute, second), the function returns a TIMESTAMP_NTZ value, with 00:00:00.000 as the starting time for the date.
// Usage notes:
// - When date_or_time_part is year, quarter, or month (or any of their variations),
// if the result month has fewer days than the original day of the month, the result day of the month might be different from the original day.
// Examples
// - dateadd(day, 30, CAST('2024-12-26' AS TIMESTAMP))
impl ScalarUDFImpl for DateDiffFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "datediff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return internal_err!("function requires three arguments");
        }
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 3 {
            return plan_err!("function requires three arguments");
        }
        let date_or_time_part = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
            _ => return plan_err!("Invalid unit type format"),
        };
        let date_or_time_expr1 = match &args[1] {
            ColumnarValue::Scalar(val) => val.to_array()?,
            ColumnarValue::Array(array) => array.clone(),
        };
        let date_or_time_expr2 = match &args[2] {
            ColumnarValue::Scalar(val) => val.to_array()?,
            ColumnarValue::Array(array) => array.clone(),
        };
        match date_or_time_part.as_str() {
            //should consider leap year (365-366 days)
            "year" | "y" | "yy" | "yyy" | "yyyy" | "yr" | "years" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Year)
            }
            //should consider months 28-31 days
            "month" | "mm" | "mon" | "mons" | "months" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Month)
            }
            "day" | "d" | "dd" | "days" | "dayofmonth" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Day)
            }
            "week" | "w" | "wk" | "weekofyear" | "woy" | "wy" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Week)
            }
            //should consider months 28-31 days
            "quarter" | "q" | "qtr" | "qtrs" | "quarters" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Quarter)
            }
            "hour" | "h" | "hh" | "hr" | "hours" | "hrs" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Hour)
            }
            "minute" | "m" | "mi" | "min" | "minutes" | "mins" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Minute)
            }
            "second" | "s" | "sec" | "seconds" | "secs" => {
                Self::date_diff_func(&date_or_time_expr1, &date_or_time_expr2, DatePart::Second)
            }
            "millisecond" | "ms" | "msec" | "milliseconds" => Self::date_diff_func(
                &date_or_time_expr1,
                &date_or_time_expr2,
                DatePart::Millisecond,
            ),
            "microsecond" | "us" | "usec" | "microseconds" => Self::date_diff_func(
                &date_or_time_expr1,
                &date_or_time_expr2,
                DatePart::Microsecond,
            ),
            "nanosecond" | "ns" | "nsec" | "nanosec" | "nsecond" | "nanoseconds" | "nanosecs" => {
                Self::date_diff_func(
                    &date_or_time_expr1,
                    &date_or_time_expr2,
                    DatePart::Nanosecond,
                )
            }
            _ => plan_err!("Invalid date_or_time_part type")?,
        }
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

super::macros::make_udf_function!(DateDiffFunc);
