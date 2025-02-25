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
use std::sync::Arc;

use crate::datafusion::functions::timestamp_from_parts::{
    make_date, take_function_args, to_primitive_array,
};
use arrow::array::builder::PrimitiveBuilder;
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::{DataType, Date32Type, Int32Type};
use arrow_schema::DataType::{Date32, Int32, Int64, UInt32, UInt64, Utf8, Utf8View};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Creates a date from individual numeric components that represent the year, month, and day of the month.",
    syntax_example = "date_from_parts(<year>, <month>, <day>)",
    sql_example = "```sql
            > select date_from_parts(2025, 2, 24);
            +------------------------------------------------+
            | date_from_parts(Int64(2025),Int64(2),Int64(24))|
            +------------------------------------------------+
            | 2025-02-24                                     |
            +------------------------------------------------+
            select date_from_parts(2025, 2, -1);
            +------------------------------------------------+
            | date_from_parts(Int64(2025),Int64(2),Int64(-1))|
            +------------------------------------------------+
            | 2025-01-30                                                                              |
            +------------------------------------------------+
```
DATE_FROM_PARTS is typically used to handle values in normal ranges 
(e.g. months 1-12, days 1-31), but it also handles values from outside these ranges.
This allows, for example, choosing the N-th day in a year, which can be used to simplify some computations.
Year, month, and day values can be negative (e.g. to calculate a date N months prior to a specific date).
Additional examples can be found [here](https://docs.snowflake.com/en/sql-reference/functions/date_from_parts)
",
    argument(
        name = "year",
        description = "An integer expression to use as a year for building a timestamp."
    ),
    argument(
        name = "month",
        description = "An integer expression to use as a month for building a timestamp, with January represented as 1, and December as 12."
    ),
    argument(
        name = "day",
        description = "An integer expression to use as a day for building a timestamp, usually in the 1-31 range."
    )
)]
#[derive(Debug)]
pub struct DateFromPartsFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for DateFromPartsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateFromPartsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                3,
                vec![Int32, Int64, UInt32, UInt64, Utf8, Utf8View],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datefromparts")],
        }
    }
}
impl ScalarUDFImpl for DateFromPartsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "date_from_parts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        let array_size = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let is_scalar = array_size == 1;

        let result = date_from_components(args, array_size)?;
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(result))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn date_from_components(
    args: &[ColumnarValue],
    array_size: usize,
) -> Result<PrimitiveArray<Date32Type>> {
    let [years, months, days] = take_function_args("date_from_parts", args)?;

    let years = to_primitive_array::<Int32Type>(&years.cast_to(&Int32, None)?)?;
    let months = to_primitive_array::<Int32Type>(&months.cast_to(&Int32, None)?)?;
    let days = to_primitive_array::<Int32Type>(&days.cast_to(&Int32, None)?)?;

    let mut builder: PrimitiveBuilder<Date32Type> = PrimitiveArray::builder(array_size);
    for i in 0..array_size {
        builder.append_value(make_date(years.value(i), months.value(i), days.value(i))?);
    }
    Ok(builder.finish())
}

super::macros::make_udf_function!(DateFromPartsFunc);

#[cfg(test)]
mod test {
    use crate::datafusion::functions::date_from_parts::DateFromPartsFunc;
    use crate::datafusion::functions::timestamp_from_parts::{
        to_primitive_array, UNIX_DAYS_FROM_CE,
    };
    use arrow::datatypes::Date32Type;
    use chrono::NaiveDate;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::ScalarValue;
    use datafusion_expr::ScalarUDFImpl;

    #[allow(clippy::unwrap_used)]
    fn columnar_value_fn<T>(is_scalar: bool, v: T) -> ColumnarValue
    where
        ScalarValue: From<T>,
        T: Clone,
    {
        if is_scalar {
            ColumnarValue::Scalar(ScalarValue::from(v))
        } else {
            ColumnarValue::Array(ScalarValue::from(v).to_array().unwrap())
        }
    }

    #[allow(clippy::type_complexity)]
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_time_from_parts() {
        let args: [(i64, i64, i64, String); 7] = [
            (2004, 2, 1, "2004-02-01".to_string()),
            (2004, 2, 0, "2004-01-31".to_string()),
            (2004, 2, -1, "2004-01-30".to_string()),
            (2004, -1, -1, "2003-10-30".to_string()),
            (2004, 0, 1, "2003-12-01".to_string()),
            (2004, -1, 1, "2003-11-01".to_string()),
            (2010, 1, 100, "2010-04-10".to_string()),
        ];

        let is_scalar_type = [true, false];

        for is_scalar in is_scalar_type {
            for (i, (y, m, d, exp)) in args.iter().enumerate() {
                let fn_args = vec![
                    columnar_value_fn(is_scalar, *y),
                    columnar_value_fn(is_scalar, *m),
                    columnar_value_fn(is_scalar, *d),
                ];
                let result = DateFromPartsFunc::new().invoke_batch(&fn_args, 1).unwrap();
                let result = to_primitive_array::<Date32Type>(&result).unwrap();
                let result =
                    NaiveDate::from_num_days_from_ce_opt(result.value(0) + UNIX_DAYS_FROM_CE)
                        .unwrap()
                        .format("%Y-%m-%d")
                        .to_string();
                assert_eq!(result, exp.as_str(), "failed at index {i}");
            }
        }
    }
}
