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

use crate::datafusion::functions::timestamp_from_parts::{take_function_args, to_primitive_array};
use arrow::array::builder::PrimitiveBuilder;
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::{DataType, Int32Type, Int64Type, Time64NanosecondType};
use arrow_schema::DataType::{Int32, Int64, Time64};
use arrow_schema::TimeUnit;
use chrono::prelude::*;
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion_common::types::logical_int64;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue, _exec_datafusion_err};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Creates a time from individual numeric components.",
    syntax_example = "time_from_parts(<hour>, <minute>, <second> [, <nanosecond> ])",
    sql_example = "```sql
            > select time_from_parts(12, 34, 56, 987654321);
            +-----------------------------------------------------------------+
            | time_from_parts(Int64(12),Int64(34),Int64(56),Int64(987654321)) |
            +-----------------------------------------------------------------+
            | 1740398450.0                                                    |
            +-----------------------------------------------------------------+
```",
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "hour",
        description = "An integer expression to use as an hour for building a timestamp, usually in the 0-23 range."
    ),
    argument(
        name = "minute",
        description = "An integer expression to use as a minute for building a timestamp, usually in the 0-59 range."
    ),
    argument(
        name = "second",
        description = "An integer expression to use as a second for building a timestamp, usually in the 0-59 range."
    ),
    argument(
        name = "nanoseconds",
        description = "Optional integer expression to use as a nanosecond for building a timestamp,
         usually in the 0-999999999 range."
    )
)]
#[derive(Debug)]
pub struct TimeFromPartsFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for TimeFromPartsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeFromPartsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![TypeSignatureClass::Native(logical_int64()); 4]),
                    Coercible(vec![TypeSignatureClass::Native(logical_int64()); 3]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("timefromparts")],
        }
    }
}
impl ScalarUDFImpl for TimeFromPartsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "time_from_parts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(TimeUnit::Nanosecond))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let array_size = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        let is_scalar = array_size == 1;

        let result = time_from_components(args, array_size)?;
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

fn time_from_components(
    args: &[ColumnarValue],
    array_size: usize,
) -> Result<PrimitiveArray<Time64NanosecondType>> {
    let (hours, minutes, seconds, nanos) = match args.len() {
        4 => {
            let [hours, minutes, seconds, nanos] = take_function_args("time_from_parts", args)?;
            (hours, minutes, seconds, Some(nanos))
        }
        3 => {
            let [hours, minutes, seconds] = take_function_args("time_from_parts", args)?;
            (hours, minutes, seconds, None)
        }
        _ => return internal_err!("Unsupported number of arguments"),
    };

    let hours = to_primitive_array::<Int32Type>(&hours.cast_to(&Int32, None)?)?;
    let minutes = to_primitive_array::<Int32Type>(&minutes.cast_to(&Int32, None)?)?;
    let seconds = to_primitive_array::<Int32Type>(&seconds.cast_to(&Int32, None)?)?;
    let nanoseconds = nanos
        .map(|nanoseconds| to_primitive_array::<Int64Type>(&nanoseconds.cast_to(&Int64, None)?))
        .transpose()?;
    let mut builder: PrimitiveBuilder<Time64NanosecondType> = PrimitiveArray::builder(array_size);
    for i in 0..array_size {
        let nanoseconds = nanoseconds.as_ref().map(|ns| ns.value(i));
        make_time(
            hours.value(i),
            minutes.value(i),
            seconds.value(i),
            nanoseconds,
            &mut builder,
        )?;
    }
    Ok(builder.finish())
}

fn make_time(
    hour: i32,
    minute: i32,
    seconds: i32,
    nanosecond: Option<i64>,
    builder: &mut PrimitiveBuilder<Time64NanosecondType>,
) -> Result<()> {
    let hour = u32::try_from(hour)
        .map_err(|_| _exec_datafusion_err!("hour value '{hour:?}' is out of range"))?;
    let minute = u32::try_from(minute)
        .map_err(|_| _exec_datafusion_err!("minute value '{minute:?}' is out of range"))?;
    let seconds = u32::try_from(seconds)
        .map_err(|_| _exec_datafusion_err!("seconds value '{seconds:?}' is out of range"))?;
    let nano = u32::try_from(nanosecond.unwrap_or(0))
        .map_err(|_| _exec_datafusion_err!("nanosecond value '{nanosecond:?}' is out of range"))?;

    if let Some(time) = NaiveTime::from_hms_nano_opt(hour, minute, seconds, nano) {
        let time_nanos = i64::from(time.hour()) * 3_600_000_000_000
            + i64::from(time.minute()) * 60_000_000_000
            + i64::from(time.second()) * 1_000_000_000
            + i64::from(time.nanosecond());
        builder.append_value(time_nanos);
    } else {
        return exec_err!("Invalid time part '{hour:?}':'{minute:?}':'{seconds:?}'");
    }
    Ok(())
}

super::macros::make_udf_function!(TimeFromPartsFunc);

#[cfg(test)]
mod test {
    use crate::datafusion::functions::time_from_parts::TimeFromPartsFunc;
    use crate::datafusion::functions::timestamp_from_parts::to_primitive_array;
    use arrow::datatypes::Time64NanosecondType;
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
        let args: [(i64, i64, i64, Option<i64>, i64); 4] = [
            (12, 0, 0, None, 43_200_000_000_000),
            (12, 10, 0, None, 43_800_000_000_000),
            (12, 10, 12, None, 43_812_000_000_000),
            (12, 10, 12, Some(255), 43_812_000_000_255),
        ];

        let is_scalar_type = [true, false];

        for is_scalar in is_scalar_type {
            for (i, (h, mi, s, n, exp)) in args.iter().enumerate() {
                let mut fn_args = vec![
                    columnar_value_fn(is_scalar, *h),
                    columnar_value_fn(is_scalar, *mi),
                    columnar_value_fn(is_scalar, *s),
                ];
                if let Some(nano) = n {
                    fn_args.push(columnar_value_fn(is_scalar, *nano));
                };
                let result = TimeFromPartsFunc::new().invoke_batch(&fn_args, 1).unwrap();
                let result = to_primitive_array::<Time64NanosecondType>(&result).unwrap();
                assert_eq!(result.value(0), *exp, "failed at index {i}");
            }
        }
    }
}
