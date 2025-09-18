use std::any::Any;
use std::sync::Arc;

use crate::datetime::timestamp_from_parts::{make_time, take_function_args, to_primitive_array};
use datafusion::arrow::array::builder::PrimitiveBuilder;
use datafusion::arrow::array::{Array, PrimitiveArray};
use datafusion::arrow::datatypes::DataType::{Int64, Time64};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::arrow::datatypes::{DataType, Int64Type, Time64NanosecondType};
use datafusion::logical_expr::TypeSignature::Coercible;
use datafusion::logical_expr::TypeSignatureClass;
use datafusion_common::types::logical_int64;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{Coercion, ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Creates a timestamp from individual numeric components.",
    syntax_example = "time_from_parts(<hour>, <minute>, <second> [, <nanosecond> ])",
    sql_example = "```sql
            > select time_from_parts(12, 34, 56, 987654321);
            +-----------------------------------------------------------------+
            | time_from_parts(Int64(12),Int64(34),Int64(56),Int64(987654321)) |
            +-----------------------------------------------------------------+
            | 1740398450.0                                                    |
            +-----------------------------------------------------------------+
```",
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(
                            logical_int64()
                        ));
                        4
                    ]),
                    Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(
                            logical_int64()
                        ));
                        3
                    ]),
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

    fn name(&self) -> &'static str {
        "time_from_parts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(TimeUnit::Nanosecond))
    }

    fn invoke_with_args(&self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
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

    let hours = to_primitive_array::<Int64Type>(&hours.cast_to(&Int64, None)?)?;
    let minutes = to_primitive_array::<Int64Type>(&minutes.cast_to(&Int64, None)?)?;
    let seconds = to_primitive_array::<Int64Type>(&seconds.cast_to(&Int64, None)?)?;
    let nanoseconds = nanos
        .map(|nanoseconds| to_primitive_array::<Int64Type>(&nanoseconds.cast_to(&Int64, None)?))
        .transpose()?;
    let mut builder: PrimitiveBuilder<Time64NanosecondType> = PrimitiveArray::builder(array_size);
    for i in 0..array_size {
        builder.append_value(make_time(
            hours.value(i),
            minutes.value(i),
            seconds.value(i),
            nanoseconds.as_ref().map(|ns| ns.value(i)),
        ));
    }
    Ok(builder.finish())
}

crate::macros::make_udf_function!(TimeFromPartsFunc);

#[cfg(test)]
mod test {
    use super::TimeFromPartsFunc;
    use crate::datetime::timestamp_from_parts::to_primitive_array;
    use chrono::NaiveTime;
    use datafusion::arrow::datatypes::Time64NanosecondType;
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
        let args: [(i64, i64, i64, Option<i64>, String); 6] = [
            (12, 0, 0, None, "12:00:00.000000000".to_string()),
            (12, 10, 0, None, "12:10:00.000000000".to_string()),
            (12, 10, 12, None, "12:10:12.000000000".to_string()),
            (12, 10, 12, Some(255), "12:10:12.000000255".to_string()),
            (12, 10, -12, Some(255), "12:09:48.000000255".to_string()),
            (12, -10, -12, Some(255), "11:49:48.000000255".to_string()),
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
                }
                let result = TimeFromPartsFunc::new()
                    .invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                        args: fn_args,
                        number_rows: 1,
                        return_type: &datafusion::arrow::datatypes::DataType::Time64(
                            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                        ),
                    })
                    .unwrap();
                let result = to_primitive_array::<Time64NanosecondType>(&result).unwrap();
                let seconds = result.value(0) / 1_000_000_000;
                let nanoseconds = result.value(0) % 1_000_000_000;

                let time = NaiveTime::from_num_seconds_from_midnight_opt(
                    u32::try_from(seconds).unwrap(),
                    u32::try_from(nanoseconds).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    time.format("%H:%M:%S.%9f").to_string(),
                    *exp,
                    "failed at index {i}"
                );
            }
        }
    }
}
