use arrow::array::timezone::Tz;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{self, Microsecond, Millisecond, Nanosecond, Second};
use datafusion::common::{internal_err, plan_err, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};
use datafusion::scalar::ScalarValue;
use datafusion_expr::{ReturnInfo, ReturnTypeArgs};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ConvertTimezoneFunc {
    signature: Signature,
}

impl Default for ConvertTimezoneFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ConvertTimezoneFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![
                        Utf8,
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Utf8, Timestamp(Second, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Nanosecond, None)]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}
//TODO: FIX docs
// convert_timezone SQL function
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
impl ScalarUDFImpl for ConvertTimezoneFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "convert_timezone"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type_from_args should be called")
    }
    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        match args.arg_types.len() {
            2 => {
                let tz = match &args.scalar_arguments[0] {
                    Some(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return internal_err!("Invalid target_tz type"),
                };

                match &args.arg_types[1] {
                    Timestamp(tu, _) => Ok(ReturnInfo::new_non_nullable(Timestamp(
                        *tu,
                        Some(Arc::from(tz.into_boxed_str())),
                    ))),
                    _ => internal_err!("Invalid source_timestamp_tz type"),
                }
            }
            3 => match &args.arg_types[2] {
                Timestamp(tu, None) => Ok(ReturnInfo::new_non_nullable(Timestamp(*tu, None))),
                _ => internal_err!("Invalid source_timestamp_ntz type"),
            },
            other => {
                internal_err!(
                    "This function can only take two or three arguments, got {}",
                    other
                )
            }
        }
    }
    //TODO: select convert_timezone('UTC', v3) with v3 a timestamp with value = '2025-01-06 08:00:00',
    //should use local session time
    //TODO: select convert_timezone('America/New_York, 'UTC', v3) with v3 a timestamp with value = '2025-01-06 08:00:00 America/New_York',
    //should be parsed as the timezone None variant timestamp
    #[allow(clippy::too_many_lines)]
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args.len() {
            2 => {
                let target_tz = match &args[0] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return plan_err!("Invalid target_tz type format"),
                };
                let source_timestamp_tz = match &args[1] {
                    ColumnarValue::Scalar(val) => val.clone(),
                    ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
                };
                //TODO: is it compliant with clippy in main branch?
                if target_tz.parse::<Tz>().is_err() {
                    return plan_err!("No such target_tz timezone");
                }
                //TODO: always takes the None timezoned variant, if you remove the logic for None,
                //it will throw our generic error
                // if you remove the logic for None and the signuture for none,
                //it will take the Some variant but with "+00" str
                match &source_timestamp_tz {
                    ScalarValue::TimestampSecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampSecond(
                            Some(*ts),
                            Some(Arc::from(target_tz.clone().into_boxed_str())),
                        )
                        .cast_to(&Utf8)?
                        .cast_to(&Timestamp(
                            TimeUnit::Second,
                            Some(Arc::from(target_tz.into_boxed_str())),
                        ))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    ScalarValue::TimestampMillisecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampMillisecond(
                            Some(*ts),
                            Some(Arc::from(target_tz.clone().into_boxed_str())),
                        )
                        .cast_to(&Utf8)?
                        .cast_to(&Timestamp(
                            TimeUnit::Millisecond,
                            Some(Arc::from(target_tz.into_boxed_str())),
                        ))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    ScalarValue::TimestampMicrosecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampMicrosecond(
                            Some(*ts),
                            Some(Arc::from(target_tz.clone().into_boxed_str())),
                        )
                        .cast_to(&Utf8)?
                        .cast_to(&Timestamp(
                            TimeUnit::Microsecond,
                            Some(Arc::from(target_tz.into_boxed_str())),
                        ))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    ScalarValue::TimestampNanosecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampNanosecond(
                            Some(*ts),
                            Some(Arc::from(target_tz.clone().into_boxed_str())),
                        )
                        .cast_to(&Utf8)?
                        .cast_to(&Timestamp(
                            TimeUnit::Nanosecond,
                            Some(Arc::from(target_tz.into_boxed_str())),
                        ))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    _ => plan_err!("Invalid source_timestamp_tz type format"),
                }
            }
            3 => {
                let source_tz = match &args[0] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return plan_err!("Invalid source_tz type format"),
                };
                let target_tz = match &args[1] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return plan_err!("Invalid target_tz type format"),
                };
                let source_timestamp_ntz = match &args[2] {
                    ColumnarValue::Scalar(val) => val.clone(),
                    ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
                };
                //TODO: is it compliant with clippy in main branch?
                if source_tz.parse::<Tz>().is_err() {
                    return plan_err!("No such source_tz timezone");
                }
                //TODO: is it compliant with clippy in main branch?
                if target_tz.parse::<Tz>().is_err() {
                    return plan_err!("No such target_tz timezone");
                }
                //TODO: passes throught a timezoned timestamp as non timezoned with added time ton the i64
                match &source_timestamp_ntz {
                    ScalarValue::TimestampSecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampSecond(Some(*ts), None)
                            .cast_to(&Timestamp(
                                TimeUnit::Second,
                                Some(Arc::from(source_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(
                                TimeUnit::Second,
                                Some(Arc::from(target_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Second, None))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    ScalarValue::TimestampMillisecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampMillisecond(Some(*ts), None)
                            .cast_to(&Timestamp(
                                TimeUnit::Millisecond,
                                Some(Arc::from(source_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(
                                TimeUnit::Millisecond,
                                Some(Arc::from(target_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Millisecond, None))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    ScalarValue::TimestampMicrosecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampMicrosecond(Some(*ts), None)
                            .cast_to(&Timestamp(
                                TimeUnit::Microsecond,
                                Some(Arc::from(source_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(
                                TimeUnit::Microsecond,
                                Some(Arc::from(target_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Microsecond, None))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    ScalarValue::TimestampNanosecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampNanosecond(Some(*ts), None)
                            .cast_to(&Timestamp(
                                TimeUnit::Nanosecond,
                                Some(Arc::from(source_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(
                                TimeUnit::Nanosecond,
                                Some(Arc::from(target_tz.into_boxed_str())),
                            ))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Nanosecond, None))?;
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    }
                    _ => plan_err!("Invalid source_timestamp_tz type format"),
                }
            }
            _ => {
                plan_err!(
                    "This function can only take two or three arguments, got {}",
                    args.len()
                )
            }
        }
    }
}

super::macros::make_udf_function!(ConvertTimezoneFunc);
#[cfg(test)]
#[allow(clippy::unwrap_in_result)]
mod tests {
    use super::ConvertTimezoneFunc;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_convert_timezone_2_arg_correct() {
        let target_tz = String::from("UTC");
        let source_timestamp_tz_value = 1736168400000000i64;
        let source_timestamp_tz_timezone = String::from("+00");
        //2025-01-06 08:00:00 America/New_York, because it automaticly converts to 2025-01-06 13:00:00 in UTC
        let source_timestamp_tz = ScalarValue::TimestampMicrosecond(
            Some(source_timestamp_tz_value),
            Some(Arc::from(source_timestamp_tz_timezone.into_boxed_str())),
        );
        #[allow(deprecated)]
        let result_wrapped = ConvertTimezoneFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_tz.clone()))),
            ColumnarValue::Scalar(source_timestamp_tz.clone()),
        ]);
        match result_wrapped {
            Ok(ColumnarValue::Scalar(result)) => {
                let expected = ScalarValue::TimestampMicrosecond(
                    Some(1736168400000000i64),
                    Some(Arc::from(target_tz.into_boxed_str())),
                );
                assert_eq!(
                    result, expected,
                    "convert_timezone created wrong value for {}",
                    source_timestamp_tz_value
                )
            }
            _ => panic!("Conversion of {} failed", source_timestamp_tz),
        }
    }
    #[test]
    fn test_convert_timezone_3_arg_correct() {
        let source_tz = String::from("America/New_York");
        let target_tz = String::from("UTC");
        //2025-01-06 08:00:00 in UTC
        let source_timestamp_tz_value = 1736150400000000i64;
        #[allow(deprecated)]
        let result_wrapped = ConvertTimezoneFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(source_tz))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_tz.clone()))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(source_timestamp_tz_value),
                None,
            )),
        ]);
        match result_wrapped {
            Ok(ColumnarValue::Scalar(result)) => {
                let expected = ScalarValue::TimestampMicrosecond(
                    Some(1736168400000000i64),
                    Some(Arc::from(target_tz.into_boxed_str())),
                );
                assert_eq!(
                    result, expected,
                    "convert_timezone created wrong value for {}",
                    source_timestamp_tz_value
                )
            }
            _ => panic!("Conversion of {} failed", source_timestamp_tz_value),
        }
    }
    #[test]
    fn test_convert_timezone_3_arg_incorrect() {
        let source_tz = String::from("America/New_York");
        let target_tz = String::from("UTC");
        //2025-01-06 08:00:00 America/New_York, because it automaticly converts to 2025-01-06 13:00:00 in UTC
        //shouldn't even consider this value returns 2025-01-06 18:00:00 America/New_York, essentially,
        //beacuse of teh internal conversion it adds the America/New_York offset twice in this example
        let source_timestamp_tz_value = 1736168400000000i64;
        #[allow(deprecated)]
        let result_wrapped = ConvertTimezoneFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(source_tz))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_tz.clone()))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(source_timestamp_tz_value),
                None,
            )),
        ]);
        match result_wrapped {
            Ok(ColumnarValue::Scalar(result)) => {
                let expected = ScalarValue::TimestampMicrosecond(
                    Some(1736168400000000i64),
                    Some(Arc::from(target_tz.into_boxed_str())),
                );
                assert_ne!(
                    result, expected,
                    "convert_timezone created wrong value for {}",
                    source_timestamp_tz_value
                )
            }
            _ => panic!("Conversion of {} failed", source_timestamp_tz_value),
        }
    }
}
