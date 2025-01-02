use arrow::array::Array;
use arrow::datatypes::DataType::{Date32, Date64, Int64, Time32, Time64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, Fields};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};
use datafusion::scalar::ScalarValue;
use std::any::Any;

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
                    Exact(vec![Utf8, Utf8, Timestamp(Second, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Nanosecond, None)]),
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
                ],
                Volatility::Immutable,
            ),
        }
    }
    //TODO: add all timezones
    fn timezone_to_hours_in_ns(tz: &str) -> Result<i64> {
        match tz {
            "UTC" => {
                Ok(0)
            }
            _ => {
                plan_err!("")
            }
        }
    }
}
//TODO: FIX docs
/// ConvertTimezone SQL function
/// Syntax: `DATEADD(<date_or_time_part>, <value>, <date_or_time_expr>)`
/// - <date_or_time_part>: This indicates the units of time that you want to add.
/// For example if you want to add two days, then specify day. This unit of measure must be one of the values listed in Supported date and time parts.
/// - <value>: This is the number of units of time that you want to add.
/// For example, if the units of time is day, and you want to add two days, specify 2. If you want to subtract two days, specify -2.
/// - <date_or_time_expr>: Must evaluate to a date, time, or timestamp.
/// This is the date, time, or timestamp to which you want to add.
/// For example, if you want to add two days to August 1, 2024, then specify '2024-08-01'::DATE.
/// If the data type is TIME, then the date_or_time_part must be in units of hours or smaller, not days or bigger.
/// If the input data type is DATE, and the date_or_time_part is hours or smaller, the input value will not be rejected,
/// but instead will be treated as a TIMESTAMP with hours, minutes, seconds, and fractions of a second all initially set to 0 (e.g. midnight on the specified date).
///
/// Note: `dateadd` returns
/// If date_or_time_expr is a time, then the return data type is a time.
/// If date_or_time_expr is a timestamp, then the return data type is a timestamp.
/// If date_or_time_expr is a date:
/// - If date_or_time_part is day or larger (for example, month, year), the function returns a DATE value.
/// - If date_or_time_part is smaller than a day (for example, hour, minute, second), the function returns a TIMESTAMP_NTZ value, with 00:00:00.000 as the starting time for the date.
/// Usage notes:
/// - When date_or_time_part is year, quarter, or month (or any of their variations),
/// if the result month has fewer days than the original day of the month, the result day of the month might be different from the original day.
/// Examples
/// - dateadd(day, 30, CAST('2024-12-26' AS TIMESTAMP))
impl ScalarUDFImpl for ConvertTimezoneFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "convert_timezone"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }
    //TODO: FIX return type
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        //can be done matheamtically
        match arg_types.len() {
            3 => {
                Ok(arg_types[2].clone())
            }
            2 => {
                Ok(arg_types[1].clone())
            }
            _ => {
                return plan_err!("function requires three arguments");
            }
        }
    }
    //TODO: FIX general logic
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        //two or three
        match args.len() {
            3 => {
                let source_tz = match &args[0] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return plan_err!("Invalid source_tz type format"),
                };
                let target_tz = match &args[1] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return plan_err!("Invalid target_tz type"),
                };
                let source_timestamp_ntz = match &args[2] {
                    ColumnarValue::Scalar(val) => val.clone(),
                    _ => return plan_err!("Invalid source_timestamp_ntz type"),
                };

            }
            2 => {
                let target_tz = match &args[0] {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return plan_err!("Invalid datetime type"),
                };
                let source_timestamp_tz = match &args[1] {
                    ColumnarValue::Scalar(val) => val.clone(),
                    _ => return plan_err!("Invalid datetime type"),
                };
                let tz = match &source_timestamp_tz {
                    ScalarValue::TimestampSecond(_, Some(val)) => val.clone(),
                    ScalarValue::TimestampMillisecond(_, Some(val)) => val.clone(),
                    ScalarValue::TimestampMicrosecond(_, Some(val)) => val.clone(),
                    ScalarValue::TimestampNanosecond(_, Some(val)) => val.clone(),
                    _ => {
                        return plan_err!("Invalid datetime type")
                    }
                };

                // if (target_tz.as_str() == tz.clone()) {
                    
                // }
            }
            _ => {
                return plan_err!("function requires three or two arguments, got {}", args.len());
            }
        }

        
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(0))))
    }
}
