<<<<<<< HEAD:crates/control_plane/src/sql/functions/convert_timezone.rs
use arrow::datatypes::DataType::{Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{self, Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::DataType;
use chrono::Local;
use datafusion::common::{internal_err, plan_err, Result};
=======
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Date32, Date64, Time32, Time64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use datafusion::common::{plan_err, Result};
>>>>>>> origin/main:crates/runtime/src/datafusion/functions/convert_timezone.rs
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};
<<<<<<< HEAD:crates/control_plane/src/sql/functions/convert_timezone.rs
use datafusion::scalar::ScalarValue;
use regex::Regex;
use std::any::Any;
use arrow::array::timezone::Tz;
use std::sync::Arc;
use datafusion::prelude::Expr;
use datafusion::common::ExprSchema;
=======
use std::any::Any;
>>>>>>> origin/main:crates/runtime/src/datafusion/functions/convert_timezone.rs

#[derive(Debug)]
pub struct ConvertTimezoneFunc {
    signature: Signature,
<<<<<<< HEAD:crates/control_plane/src/sql/functions/convert_timezone.rs
=======
    #[allow(dead_code)]
    aliases: Vec<String>,
>>>>>>> origin/main:crates/runtime/src/datafusion/functions/convert_timezone.rs
}

impl Default for ConvertTimezoneFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ConvertTimezoneFunc {
<<<<<<< HEAD:crates/control_plane/src/sql/functions/convert_timezone.rs
    pub fn new() -> Self { 
        Self {
            signature: Signature::one_of(
                vec![
=======
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Date32]),
                    Exact(vec![Utf8, Date64]),
                    Exact(vec![Utf8, Time32(Second)]),
                    Exact(vec![Utf8, Time32(Nanosecond)]),
                    Exact(vec![Utf8, Time32(Microsecond)]),
                    Exact(vec![Utf8, Time32(Millisecond)]),
                    Exact(vec![Utf8, Time64(Second)]),
                    Exact(vec![Utf8, Time64(Nanosecond)]),
                    Exact(vec![Utf8, Time64(Microsecond)]),
                    Exact(vec![Utf8, Time64(Millisecond)]),
                    Exact(vec![Utf8, Timestamp(Second, None)]),
                    Exact(vec![Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![Utf8, Timestamp(Nanosecond, None)]),
>>>>>>> origin/main:crates/runtime/src/datafusion/functions/convert_timezone.rs
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
<<<<<<< HEAD:crates/control_plane/src/sql/functions/convert_timezone.rs
                    // Exact(vec![
                    //     Utf8,
                    //     Utf8,
                    // ]),
                    // Exact(vec![
                    //     Utf8,
                    //     Timestamp(Second, None),
                    // ]),
                    // Exact(vec![
                    //     Utf8,
                    //     Timestamp(Millisecond, None),
                    // ]),
                    // Exact(vec![
                    //     Utf8,
                    //     Timestamp(Microsecond, None),
                    // ]),
                    // Exact(vec![
                    //     Utf8,
                    //     Timestamp(Nanosecond, None),
                    // ]),
                    Exact(vec![Utf8, Utf8, Timestamp(Second, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![Utf8, Utf8, Timestamp(Nanosecond, None)]),
                    // Exact(vec![Utf8, Utf8, Utf8]),
                ],
                Volatility::Immutable,
            ),
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
=======
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("convert_timezone")],
        }
    }
}

>>>>>>> origin/main:crates/runtime/src/datafusion/functions/convert_timezone.rs
impl ScalarUDFImpl for ConvertTimezoneFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
<<<<<<< HEAD:crates/control_plane/src/sql/functions/convert_timezone.rs
    fn name(&self) -> &str {
        "convert_timezone"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        return internal_err!("return_types_from_exprs should be called")
    }
    fn return_type_from_exprs(
            &self,
            args: &[Expr],
            _schema: &dyn ExprSchema,
            arg_types: &[DataType],
        ) -> Result<DataType> {
        match args.len() {
            2 => {
                let tz = match &args[0] {
                    Expr::Literal(ScalarValue::Utf8(Some(part))) => part.clone(),
                    _ => return internal_err!("Invalid target_tz type"),
                };

                match &arg_types[1] {
                    DataType::Timestamp(tu, _) => Ok(DataType::Timestamp(*tu, Some(Arc::from(tz.into_boxed_str())))),
                    // DataType::Utf8 => Ok(DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(tz.into_boxed_str())))),
                    _ => return internal_err!("Invalid source_timestamp_tz type"),
                }
            },
            3 => {
                match &arg_types[2] {
                    DataType::Timestamp(tu, None) => Ok(DataType::Timestamp(*tu, None)),
                    // DataType::Utf8 => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                    _ => return internal_err!("Invalid source_timestamp_ntz type"),
                }
            },
            other => return internal_err!("This function can only take two or three arguments, got {}", other),
        }
    }
    //TODO: select convert_timezone('UTC', v3) with v3 a timestamp with value = '2025-01-06 08:00:00', 
    //should use local session time
    //TODO: select convert_timezone('America/New_York, 'UTC', v3) with v3 a timestamp with value = '2025-01-06 08:00:00 America/New_York', 
    //should be parsed as the timezone None variant timestamp
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
                        let modified_timestamp = ScalarValue::TimestampSecond(Some(*ts), Some(Arc::from(target_tz.clone().into_boxed_str())))
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Second, Some(Arc::from(target_tz.into_boxed_str()))))?;
                        // dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    },
                    ScalarValue::TimestampMillisecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampMillisecond(Some(*ts), Some(Arc::from(target_tz.clone().into_boxed_str())))
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Millisecond, Some(Arc::from(target_tz.into_boxed_str()))))?;
                        // dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    },
                    ScalarValue::TimestampMicrosecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampMicrosecond(Some(*ts), Some(Arc::from(target_tz.clone().into_boxed_str())))
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Microsecond, Some(Arc::from(target_tz.into_boxed_str()))))?;
                        // dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    },
                    ScalarValue::TimestampNanosecond(Some(ts), Some(_)) => {
                        let modified_timestamp = ScalarValue::TimestampNanosecond(Some(*ts), Some(Arc::from(target_tz.clone().into_boxed_str())))
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(target_tz.into_boxed_str()))))?;
                        // dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))                    
                    },
                    // ScalarValue::Utf8(Some(part)) => {
                    //     //TODO: add local time zone if no time zone
                    //     dbg!(part.clone());
                    //     let timezone = regex::Regex::new(r"(Z|[+-]\d{2}:\d{2}|[A-Za-z]+/[A-Za-z_]+)").unwrap();
                    //     if let Some(matched) = timezone.find(part.as_str()) {
                    //         let modified_timestamp = ScalarValue::Utf8(Some(part.clone()))
                    //             .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))))?
                    //             .cast_to(&Utf8)?
                    //             .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(target_tz.into_boxed_str()))))?;
                    //         //dbg!(&modified_timestamp.cast_to(&Utf8)?);
                    //         return Ok(ColumnarValue::Scalar(modified_timestamp))
                    //     }
                    //     let local_tz = Local::now().offset().to_string();
                    //     let modified_timestamp = ScalarValue::Utf8(Some(part.clone()))
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(local_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(target_tz.into_boxed_str()))))?;
                    //     //dbg!(&modified_timestamp.cast_to(&Utf8)?);
                    //     Ok(ColumnarValue::Scalar(modified_timestamp))
                    // },
                    // ScalarValue::TimestampSecond(Some(ts), None) => {
                    //     let local_tz = Local::now().offset().to_string();
                    //     let modified_timestamp = ScalarValue::TimestampSecond(Some(*ts), None)
                    //         .cast_to(&Timestamp(TimeUnit::Second, Some(Arc::from(local_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Second, Some(Arc::from(target_tz.into_boxed_str()))))?
                    //         ;
                    //     dbg!(&modified_timestamp.cast_to(&Utf8)?);
                    //     Ok(ColumnarValue::Scalar(modified_timestamp))
                    // },
                    // ScalarValue::TimestampMillisecond(Some(ts), None) => {
                    //     let local_tz = Local::now().offset().to_string();
                    //     let modified_timestamp = ScalarValue::TimestampMillisecond(Some(*ts), None)
                    //         .cast_to(&Timestamp(TimeUnit::Millisecond, Some(Arc::from(local_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Millisecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                    //         ;
                    //     dbg!(&modified_timestamp.cast_to(&Utf8)?);
                    //     Ok(ColumnarValue::Scalar(modified_timestamp))
                    // },
                    // ScalarValue::TimestampMicrosecond(Some(ts), None) => {
                    //     let local_tz = Local::now().offset().to_string();
                    //     let modified_timestamp = ScalarValue::TimestampMicrosecond(Some(*ts), None)
                    //         .cast_to(&Timestamp(TimeUnit::Microsecond, Some(Arc::from(local_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Microsecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                    //         ;
                    //     dbg!(&modified_timestamp.cast_to(&Utf8)?);
                    //     Ok(ColumnarValue::Scalar(modified_timestamp))
                    // },
                    // ScalarValue::TimestampNanosecond(Some(ts), None) => {
                    //     let local_tz = Local::now().offset().to_string();
                    //     let modified_timestamp = ScalarValue::TimestampNanosecond(Some(*ts), None)
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(local_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                    //         ;
                    //     dbg!(&modified_timestamp.cast_to(&Utf8)?);
                    //     Ok(ColumnarValue::Scalar(modified_timestamp))                    
                    // },
                    _ => {
                        return plan_err!("Invalid source_timestamp_tz type format")
                    }
                }
            },
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
                            .cast_to(&Timestamp(TimeUnit::Second, Some(Arc::from(source_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Second, Some(Arc::from(target_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Second, None))?;
                        dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    },
                    ScalarValue::TimestampMillisecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampMillisecond(Some(*ts), None)
                            .cast_to(&Timestamp(TimeUnit::Millisecond, Some(Arc::from(source_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Millisecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Millisecond, None))?;
                        dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    },
                    ScalarValue::TimestampMicrosecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampMicrosecond(Some(*ts), None)
                            .cast_to(&Timestamp(TimeUnit::Microsecond, Some(Arc::from(source_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Microsecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Microsecond, None))?;
                        dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))
                    },
                    ScalarValue::TimestampNanosecond(Some(ts), None) => {
                        let modified_timestamp = ScalarValue::TimestampNanosecond(Some(*ts), None)
                            .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(source_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                            .cast_to(&Utf8)?
                            .cast_to(&Timestamp(TimeUnit::Nanosecond, None))?;
                        dbg!(&modified_timestamp.cast_to(&Utf8)?);
                        Ok(ColumnarValue::Scalar(modified_timestamp))                    
                    },
                    // ScalarValue::Utf8(Some(val)) => {
                    //     let modified_timestamp = ScalarValue::Utf8(Some(val.clone()))
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, None))?
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(source_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, Some(Arc::from(target_tz.into_boxed_str()))))?
                    //         .cast_to(&Utf8)?
                    //         .cast_to(&Timestamp(TimeUnit::Nanosecond, None))?
                    //         ;
                    //     dbg!(&modified_timestamp.cast_to(&Utf8)?);

                    //     Ok(ColumnarValue::Scalar(modified_timestamp))
                    // },
                    _ => {
                        return plan_err!("Invalid source_timestamp_tz type format")
                    }
                }
            },
            _ => {
                return plan_err!("This function can only take two or three arguments, got {}", args.len());
            }
        }
    }
}
=======

    fn name(&self) -> &'static str {
        "convert_timezone"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        //two or three
        if arg_types.len() != 2 {
            return plan_err!("function requires three arguments");
        }
        Ok(arg_types[1].clone())
    }
    //TODO: FIX general logic
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return plan_err!("function requires three arguments");
        }

        Ok(args[1].clone())
    }
}

super::macros::make_udf_function!(ConvertTimezoneFunc);
>>>>>>> origin/main:crates/runtime/src/datafusion/functions/convert_timezone.rs
