use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Date32, Date64, Time32, Time64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};
use std::any::Any;

#[derive(Debug)]
pub struct ConvertTimezoneFunc {
    signature: Signature,
    #[allow(dead_code)]
    aliases: Vec<String>,
}

impl Default for ConvertTimezoneFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ConvertTimezoneFunc {
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
            aliases: vec![String::from("convert_timezone")],
        }
    }
}

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
