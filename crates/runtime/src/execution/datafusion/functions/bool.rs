use arrow::datatypes::i256;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use half::f16;
use std::any::Any;

// booland SQL function
// Computes the Boolean AND of two numeric expressions. In accordance with Boolean semantics:
// Syntax: BOOLAND( <expr1> , <expr2> )
// - <expr1>: Non-zero values (including negative numbers) are regarded as True. Zero values are regarded as False.
// - <expr2>: Non-zero values (including negative numbers) are regarded as True. Zero values are regarded as False.
// Note: `booland` returns
// - True if both expressions are non-zero.
// - False if both expressions are zero or one expression is zero and the other expression is non-zero or NULL.
// - NULL if both expressions are NULL or one expression is NULL and the other expression is non-zero.
#[derive(Debug)]
pub struct BoolAndFunc {
    signature: Signature,
}

impl Default for BoolAndFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolAndFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolAndFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "booland"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let lhs = match &args.args[0] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        let rhs = match &args.args[1] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        if lhs.is_null() && rhs.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        if (lhs.is_null() || rhs.is_null()) && (is_true(&lhs)? || is_true(&rhs)?) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::from(
            is_true(&lhs)? && is_true(&rhs)?,
        )))
    }
}

// boolor SQL function
// Computes the Boolean OR of two numeric expressions. In accordance with Boolean semantics:
// - Non-zero values (including negative numbers) are regarded as True.
// - Zero values are regarded as False.
// Syntax: BOOLOR( <expr1> , <expr2> )
// Note: `boolor` returns
// - True if both expressions are non-zero or one expression is non-zero and the other expression is zero or NULL.
// - False if both expressions are zero.
// - NULL if both expressions are NULL or one expression is NULL and the other expression is zero.
#[derive(Debug)]
pub struct BoolOrFunc {
    signature: Signature,
}

impl Default for BoolOrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolOrFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolOrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "boolor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let lhs = match &args.args[0] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        let rhs = match &args.args[1] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        if lhs.is_null() && rhs.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        if (lhs.is_null() || rhs.is_null()) && (!is_true(&lhs)? && !is_true(&rhs)?) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::from(
            is_true(&lhs)? || is_true(&rhs)?,
        )))
    }
}

// boolxor SQL function
// Computes the Boolean XOR of two numeric expressions (i.e. one of the expressions, but not both expressions, is TRUE). In accordance with Boolean semantics:
// - Non-zero values (including negative numbers) are regarded as True.
// - Zero values are regarded as False.
// Syntax: BOOLXOR( <expr1> , <expr2> )
// Note: `boolxor` returns
// - True if one expression is non-zero and the other expression is zero.
// - False if both expressions are non-zero or both expressions are zero.
// - NULL if one or both expressions are NULL.
#[derive(Debug)]
pub struct BoolXorFunc {
    signature: Signature,
}

impl Default for BoolXorFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BoolXorFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::comparable(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoolXorFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "boolxor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let lhs = match &args.args[0] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        let rhs = match &args.args[1] {
            ColumnarValue::Scalar(val) => val.to_owned(),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0)?,
        };

        if lhs.is_null() && rhs.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        if lhs.is_null() || rhs.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        if (is_true(&lhs)? && !is_true(&rhs)?) || (!is_true(&lhs)? && is_true(&rhs)?) {
            Ok(ColumnarValue::Scalar(ScalarValue::from(Some(true))))
        } else if (is_true(&lhs)? && is_true(&rhs)?) || (!is_true(&lhs)? && !is_true(&rhs)?) {
            Ok(ColumnarValue::Scalar(ScalarValue::from(Some(false))))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
        }
    }
}

fn is_true(v: &ScalarValue) -> DFResult<bool> {
    if v.is_null() {
        return Ok(false);
    }
    Ok(match v {
        ScalarValue::Float16(Some(v)) => *v != f16::from_f32(0.0),
        ScalarValue::Float32(Some(v)) => *v != 0.,
        ScalarValue::Float64(Some(v)) => *v != 0.,
        ScalarValue::Decimal128(Some(v), _, _) => *v != 0,
        ScalarValue::Decimal256(Some(v), _, _) => *v != i256::from_i128(0),
        ScalarValue::Int8(Some(v)) => *v != 0,
        ScalarValue::Int16(Some(v)) => *v != 0,
        ScalarValue::Int32(Some(v)) => *v != 0,
        ScalarValue::Int64(Some(v)) => *v != 0,
        ScalarValue::UInt8(Some(v)) => *v != 0,
        ScalarValue::UInt16(Some(v)) => *v != 0,
        ScalarValue::UInt32(Some(v)) => *v != 0,
        ScalarValue::UInt64(Some(v)) => *v != 0,
        _ => {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Unsupported type {:?}",
                v.data_type()
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_bool_and() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolAndFunc::new()));
        let q = "SELECT BOOLAND(1, -2), BOOLAND(0, 2.35), BOOLAND(0, 0), BOOLAND(0, NULL), BOOLAND(NULL, 3), BOOLAND(NULL, NULL);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
        &[
"+-----------------------------+---------------------------------+----------------------------+------------------------+------------------------+--------------------+",
"| booland(Int64(1),Int64(-2)) | booland(Int64(0),Float64(2.35)) | booland(Int64(0),Int64(0)) | booland(Int64(0),NULL) | booland(NULL,Int64(3)) | booland(NULL,NULL) |",
"+-----------------------------+---------------------------------+----------------------------+------------------------+------------------------+--------------------+",
"| true                        | false                           | false                      | false                  |                        |                    |",
"+-----------------------------+---------------------------------+----------------------------+------------------------+------------------------+--------------------+",
        ],
        &result
    );

        Ok(())
    }

    #[tokio::test]
    async fn test_bool_or() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolOrFunc::new()));
        let q = "SELECT BOOLOR(1, 2), BOOLOR(-1.35, 0), BOOLOR(3, NULL), BOOLOR(0, 0), BOOLOR(NULL, 0), BOOLOR(NULL, NULL);
";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
        &[
"+---------------------------+---------------------------------+-----------------------+---------------------------+-----------------------+-------------------+",
"| boolor(Int64(1),Int64(2)) | boolor(Float64(-1.35),Int64(0)) | boolor(Int64(3),NULL) | boolor(Int64(0),Int64(0)) | boolor(NULL,Int64(0)) | boolor(NULL,NULL) |",
"+---------------------------+---------------------------------+-----------------------+---------------------------+-----------------------+-------------------+",
"| true                      | true                            | true                  | false                     |                       |                   |",
"+---------------------------+---------------------------------+-----------------------+---------------------------+-----------------------+-------------------+",
        ],
        &result
    );
        Ok(())
    }

    #[tokio::test]
    async fn test_bool_xor() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BoolXorFunc::new()));
        let q = "SELECT BOOLXOR(2, 0), BOOLXOR(1, -1), BOOLXOR(0, 0), BOOLXOR(NULL, 3), BOOLXOR(NULL, 0), BOOLXOR(NULL, NULL);
";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
        &[
"+----------------------------+-----------------------------+----------------------------+------------------------+------------------------+--------------------+",
"| boolxor(Int64(2),Int64(0)) | boolxor(Int64(1),Int64(-1)) | boolxor(Int64(0),Int64(0)) | boolxor(NULL,Int64(3)) | boolxor(NULL,Int64(0)) | boolxor(NULL,NULL) |",
"+----------------------------+-----------------------------+----------------------------+------------------------+------------------------+--------------------+",
"| true                       | false                       | false                      |                        |                        |                    |",
"+----------------------------+-----------------------------+----------------------------+------------------------+------------------------+--------------------+",
        ],
        &result
    );
        Ok(())
    }
}
