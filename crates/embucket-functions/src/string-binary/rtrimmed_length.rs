use datafusion::arrow::{array::UInt64Array, datatypes::DataType};
use datafusion::error::Result as DFResult;
use datafusion_common::cast::as_string_array;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

// rtrimmed_length SQL function
// Returns the length of its argument, minus trailing whitespace, but including leading whitespace.
// Syntax: RTRIMMED_LENGTH( <string_expr> )
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RTrimmedLengthFunc {
    signature: Signature,
}

impl Default for RTrimmedLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RTrimmedLengthFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RTrimmedLengthFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "rtrimmed_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    #[allow(clippy::as_conversions)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let arr = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let strs = as_string_array(&arr)?;

        let new_array = strs
            .iter()
            .map(|array_elem| array_elem.map(|value| value.trim_end_matches(' ').len() as u64))
            .collect::<UInt64Array>();

        Ok(ColumnarValue::Array(Arc::new(new_array)))
    }
}

crate::macros::make_udf_function!(RTrimmedLengthFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(RTrimmedLengthFunc::new()));

        let create = "CREATE OR REPLACE TABLE test_strings (s STRING);";
        ctx.sql(create).await?.collect().await?;

        let insert = r"
          INSERT INTO test_strings VALUES
              ('  ABCD  '),
              ('   ABCDEFG'),
              ('ABCDEFGH  '),
              ('   '),
              (''),
              ('ABC'),
              (E'ABCDEFGH  \t'),
              (E'ABCDEFGH  \n'),
              (NULL);
          ";
        ctx.sql(insert).await?.collect().await?;

        let q = "SELECT RTRIMMED_LENGTH(s) FROM test_strings;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------+",
                "| rtrimmed_length(test_strings.s) |",
                "+---------------------------------+",
                "| 6                               |",
                "| 10                              |",
                "| 8                               |",
                "| 0                               |",
                "| 0                               |",
                "| 3                               |",
                "| 11                              |",
                "| 11                              |",
                "|                                 |",
                "+---------------------------------+",
            ],
            &result
        );
        Ok(())
    }
}
