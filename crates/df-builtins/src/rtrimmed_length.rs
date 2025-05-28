use datafusion::arrow::{array::UInt64Array, datatypes::DataType};
use datafusion::error::Result as DFResult;
use datafusion_common::cast::as_string_array;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

// RTRIMMED_LENGTH SQL function implementation
// This function calculates the length of a string after removing trailing whitespace characters.
// It preserves leading whitespace but removes all trailing spaces from the input string.
// Returns the length of its argument, minus trailing whitespace, but including leading whitespace.
// Syntax: RTRIMMED_LENGTH( <string_expr> )
//
// Examples:
// - RTRIMMED_LENGTH('  hello  ') returns 7 (includes leading spaces, excludes trailing spaces)
// - RTRIMMED_LENGTH('hello') returns 5 (no whitespace to trim)
// - RTRIMMED_LENGTH('   ') returns 0 (all spaces are trailing)
#[derive(Debug)]
pub struct RTrimmedLengthFunc {
    signature: Signature,
}

impl Default for RTrimmedLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RTrimmedLengthFunc {
    /// Creates a new instance of the RTRIMMED_LENGTH function
    /// This function accepts exactly one UTF-8 string argument and is immutable
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

        // Extract the input array from either Array or Scalar columnar value
        let arr = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        // Convert to string array for processing
        let strs = as_string_array(&arr)?;

        // Process each string element: trim trailing spaces and calculate length
        // This is the core logic that removes trailing whitespace and measures the result
        let new_array = strs
            .iter()
            .map(|array_elem| {
                array_elem.map(|value| {
                    // Remove trailing spaces and get the length of the trimmed string
                    value.trim_end_matches(' ').len() as u64
                })
            })
            .collect::<UInt64Array>();

        // Return the result as a columnar array
        Ok(ColumnarValue::Array(Arc::new(new_array)))
    }
}

super::macros::make_udf_function!(RTrimmedLengthFunc);

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
