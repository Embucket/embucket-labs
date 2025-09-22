use datafusion::arrow::array::{Array, ArrayRef, Int64Array, LargeStringBuilder, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{logical_int64, logical_string};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_doc::Documentation;
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{StringBuildLike, Utf8LikeArray};

#[user_doc(
    doc_section(label = "String and Binary Functions"),
    description = "Tokenizes a given string and returns the requested part. If the requested part does not exist, then NULL is returned. If any parameter is NULL, then NULL is returned.",
    syntax_example = "STRTOK(<string> [,<delimiter>] [,<partNr>])",
    sql_example = r"```sql
> SELECT STRTOK('a.b.c', '.', 1);
+------------------------------------------+
| strtok(Utf8(\a.b.c\),Utf8(\.\),Int64(1)) |
+------------------------------------------+
| a                                        |
+------------------------------------------+
```",
    standard_argument(name = "string"),
    argument(
        name = "delimiter",
        description = "Text representing the set of delimiters to tokenize on."
    ),
    argument(name = "partNr", description = "Requested token, which is 1-based.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StrtokFunc {
    signature: Signature,
}

impl Default for StrtokFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StrtokFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::String(1),
                    TypeSignature::String(2),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_int64())),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StrtokFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "strtok"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let datatype = arg_types[0].clone();
        match arg_types[0].clone() {
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
            _ => Ok(datatype),
        }
    }

    #[allow(clippy::unwrap_used)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let (first_arg, second_arg, third_arg) = match args.len() {
            1 => (
                resolve_arg(&args[0])?,
                ScalarValue::Utf8(Some(" ".to_string())).to_array()?,
                ScalarValue::UInt64(Some(1)).to_array()?,
            ),
            2 => (
                resolve_arg(&args[0])?,
                resolve_arg(&args[1])?,
                ScalarValue::UInt64(Some(1)).to_array()?,
            ),
            _ => (
                resolve_arg(&args[0])?,
                resolve_arg(&args[1])?,
                resolve_arg(&args[2])?,
            ),
        };

        let strs = Utf8LikeArray::try_from_array(&first_arg)?;
        let delms = Utf8LikeArray::try_from_array(&second_arg)?;
        let part_nrs = as_int64_array(&third_arg)?;

        let res = match (&strs, &delms) {
            (Utf8LikeArray::LargeUtf8(_), _) | (_, Utf8LikeArray::LargeUtf8(_)) => {
                process_split(&strs, &delms, part_nrs, LargeStringBuilder::new())?
            }
            _ => process_split(&strs, &delms, part_nrs, StringBuilder::new())?,
        };

        Ok(ColumnarValue::Array(res))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn resolve_arg(arg: &ColumnarValue) -> DFResult<ArrayRef> {
    match arg {
        ColumnarValue::Array(arr) => Ok(Arc::clone(arr)),
        ColumnarValue::Scalar(scalar) => scalar.to_array(),
    }
}

#[allow(clippy::unwrap_used)]
fn process_split<B>(
    strs: &Utf8LikeArray,
    delms: &Utf8LikeArray,
    part_nrs: &Int64Array,
    mut builder: B,
) -> DFResult<ArrayRef>
where
    B: StringBuildLike,
{
    for i in 0..strs.len() {
        if strs.is_null(i) || delms.is_null(i) || part_nrs.is_null(i) {
            builder.append_null();
            continue;
        }

        let string = strs.value(i).unwrap();
        let delimiter = delms.value(i).unwrap();
        let part_nr: usize = part_nrs.value(i).try_into().unwrap();

        if part_nr < 1 {
            return exec_err!("partNr cannot be less than 1");
        }

        let delimiter_set: HashSet<char> = delimiter.chars().collect();
        let mut tokens = vec![];
        let mut last_split_index: usize = 0;

        for (i, ch) in string.char_indices() {
            if delimiter_set.contains(&ch) {
                let value = &string[last_split_index..i];
                if !value.is_empty() {
                    tokens.push(value);
                }
                last_split_index = i + ch.len_utf8();
            }
        }

        let tail = &string[last_split_index..];
        if !tail.is_empty() {
            tokens.push(tail);
        }

        if part_nr > tokens.len() {
            builder.append_null();
        } else {
            builder.append_value(tokens[part_nr - 1]);
        }
    }

    Ok(Arc::new(builder.finish()))
}

crate::macros::make_udf_function!(StrtokFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, assert_batches_eq};
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StrtokFunc::new()));

        let q = "SELECT STRTOK('a.b.c', '.', 1);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------------------------------+",
                "| strtok(Utf8(\"a.b.c\"),Utf8(\".\"),Int64(1)) |",
                "+------------------------------------------+",
                "| a                                        |",
                "+------------------------------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK('user@snowflake.com', '@.', 1);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------------------------------------------------+",
                "| strtok(Utf8(\"user@snowflake.com\"),Utf8(\"@.\"),Int64(1)) |",
                "+--------------------------------------------------------+",
                "| user                                                   |",
                "+--------------------------------------------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK('user@snowflake.com', '@.', 2);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------------------------------------------------+",
                "| strtok(Utf8(\"user@snowflake.com\"),Utf8(\"@.\"),Int64(2)) |",
                "+--------------------------------------------------------+",
                "| snowflake                                              |",
                "+--------------------------------------------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK('user@snowflake.com', '@.', 3);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------------------------------------------------+",
                "| strtok(Utf8(\"user@snowflake.com\"),Utf8(\"@.\"),Int64(3)) |",
                "+--------------------------------------------------------+",
                "| com                                                    |",
                "+--------------------------------------------------------+",
            ],
            &result
        );

        // Indexing past the last possible token returns NULL
        let q = "SELECT STRTOK('user@snowflake.com.', '@.', 4);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------------------------------+",
                "| strtok(Utf8(\"user@snowflake.com.\"),Utf8(\"@.\"),Int64(4)) |",
                "+---------------------------------------------------------+",
                "|                                                         |",
                "+---------------------------------------------------------+",
            ],
            &result
        );

        // In this example, because the input string is empty, there are 0 elements, and therefore element #1
        // is past the end of the string, so the function returns NULL rather than an empty string
        let q = "SELECT STRTOK('', '', 1);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------------------------+",
                "| strtok(Utf8(\"\"),Utf8(\"\"),Int64(1)) |",
                "+------------------------------------+",
                "|                                    |",
                "+------------------------------------+",
            ],
            &result
        );

        // Empty delimeter
        let q = "SELECT STRTOK('a.b', '', 1);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------------+",
                "| strtok(Utf8(\"a.b\"),Utf8(\"\"),Int64(1)) |",
                "+---------------------------------------+",
                "| a.b                                   |",
                "+---------------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_null_args() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StrtokFunc::new()));

        let q = "SELECT STRTOK(NULL, '.', 1);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------------------------------+",
                "| strtok(NULL,Utf8(\".\"),Int64(1)) |",
                "+---------------------------------+",
                "|                                 |",
                "+---------------------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK('a.b', NULL, 1);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-----------------------------------+",
                "| strtok(Utf8(\"a.b\"),NULL,Int64(1)) |",
                "+-----------------------------------+",
                "|                                   |",
                "+-----------------------------------+",
            ],
            &result
        );

        let q = "SELECT STRTOK('a.b', '.', NULL);";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------------------------+",
                "| strtok(Utf8(\"a.b\"),Utf8(\".\"),NULL) |",
                "+------------------------------------+",
                "|                                    |",
                "+------------------------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_argument_fails() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(StrtokFunc::new()));

        // Zero arguments
        let q = "SELECT STRTOK('a.b.c', '.', 0);";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => assert!(
                    matches!(e, DataFusionError::Execution(_)),
                    "Expected Execution error for partNr less than 1, got: {e}",
                ),
                Ok(_) => panic!("Expected error but partNr less than 1 succeeded"),
            }
        }
    }
}
