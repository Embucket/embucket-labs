use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, ListBuilder,
    OffsetSizeTrait, StringArrayType, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_common::types::logical_string;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

impl Default for SplitFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Coercible(vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

#[derive(Debug)]
pub struct SplitFunc {
    signature: Signature,
}

impl ScalarUDFImpl for SplitFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "split"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        match arg_types[0] {
            DataType::Utf8 => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
            DataType::LargeUtf8 => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::LargeUtf8,
                true,
            )))),
            DataType::Utf8View => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8View,
                true,
            )))),
            _ => exec_err!("split function only supports Utf8, LargeUtf8, and Utf8View data types"),
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // First, determine if any of the arguments is an Array
        let len = args.iter().find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        });

        let inferred_length = len.unwrap_or(1);
        let is_scalar = len.is_none();

        // Convert all ColumnarValues to ArrayRefs
        let args = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(inferred_length),
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
            })
            .collect::<DFResult<Vec<_>>>()?;

        let result = match (args[0].data_type(), args[1].data_type()) {
            (DataType::Utf8View, DataType::Utf8View) => {
                split_impl::<&StringViewArray, &StringViewArray, i32>(
                    args[0].as_string_view(),
                    args[1].as_string_view(),
                )
            }
            (DataType::Utf8View, DataType::Utf8) => {
                split_impl::<&StringViewArray, &GenericStringArray<i32>, i32>(
                    args[0].as_string_view(),
                    args[1].as_string::<i32>(),
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8) => {
                split_impl::<&StringViewArray, &GenericStringArray<i64>, i32>(
                    args[0].as_string_view(),
                    args[1].as_string::<i64>(),
                )
            }
            (DataType::Utf8, DataType::Utf8View) => {
                split_impl::<&GenericStringArray<i32>, &StringViewArray, i32>(
                    args[0].as_string::<i32>(),
                    args[1].as_string_view(),
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View) => {
                split_impl::<&GenericStringArray<i64>, &StringViewArray, i64>(
                    args[0].as_string::<i64>(),
                    args[1].as_string_view(),
                )
            }
            (DataType::Utf8, DataType::Utf8) => {
                split_impl::<&GenericStringArray<i32>, &GenericStringArray<i32>, i32>(
                    args[0].as_string::<i32>(),
                    args[1].as_string::<i32>(),
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                split_impl::<&GenericStringArray<i64>, &GenericStringArray<i64>, i64>(
                    args[0].as_string::<i64>(),
                    args[1].as_string::<i64>(),
                )
            }
            (DataType::Utf8, DataType::LargeUtf8) => {
                split_impl::<&GenericStringArray<i32>, &GenericStringArray<i64>, i32>(
                    args[0].as_string::<i32>(),
                    args[1].as_string::<i64>(),
                )
            }
            (DataType::LargeUtf8, DataType::Utf8) => {
                split_impl::<&GenericStringArray<i64>, &GenericStringArray<i32>, i64>(
                    args[0].as_string::<i64>(),
                    args[1].as_string::<i32>(),
                )
            }
            _ => {
                return exec_err!(
                    "split function only supports Utf8View, Utf8, and LargeUtf8 data types"
                );
            }
        };

        if is_scalar {
            // If all inputs are scalar, keep the output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

pub fn split_impl<'a, StringArrType, DelimiterArrType, StringArrayLen>(
    string_array: StringArrType,
    delimiter_array: DelimiterArrType,
) -> DFResult<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    DelimiterArrType: StringArrayType<'a>,
    StringArrayLen: OffsetSizeTrait,
{
    let string_builder: GenericStringBuilder<StringArrayLen> = GenericStringBuilder::new();
    let mut list_builder = ListBuilder::new(string_builder);

    string_array
        .iter()
        .zip(delimiter_array.iter())
        .try_for_each(|(str, delimiter)| -> DFResult<()> {
            match (str, delimiter) {
                (Some(str), Some(delimiter)) => {
                    let parts: Vec<&str> = str.split(delimiter).collect();
                    list_builder.append_value(parts.iter().map(|s| Some(*s)));
                }
                _ => list_builder.append_null(),
            }

            Ok(())
        })?;

    Ok(Arc::new(list_builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_boolean() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(SplitFunc::new()));
        let q = "CREATE OR REPLACE TABLE strs(str string, sep string);";
        ctx.sql(q).await?.collect().await?;

        let q = "INSERT INTO strs VALUES ('hello world', ' '), ('a.b.c','.'), ('abc',','),(null,'.'),('a',null);";
        ctx.sql(q).await?.collect().await?;

        let q = "SELECT split(str,sep) FROM strs;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------------------------+",
                "| split(strs.str,strs.sep) |",
                "+--------------------------+",
                "| [hello, world]           |",
                "| [a, b, c]                |",
                "| [abc]                    |",
                "|                          |",
                "|                          |",
                "+--------------------------+",
            ],
            &result
        );
        Ok(())
    }
}
