use datafusion::arrow::array::{BinaryBuilder, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion_common::cast::{as_binary_array, as_int64_array, as_string_array};
use datafusion_common::{exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

// insert SQL function
// Replaces a substring of the specified length, starting at the specified position, with a
// new string or binary value.
// This function should not be confused with the INSERT DML command.
// Syntax: INSERT( <base_expr>, <pos>, <len>, <insert_expr> )
// Arguments:
// <base_expr>
// The string or BINARY expression for which you want to insert/replace characters.
// <pos>
// The offset at which to start inserting characters. This is 1-based, not 0-based.
// In other words, the first character in the string is considered to be at position 1,
// not position 0. For example, to insert at the beginning of the string, set <pos> to 1.
// Valid values are between 1 and one more than the length of the string (inclusive).
// Setting <pos> to one more than the length of the string makes the operation equivalent
// to an append. (This also requires that the <len> parameter be 0 because you should not
// try to delete any characters past the last character.)
// <len>
// The number of characters (starting at <pos>) that you want to replace. Valid values range
// from 0 to the number of characters between <pos> and the end of the string. If this is 0,
// it means add the new characters without deleting any existing characters.
// <insert_expr>
// The string to insert into the base_expr. If this string is empty, and if len is greater than
// zero, then effectively the operation becomes a delete (some characters are deleted, and none are added).
// Usage notes:
// The <base_expr> and <insert_expr> should be the same data type; either both should be string
// (e.g. VARCHAR) or both should be binary.
// If any of the arguments are NULL, the returned value is NULL.
// Note: `insert` returns
// Returns a string or BINARY that is equivalent to making a copy of <base_expr>, deleting <len> characters
// starting at <pos>, and then inserting <insert_expr> at <pos>.
// Note that the original input base_expr is not changed; the function returns a separate (modified) copy.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Insert {
    signature: Signature,
}

impl Default for Insert {
    fn default() -> Self {
        Self::new()
    }
}

impl Insert {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Binary,
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Binary,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Insert {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "insert"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match (arg_types[0].clone(), arg_types[3].clone()) {
            (DataType::Utf8, DataType::Utf8) => Ok(DataType::Utf8),
            (DataType::Binary, DataType::Binary) => Ok(DataType::Binary),
            _ => plan_err!("Base expression and insert expression should be the same data type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let base_arr = match &args[0] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let pos_arr = match &args[1] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let len_arr = match &args[2] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let insert_arr = match &args[3] {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(v) => &v.to_array()?,
        };

        let res_arr: Arc<dyn datafusion::arrow::array::Array> = match base_arr.data_type() {
            DataType::Utf8 => {
                let base_strs = as_string_array(&base_arr)?;
                let pos_i64s = as_int64_array(&pos_arr)?;
                let len_i64s = as_int64_array(&len_arr)?;
                let insert_strs = as_string_array(&insert_arr)?;

                let mut res = StringBuilder::new();

                let zipped = crate::macros::izip!(
                    base_strs.iter(),
                    pos_i64s.iter(),
                    len_i64s.iter(),
                    insert_strs.iter()
                );

                for (base, pos, len, insert) in zipped {
                    if base.is_none() || pos.is_none() || len.is_none() || insert.is_none() {
                        res.append_null();
                        continue;
                    }

                    let inserted_string = insert_string(base, pos, len, insert);
                    match inserted_string {
                        Ok(v) => res.append_value(v),
                        Err(e) => return Err(e),
                    }
                }
                Arc::new(res.finish())
            }
            DataType::Binary => {
                let base_bin = as_binary_array(&base_arr)?;
                let pos_i64s = as_int64_array(&pos_arr)?;
                let len_i64s = as_int64_array(&len_arr)?;
                let insert_bin = as_binary_array(&insert_arr)?;

                let mut res = BinaryBuilder::new();

                let zipped = crate::macros::izip!(
                    base_bin.iter(),
                    pos_i64s.iter(),
                    len_i64s.iter(),
                    insert_bin.iter()
                );

                for (base, pos, len, insert) in zipped {
                    if base.is_none() || pos.is_none() || len.is_none() || insert.is_none() {
                        res.append_null();
                        continue;
                    }

                    let inserted_binary = insert_binary(base, pos, len, insert);
                    match inserted_binary {
                        Ok(v) => res.append_value(v),
                        Err(e) => return Err(e),
                    }
                }

                Arc::new(res.finish())
            }
            _ => {
                return exec_err!("Invalid datatype");
            }
        };

        Ok(ColumnarValue::Array(res_arr))
    }
}

#[allow(clippy::unwrap_used)]
fn insert_string(
    base: Option<&str>,
    pos: Option<i64>,
    len: Option<i64>,
    insert: Option<&str>,
) -> Result<String, datafusion_common::DataFusionError> {
    let base = base.unwrap();
    let base_len: i64 = base.len().try_into().unwrap();
    let pos = pos.unwrap();
    let len = len.unwrap();
    let insert = insert.unwrap();

    if pos < 1 || pos > base_len + 1 {
        return exec_err!(
            "Valid values for position are between 1 and one more than length of string"
        );
    }

    if len < 0 || len > (base_len - pos + 1) {
        return exec_err!(
            "Valid values for length range from 0 to the number of characters from position to end of string"
        );
    }

    let mut chs = base.chars();
    let left_part = chs
        .by_ref()
        .take((pos - 1).try_into().unwrap())
        .collect::<String>();
    let right_part = chs.skip(len.try_into().unwrap()).collect::<String>();

    Ok(format!("{left_part}{insert}{right_part}"))
}

#[allow(clippy::unwrap_used)]
fn insert_binary(
    base: Option<&[u8]>,
    pos: Option<i64>,
    len: Option<i64>,
    insert: Option<&[u8]>,
) -> Result<Vec<u8>, datafusion_common::DataFusionError> {
    let base = base.unwrap();
    let pos = pos.unwrap();
    let len = len.unwrap();
    let insert = insert.unwrap();

    let base_len: i64 = base.len().try_into().unwrap();

    if pos < 1 || pos > base_len + 1 {
        return exec_err!(
            "Valid values for position are between 1 and one more than length of binary"
        );
    }

    if len < 0 || len > (base_len - pos + 1) {
        return exec_err!(
            "Valid values for length range from 0 to the number of bytes from position to end of binary"
        );
    }

    let pos: usize = (pos - 1).try_into().unwrap();
    let len: usize = len.try_into().unwrap();

    let mut result = vec![];
    result.extend_from_slice(&base[..pos]);
    result.extend_from_slice(insert);
    result.extend_from_slice(&base[pos + len..]);

    Ok(result)
}

crate::macros::make_udf_function!(Insert);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, assert_batches_eq};
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Insert::new()));

        let q = "SELECT INSERT('abc', 1, 2, 'Z') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+-----+", "| str |", "+-----+", "| Zc  |", "+-----+",],
            &result
        );

        let q = "SELECT INSERT('abcdef', 3, 2, 'zzz') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+",
                "| str     |",
                "+---------+",
                "| abzzzef |",
                "+---------+",
            ],
            &result
        );

        let q = "SELECT INSERT('abc', 2, 1, '') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+-----+", "| str |", "+-----+", "| ac  |", "+-----+",],
            &result
        );

        let q = "SELECT INSERT('abc', 4, 0, 'Z') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+------+", "| str  |", "+------+", "| abcZ |", "+------+",],
            &result
        );

        let q = "SELECT INSERT(NULL, 1, 2, 'Z') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+-----+", "| str |", "+-----+", "|     |", "+-----+",],
            &result
        );

        let q = "SELECT INSERT('abc', NULL, 2, 'Z') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+-----+", "| str |", "+-----+", "|     |", "+-----+",],
            &result
        );

        let q = "SELECT INSERT('abc', 1, NULL, 'Z') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+-----+", "| str |", "+-----+", "|     |", "+-----+",],
            &result
        );

        let q = "SELECT INSERT('abc', 1, 2, NULL) as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &["+-----+", "| str |", "+-----+", "|     |", "+-----+",],
            &result
        );

        // Binary data test
        let q = "SELECT INSERT(X'1234567890ABCDEF', 4, 2, X'FFFF') as STR;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+------------------+",
                "| str              |",
                "+------------------+",
                "| 123456ffffabcdef |",
                "+------------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_pos_fails() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Insert::new()));

        // pos less than 1
        let q = "SELECT INSERT('abc', 0, 2, 'Z') as STR;";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => {
                    assert!(
                        matches!(e, DataFusionError::Execution(_)),
                        "Expected Execution error, got: {e}",
                    );
                }
                Ok(_) => panic!("Expected error but query succeeded"),
            }
        }

        // pos beyond one more than the length of the string
        let q = "SELECT INSERT('abc', 5, 2, 'Z') as STR;";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => {
                    assert!(
                        matches!(e, DataFusionError::Execution(_)),
                        "Expected Execution error, got: {e}",
                    );
                }
                Ok(_) => panic!("Expected error but query succeeded"),
            }
        }

        // Setting pos to one more than the length of the string
        // means that len must be 0
        let q = "SELECT INSERT('abc', 4, 1, 'Z') as STR;";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => {
                    assert!(
                        matches!(e, DataFusionError::Execution(_)),
                        "Expected Execution error, got: {e}",
                    );
                }
                Ok(_) => panic!("Expected error but query succeeded"),
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_len_fails() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Insert::new()));

        // Length is outside valid range from 0 to number of characters
        // between pos and end of the string
        let q = "SELECT INSERT('abc', 1, 50, 'Z') as STR;";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => {
                    assert!(
                        matches!(e, DataFusionError::Execution(_)),
                        "Expected Execution error, got: {e}",
                    );
                }
                Ok(_) => panic!("Expected error but query succeeded"),
            }
        }
    }

    #[tokio::test]
    async fn test_mixed_arg_types_fails() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(Insert::new()));

        let q = "SELECT INSERT(X'1234567890ABCDEF', 4, 2, 'abc') as STR;";
        let result = ctx.sql(q).await;

        if let Ok(df) = result {
            let result = df.collect().await;

            match result {
                Err(e) => {
                    assert!(
                        matches!(e, DataFusionError::ArrowError(..)),
                        "Expected Arrow error, got: {e}",
                    );
                }
                Ok(_) => panic!("Expected error but query succeeded"),
            }
        }
    }
}
