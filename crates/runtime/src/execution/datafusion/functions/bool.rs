use arrow::datatypes::i256;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use half::f16;
use std::any::Any;








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
