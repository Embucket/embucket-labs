// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Time32;
use datafusion::arrow::datatypes::TimeUnit::Second;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r"
Returns the current UTC time.

The `current_time()` return value is determined at query time and will return the same time, no matter when in the query plan the function executes.
",
    syntax_example = "current_time()"
)]
#[derive(Debug)]
pub struct CurrentTimeFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for CurrentTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentTimeFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::Numeric(1)],
                Volatility::Stable,
            ),
            aliases: vec!["localtime".to_string()],
        }
    }
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for CurrentTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "current_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        //TODO: support different return type precision based on the provided number
        // this only works with `ALTER SESSION SET TIME_OUTPUT_FORMAT = 'HH24:MI:SS.FF';`
        Ok(Time32(Second))
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified current_time() function")
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    #[allow(clippy::unwrap_used)]
    fn simplify(&self, _args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        let now_ts = info.execution_props().query_execution_start_time;
        let time = now_ts.timestamp_nanos_opt().map(|ts| {
            i32::try_from((ts % 86_400_000_000_000) / 1_000_000_000)
                .ok()
                .unwrap()
        });
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Time32Second(time),
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

crate::macros::make_udf_function!(CurrentTimeFunc);

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::SimplifyContext;

    #[test]
    fn test_current_time() {
        let props = ExecutionProps::new();
        let now_ts = props.query_execution_start_time;
        let context = SimplifyContext::new(&props);
        let result = CurrentTimeFunc::new().simplify(vec![], &context);
        match result {
            Ok(ExprSimplifyResult::Simplified(Expr::Literal(ScalarValue::Time32Second(Some(
                time,
            ))))) => {
                assert_eq!(
                    time,
                    i32::try_from(
                        (now_ts.timestamp_nanos_opt().unwrap() % 86_400_000_000_000)
                            / 1_000_000_000
                    )
                    .ok()
                    .unwrap()
                );
            }
            _ => panic!("unexpected result"),
        }
    }
}
