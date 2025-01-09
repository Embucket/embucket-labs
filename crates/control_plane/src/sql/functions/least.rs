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

use crate::sql::functions::greatest_least_utils::GreatestLeastOperator;
use arrow::array::{make_comparator, Array, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::cmp;
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_CONDITIONAL;
use datafusion::logical_expr::{ColumnarValue, Documentation};
use datafusion::logical_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::OnceLock;

const SORT_OPTIONS: SortOptions = SortOptions {
    // Having the smallest result first
    descending: false,

    // NULL will be greater than any other value
    nulls_first: false,
};

#[derive(Debug)]
pub struct LeastFunc {
    signature: Signature,
}

impl Default for LeastFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LeastFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl GreatestLeastOperator for LeastFunc {
    const NAME: &'static str = "least";

    fn keep_scalar<'a>(lhs: &'a ScalarValue, rhs: &'a ScalarValue) -> Result<&'a ScalarValue> {
        // Manual checking for nulls as:
        // 1. If we're going to use <=, in Rust None is smaller than Some(T), which we don't want
        // 2. And we can't use make_comparator as it has no natural order (Arrow error)
        if lhs.is_null() {
            return Ok(rhs);
        }

        if rhs.is_null() {
            return Ok(lhs);
        }

        if !lhs.data_type().is_nested() {
            return if lhs <= rhs { Ok(lhs) } else { Ok(rhs) };
        }

        // Not using <= as in Rust None is smaller than Some(T)

        // If complex type we can't compare directly as we want null values to be larger
        let cmp = make_comparator(
            lhs.to_array()?.as_ref(),
            rhs.to_array()?.as_ref(),
            SORT_OPTIONS,
        )?;

        if cmp(0, 0).is_le() {
            Ok(lhs)
        } else {
            Ok(rhs)
        }
    }

    /// Return boolean array where `arr[i] = lhs[i] <= rhs[i]` for all i, where `arr` is the result array
    /// Nulls are always considered larger than any other value
    fn get_indexes_to_keep(lhs: &dyn Array, rhs: &dyn Array) -> Result<BooleanArray> {
        // Fast path:
        // If both arrays are not nested, have the same length and no nulls, we can use the faster vectorized kernel
        // - If both arrays are not nested: Nested types, such as lists, are not supported as the null semantics are not well-defined.
        // - both array does not have any nulls: cmp::lt_eq will return null if any of the input is null while we want to return false in that case
        if !lhs.data_type().is_nested()
            && lhs.logical_null_count() == 0
            && rhs.logical_null_count() == 0
        {
            return cmp::lt_eq(&lhs, &rhs).map_err(Into::into);
        }

        let cmp = make_comparator(lhs, rhs, SORT_OPTIONS)?;

        if lhs.len() != rhs.len() {
            return internal_err!("All arrays should have the same length for least comparison");
        }

        let values = BooleanBuffer::collect_bool(lhs.len(), |i| cmp(i, i).is_le());

        // No nulls as we only want to keep the values that are smaller, its either true or false
        Ok(BooleanArray::new(values, None))
    }
}

impl ScalarUDFImpl for LeastFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "least"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        super::greatest_least_utils::execute_conditional::<Self>(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let coerced_type = super::greatest_least_utils::find_coerced_type::<Self>(arg_types)?;

        Ok(vec![coerced_type; arg_types.len()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_smallest_doc())
    }
}
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

#[allow(clippy::unwrap_used)]
fn get_smallest_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder().with_doc_section(DOC_SECTION_CONDITIONAL)
            .with_sql_example(r"```sql
> select least(4, 7, 5);
+---------------------------+
| least(4,7,5)              |
+---------------------------+
| 4                         |
+---------------------------+
```",
            )
            .with_argument(
                "expression1, expression_n",
                "Expressions to compare and return the smallest value. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary.",
            )
            .build().unwrap()
    })
}
