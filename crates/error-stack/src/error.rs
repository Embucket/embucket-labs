// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// --------------------------------------------------------------------------------
// Modifications by Embucket Team, 2025
// - Remove status_code related code from ErrorExt
// - Replace original ErrorExt implementation by simple blanket implementation
// --------------------------------------------------------------------------------

use std::sync::Arc;

pub trait ErrorExt: StackError {
    fn output_msg(&self) -> String {
        format!("{self}\n{self:?}")
    }
}

impl<T> ErrorExt for T where T: StackError {}

pub trait StackError: std::error::Error {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>);

    fn next(&self) -> Option<&dyn StackError>;

    fn last(&self) -> &dyn StackError
    where
        Self: Sized,
    {
        let Some(mut result) = self.next() else {
            return self;
        };
        while let Some(err) = result.next() {
            result = err;
        }
        result
    }

    /// Indicates whether this error is "transparent", that it delegates its "display" and "source"
    /// to the underlying error. Could be useful when you are just wrapping some external error,
    /// **AND** can not or would not provide meaningful contextual info. For example, the
    /// `DataFusionError`.
    fn transparent(&self) -> bool {
        false
    }
}

impl<T: ?Sized + StackError> StackError for Arc<T> {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        self.as_ref().debug_fmt(layer, buf);
    }

    fn next(&self) -> Option<&dyn StackError> {
        self.as_ref().next()
    }
}

impl<T: StackError> StackError for Box<T> {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        self.as_ref().debug_fmt(layer, buf);
    }

    fn next(&self) -> Option<&dyn StackError> {
        self.as_ref().next()
    }
}
