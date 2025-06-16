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
// - Replace `sources()` requires unstable `error_iter` feature by custom code
// - No unwrap
// --------------------------------------------------------------------------------

use std::sync::Arc;
use std::any::Any;
use crate::sources::Source;

/// Extension to [`Error`](std::error::Error) in std.
pub trait ErrorExt: StackError {
    /// Returns the error as [Any](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the error message
    fn output_msg(&self) -> String
    where
        Self: Sized,
    {
        let error = self.last();
        if let Some(external_error) = error.source() {
            let sources = Source { current: Some(external_error) };
            if let Some(external_root) = sources.last() {
                if error.transparent() {
                    format!("{external_root}")
                } else {
                    format!("{error}: {external_root}")
                }
            } else {
                format!("{error}")
            }
        } else {
            format!("{error}")
        }   
    }

    /// Find out root level error for nested error
    fn root_cause(&self) -> Option<&dyn std::error::Error>
    where
        Self: Sized,
    {
        let error = self.last();
        if let Some(external_error) = error.source() {
            let sources = Source { current: Some(external_error) };
            if let Some(external_root) = sources.last() {
                return Some(external_root);
            }
        }
        None
    }
}

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
