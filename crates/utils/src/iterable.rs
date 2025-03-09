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

use bytes::{Bytes, BytesMut};

pub trait IterableCursor {
    const MAX_LEN: usize;
    const CURSOR_MIN: Self;
    const CURSOR_MAX: Self;

    fn cursor_from<T: ToString>(value: T) -> Bytes;
    fn next_cursor(&self) -> Bytes;
    fn as_bytes(&self) -> Bytes;
}

#[allow(clippy::trait_duplication_in_bounds)]
impl IterableCursor for i64 {
    const MAX_LEN: usize = 19; // holds 19 digits max
    const CURSOR_MIN: Self = 0;
    const CURSOR_MAX: Self = Self::MAX;

    fn cursor_from<T: ToString>(value: T) -> Bytes {
        Bytes::from(value.to_string())
    }

    fn next_cursor(&self) -> Bytes {
        (self + 1).as_bytes()
    }

    fn as_bytes(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

pub trait IterableEntity {
    type Cursor: IterableCursor + ToString;
    const PREFIX: &[u8];

    fn cursor(&self) -> Self::Cursor;
    fn next_cursor(&self) -> Self::Cursor;

    fn cursor_bytes(&self) -> Bytes {
        Bytes::from(self.cursor().to_string())
    }

    fn key(&self) -> Bytes {
        Self::key_from_cursor(self.cursor_bytes())
    }

    fn min_key() -> Bytes {
        Self::key_from_cursor(Self::Cursor::CURSOR_MIN.as_bytes())
    }

    fn max_key() -> Bytes {
        Self::key_from_cursor(Self::Cursor::CURSOR_MAX.as_bytes())
    }

    fn key_from_cursor(cursor: Bytes) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::PREFIX.len() + Self::Cursor::MAX_LEN);
        buf.extend_from_slice(Self::PREFIX);
        buf.extend_from_slice(cursor.as_ref());
        buf.into()
    }
}
