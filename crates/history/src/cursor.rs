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

use crate::HistoryItem;
use bytes::Bytes;
use icebucket_utils::IterableEntity;

pub trait Cursor {
    fn key_from_cursor (cursor: String) -> Bytes;
    fn cursor_from_key (key: Bytes) -> String;
}

impl Cursor for HistoryItem {
    fn key_from_cursor (cursor: String) -> Bytes {
        Self::key_with_prefix(cursor)
    }

    fn cursor_from_key (key: Bytes) -> String {
        let (_, cursor) = key.split_at(Self::PREFIX.len());
        String::from_utf8(cursor.to_vec()).unwrap_or(0.to_string())
    }
}