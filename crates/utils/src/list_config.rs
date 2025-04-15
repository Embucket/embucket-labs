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

pub struct ListConfig {
    //From where to start the scan range for SlateDB
    // ex: if we ended on "tested2", the cursor would be "tested2"
    // and inside the `fn list_objects` in utils crate the start of the range would be "tested2\x00"
    // ("\x00" is the smallest ASCII char to find anything after the "tested2" excluding it)
    // and the whole range would be `tested2\x00..\x7F
    // (`\x7F` is the largest ASCII char to find anything before it)
    // if there are 4 tables `tested1..tested4` which would yield us "tested3" and "tested4" including other names if any exist
    pub cursor: Option<String>,
    //Limit of how much items to take back from `fn list_objects` in utils crate
    pub limit: Option<usize>,
    //Search string, from where (and to where in lexicographical sort order) to do the search
    // ex: if we want to find all the test tables it could be "tes" (if there are 4 tables `tested1..tested4`)
    // the range would be `tes..tes\x7F` tables
    // (`\x7F` is the largest ASCII char to find anything before it)
    // if we however had the cursor from cursor comment (line 21)
    // we could also go from `tested2\x00..tes\x7F` which would yield us "tested3" and "tested4" only excluding other names if any exist
    pub token: Option<String>,
}

impl ListConfig {
    #[must_use]
    pub const fn new(cursor: Option<String>, limit: Option<usize>, token: Option<String>) -> Self {
        Self {
            cursor,
            limit,
            token,
        }
    }
}

impl Default for ListConfig {
    fn default() -> Self {
        Self::new(None, None, None)
    }
}
