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

// use crate::datafusion::functions::register_udfs;
// use datafusion::prelude::{SessionConfig, SessionContext};

// static TABLE_SETUP: &str = include_str!(r"./queries/table_setup.sql");

// #[allow(clippy::unwrap_used)]
// pub async fn create_df_session() -> SessionContext {
//     let mut config = SessionConfig::new();
//     config.options_mut().catalog.information_schema = true;
//     let mut ctx = SessionContext::new_with_config(config);

//     register_udfs(&mut ctx).unwrap();

//     for query in TABLE_SETUP.split(';') {
//         if !query.is_empty() {
//             dbg!("Running query: ", query);
//             ctx.sql(query).await.unwrap().collect().await.unwrap();
//         }
//     }
//     ctx
// }

// pub mod macros {
//     macro_rules! test_query {
//         ($test_fn_name:ident, $query:expr) => {
//             paste::paste! {
//                 #[tokio::test]
//                 async fn [< query_ $test_fn_name >]() {
//                     let ctx = crate::tests::utils::create_df_session().await;
//                     let statement = ctx.state().sql_to_statement($query, "snowflake");

//                     let plan = ctx.state().create_logical_plan($query)
//                         .await;

//                     let df = match &plan {
//                         Ok(plan) => {
//                             match ctx.execute_logical_plan(plan.clone()).await {
//                                 Ok(df) => {
//                                     let record_batches = df.collect().await.unwrap();
//                                     Ok(datafusion::arrow::util::pretty::pretty_format_batches(&record_batches).unwrap().to_string())
//                                 },
//                                 Err(e) => Err(e)
//                             }
//                         },
//                         _ => Err(datafusion::error::DataFusionError::Execution("Failed to create logical plan".to_string()))
//                     };
//                     insta::with_settings!({
//                         description => stringify!($query),
//                         omit_expression => true,
//                         prepend_module_to_snapshot => false
//                     }, {
//                         let plan = plan.map(|plan| plan.to_string().split("\n").map(|s| s.to_string()).collect::<Vec<String>>());
//                         let df = df.map(|df| df.split("\n").map(|s| s.to_string()).collect::<Vec<String>>());
//                         insta::assert_debug_snapshot!((statement, plan, df));
//                     })
//                 }
//             }
//         }
//     }

//     pub(crate) use test_query;
// }
