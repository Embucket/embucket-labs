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

use arrow_array::builder::{StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{exec_err, DFSchema, DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::Expr;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
enum Mode {
    Object,
    Array,
    Both,
}

impl Mode {
    const fn is_object(&self) -> bool {
        matches!(self, Self::Object | Self::Both)
    }

    const fn is_array(&self) -> bool {
        matches!(self, Self::Array | Self::Both)
    }
}

#[derive(Debug, Clone)]
enum PathToken {
    Key(String),
    Index(usize),
}

struct Out {
    seq: UInt64Builder,
    key: StringBuilder,
    path: StringBuilder,
    index: UInt64Builder,
    value: StringBuilder,
    this: StringBuilder,
    last_outer: Option<Value>,
}

struct Args {
    input_str: String,
    path: Vec<PathToken>,
    is_outer: bool,
    is_recursive: bool,
    mode: Mode,
}

/// Flatten function
/// Flattens (explodes) compound values into multiple rows
///
/// Syntax: flatten(\<expr\>,\<path\>,\<outer\>,\<recursive\>,\<mode\>)
///
/// sql example:
///
///  SELECT * from flatten(\'{"a":1, "b":\[77,88\]}\',\'b\',false,false,'both')
///
///
///  +-----+-----+------+-------+-------+-------+
///  | SEQ | KEY | PATH | INDEX | VALUE | THIS  |
///  +-----+-----+------+-------+-------+-------+
///  | 1   |     | b[0] | 0     | 77    | [     |
///  |     |     |      |       |       |   77, |
///  |     |     |      |       |       |   88  |
///  |     |     |      |       |       | ]     |
///  | 1   |     | b[1] | 1     | 88    | [     |
///  |     |     |      |       |       |   77, |
///  |     |     |      |       |       |   88  |
///  |     |     |      |       |       | ]     |
///  +-----+-----+------+-------+-------+-------+
///
/// - \<input\>
///   Input expression. Must be a JSON string
///
/// - \<path\>
///   The path to the element within a JSON data structure that needs to be flattened
///
/// DEFAULT ''
///
/// - \<outer\>
///   If FALSE, any input rows that cannot be expanded, either because they cannot be accessed in the path or because they have zero fields or entries, are completely omitted from the output.
///
/// If TRUE, exactly one row is generated for zero-row expansions (with NULL in the KEY, INDEX, and VALUE columns).
///
/// DEFAULT FALSE
///
/// - \<recursive\>
///   If FALSE, only the element referenced by PATH is expanded.
///
/// If TRUE, the expansion is performed for all sub-elements recursively.
///
/// Default FALSE
///
/// - \<mode\>
///   MODE => 'OBJECT' | 'ARRAY' | 'BOTH'
///   Specifies whether only objects, arrays, or both should be flattened.
///
/// Default: BOTH

#[derive(Debug, Clone)]
pub struct FlattenTableFunc {
    row_id: Arc<AtomicU64>,
}

impl Default for FlattenTableFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FlattenTableFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            row_id: Arc::new(AtomicU64::new(0)),
        }
    }

    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    fn empty_table(
        &self,
        schema: SchemaRef,
        path: &[PathToken],
        last_outer: Option<Value>,
        null: bool,
    ) -> Arc<MemTable> {
        let batch = if null {
            let last_outer_ = last_outer.map(|v| serde_json::to_string_pretty(&v).unwrap());

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![self.row_id.load(Ordering::Acquire)]))
                        as ArrayRef,
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                    Arc::new(StringArray::from(vec![path_to_string(path)])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![None])) as ArrayRef,
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                    Arc::new(StringArray::from(vec![last_outer_])) as ArrayRef,
                ],
            )
            .unwrap()
        } else {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::new_null(0)) as ArrayRef,
                    Arc::new(StringArray::new_null(0)) as ArrayRef,
                    Arc::new(StringArray::new_null(0)) as ArrayRef,
                    Arc::new(UInt64Array::new_null(0)) as ArrayRef,
                    Arc::new(StringArray::new_null(0)) as ArrayRef,
                    Arc::new(StringArray::new_null(0)) as ArrayRef,
                ],
            )
            .unwrap()
        };

        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[allow(
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::only_used_in_recursion
    )]
    fn flatten(
        &self,
        value: &Value,
        path: &[PathToken],
        outer: bool,
        recursive: bool,
        mode: &Mode,
        out: &Rc<RefCell<Out>>,
    ) -> DFResult<()> {
        match value {
            Value::Array(v) => {
                if !mode.is_array() {
                    return Ok(());
                }
                out.borrow_mut().last_outer = Some(value.clone());
                for (i, v) in v.iter().enumerate() {
                    let mut p = path.to_owned();
                    p.push(PathToken::Index(i));
                    {
                        let mut o = out.borrow_mut();
                        o.seq.append_value(self.row_id.load(Ordering::Acquire));
                        o.key.append_null();
                        o.path.append_value(path_to_string(&p));
                        o.index.append_value(i as u64);
                        o.value
                            .append_value(serde_json::to_string_pretty(v).unwrap());
                        o.this
                            .append_value(serde_json::to_string_pretty(value).unwrap());
                    }
                    if recursive {
                        self.flatten(v, &p, outer, recursive, mode, out)?;
                    }
                }
            }
            Value::Object(v) => {
                if !mode.is_object() {
                    return Ok(());
                }
                out.borrow_mut().last_outer = Some(value.clone());
                for (k, v) in v {
                    let mut p = path.to_owned();
                    p.push(PathToken::Key(k.to_owned()));
                    {
                        let mut o = out.borrow_mut();
                        o.seq.append_value(self.row_id.load(Ordering::Acquire));
                        o.key.append_value(k);
                        o.path.append_value(path_to_string(&p));
                        o.index.append_null();
                        o.value
                            .append_value(serde_json::to_string_pretty(v).unwrap());
                        o.this
                            .append_value(serde_json::to_string_pretty(value).unwrap());
                    }
                    if recursive {
                        self.flatten(v, &p, outer, recursive, mode, out)?;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl TableFunctionImpl for FlattenTableFunc {
    fn call(&self, args: &[(Expr, Option<String>)]) -> DFResult<Arc<dyn TableProvider>> {
        let named_args_count = args.iter().filter(|(_, name)| name.is_some()).count();
        if named_args_count > 0 && named_args_count != args.len() {
            return exec_err!("flatten() supports either all named arguments or positional");
        }
        if named_args_count == 0 && args.len() != 5 {
            return exec_err!("flatten() expects 5 args: INPUT, PATH, OUTER, RECURSIVE, MODE");
        }

        let Args {
            input_str,
            path,
            is_outer,
            is_recursive,
            mode,
        } = if named_args_count > 0 {
            get_named_args(args)?
        } else {
            get_args(
                args.iter()
                    .map(|(expr, _)| expr)
                    .collect::<Vec<_>>()
                    .as_ref(),
            )?
        };

        let input: Value = serde_json::from_str(&input_str)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse array JSON: {e}")))?;

        let schema_fields = vec![
            Field::new("SEQ", DataType::UInt64, false),
            Field::new("KEY", DataType::Utf8, true),
            Field::new("PATH", DataType::Utf8, true),
            Field::new("INDEX", DataType::UInt64, true),
            Field::new("VALUE", DataType::Utf8, true),
            Field::new("THIS", DataType::Utf8, true),
        ];

        let schema = Arc::new(Schema::new(schema_fields));

        self.row_id.fetch_add(1, Ordering::Acquire);

        let input = match get_json_value(&input, &path) {
            None => return Ok(self.empty_table(schema, &[], None, is_outer)),
            Some(v) => v,
        };

        let out = Rc::new(RefCell::new(Out {
            seq: UInt64Builder::new(),
            key: StringBuilder::new(),
            path: StringBuilder::new(),
            index: UInt64Builder::new(),
            value: StringBuilder::new(),
            this: StringBuilder::new(),
            last_outer: None,
        }));

        self.flatten(input, &path, is_outer, is_recursive, &mode, &out)?;

        let mut out = out.borrow_mut();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(out.seq.finish()),
            Arc::new(out.key.finish()),
            Arc::new(out.path.finish()),
            Arc::new(out.index.finish()),
            Arc::new(out.value.finish()),
            Arc::new(out.this.finish()),
        ];

        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        Ok(if batch.num_rows() == 0 {
            self.empty_table(schema, &path, out.last_outer.clone(), is_outer)
        } else {
            Arc::new(MemTable::try_new(schema, vec![vec![batch]])?)
        })
    }
}

#[allow(clippy::format_push_string)]
fn path_to_string(path: &[PathToken]) -> String {
    let mut out = String::new();

    for (idx, token) in path.iter().enumerate() {
        match token {
            PathToken::Key(k) => {
                if idx == 0 {
                    out.push_str(k);
                } else {
                    out.push_str(&format!(".{k}"));
                }
            }
            PathToken::Index(idx) => {
                out.push_str(&format!("[{idx}]"));
            }
        }
    }

    out
}

fn get_json_value<'a>(value: &'a Value, tokens: &[PathToken]) -> Option<&'a Value> {
    let mut current = value;

    for token in tokens {
        match token {
            PathToken::Key(k) => {
                current = current.get(k)?;
            }
            PathToken::Index(i) => {
                current = current.get(i)?;
            }
        }
    }

    Some(current)
}

#[allow(clippy::while_let_on_iterator)]
fn tokenize_path(path: &str) -> Option<Vec<PathToken>> {
    let mut tokens = Vec::new();
    let mut chars = path.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            '.' => {
                chars.next(); // skip dot
            }
            '[' => {
                chars.next(); // skip [
                if let Some(&quote) = chars.peek() {
                    if quote == '"' || quote == '\'' {
                        chars.next(); // skip quote
                        let mut key = String::new();
                        while let Some(c) = chars.next() {
                            if c == quote {
                                break;
                            }
                            key.push(c);
                        }
                        tokens.push(PathToken::Key(key));
                    } else {
                        // parse index
                        let mut num = String::new();
                        while let Some(&c) = chars.peek() {
                            if c == ']' {
                                break;
                            }
                            num.push(c);
                            chars.next();
                        }
                        chars.next(); // skip ]
                        let index = num.parse::<usize>().ok()?;
                        tokens.push(PathToken::Index(index));
                    }
                }
                chars.next(); // skip ]
            }
            _ => {
                // parse unquoted key until '.' or '['
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                tokens.push(PathToken::Key(key));
            }
        }
    }

    Some(tokens)
}

fn eval_expr(expr: &Expr) -> DFResult<ScalarValue> {
    let exec_props = ExecutionProps::new();
    let phys_expr = create_physical_expr(expr, &DFSchema::empty(), &exec_props)?;
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let result = phys_expr.evaluate(&batch)?;

    match result {
        ColumnarValue::Scalar(s) => Ok(s),
        ColumnarValue::Array(arr) => ScalarValue::try_from_array(&arr, 0),
    }
}

#[allow(clippy::unwrap_used)]
fn get_arg(args: &[(Expr, Option<String>)], name: &str) -> Option<Expr> {
    args.iter().find_map(|(expr, n)| {
        if n.as_ref().unwrap().to_lowercase().as_str() == name {
            Some(expr.to_owned())
        } else {
            None
        }
    })
}

fn get_named_args(args: &[(Expr, Option<String>)]) -> DFResult<Args> {
    let mut path: Vec<PathToken> = vec![];
    let mut is_outer: bool = false;
    let mut is_recursive: bool = false;
    let mut mode = Mode::Both;

    // input
    let input_str = if let Some(v) = get_arg(args, "input") {
        if let ScalarValue::Utf8(Some(v)) = eval_expr(&v)? {
            v
        } else {
            return exec_err!("Wrong INPUT argument");
        }
    } else {
        return exec_err!("Missing INPUT argument");
    };

    // path
    if let Some(Expr::Literal(ScalarValue::Utf8(Some(v)))) = get_arg(args, "path") {
        path = if let Some(p) = tokenize_path(&v) {
            p
        } else {
            return exec_err!("Invalid JSON path");
        }
    }

    // is_outer
    if let Some(Expr::Literal(ScalarValue::Boolean(Some(v)))) = get_arg(args, "is_outer") {
        is_outer = v;
    }

    // is_recursive
    if let Some(Expr::Literal(ScalarValue::Boolean(Some(v)))) = get_arg(args, "is_recursive") {
        is_recursive = v;
    }

    // mode
    if let Some(Expr::Literal(ScalarValue::Utf8(Some(v)))) = get_arg(args, "mode") {
        mode = match v.to_lowercase().as_str() {
            "object" => Mode::Object,
            "array" => Mode::Array,
            "both" => Mode::Both,
            _ => return exec_err!("MODE must be one of: object, array, both"),
        }
    }

    Ok(Args {
        input_str,
        path,
        is_outer,
        is_recursive,
        mode,
    })
}

fn get_args(args: &[&Expr]) -> DFResult<Args> {
    // input
    let ScalarValue::Utf8(Some(input_str)) = eval_expr(args[0])? else {
        return exec_err!("INPUT must be a string");
    };

    // path
    let path = if let Expr::Literal(ScalarValue::Utf8(Some(v))) = &args[1] {
        if let Some(p) = tokenize_path(v) {
            p
        } else {
            return exec_err!("Invalid JSON path");
        }
    } else {
        vec![]
    };

    // is_outer
    let is_outer = if let Expr::Literal(ScalarValue::Boolean(Some(v))) = &args[2] {
        *v
    } else {
        false
    };

    // is_recursive
    let is_recursive = if let Expr::Literal(ScalarValue::Boolean(Some(v))) = &args[3] {
        *v
    } else {
        false
    };

    // mode
    let mode = if let Expr::Literal(ScalarValue::Utf8(Some(v))) = &args[4] {
        match v.to_lowercase().as_str() {
            "object" => Mode::Object,
            "array" => Mode::Array,
            "both" => Mode::Both,
            _ => return exec_err!("MODE must be one of: object, array, both"),
        }
    } else {
        Mode::Both
    };

    Ok(Args {
        input_str,
        path,
        is_outer,
        is_recursive,
        mode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datafusion::functions::parse_json::ParseJsonFunc;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_array() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = "SELECT * from flatten('[1,77]','',false,false,'both')";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+-------+------+",
                "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
                "+-----+-----+------+-------+-------+------+",
                "| 1   |     | [0]  | 0     | 1     | [    |",
                "|     |     |      |       |       |   1, |",
                "|     |     |      |       |       |   77 |",
                "|     |     |      |       |       | ]    |",
                "| 1   |     | [1]  | 1     | 77    | [    |",
                "|     |     |      |       |       |   1, |",
                "|     |     |      |       |       |   77 |",
                "|     |     |      |       |       | ]    |",
                "+-----+-----+------+-------+-------+------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = r#"SELECT * from flatten('{"a":1, "b":[77,88]}','',false,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+-------+-----------+",
                "| SEQ | KEY | PATH | INDEX | VALUE | THIS      |",
                "+-----+-----+------+-------+-------+-----------+",
                "| 1   | a   | a    |       | 1     | {         |",
                "|     |     |      |       |       |   \"a\": 1, |",
                "|     |     |      |       |       |   \"b\": [  |",
                "|     |     |      |       |       |     77,   |",
                "|     |     |      |       |       |     88    |",
                "|     |     |      |       |       |   ]       |",
                "|     |     |      |       |       | }         |",
                "| 1   | b   | b    |       | [     | {         |",
                "|     |     |      |       |   77, |   \"a\": 1, |",
                "|     |     |      |       |   88  |   \"b\": [  |",
                "|     |     |      |       | ]     |     77,   |",
                "|     |     |      |       |       |     88    |",
                "|     |     |      |       |       |   ]       |",
                "|     |     |      |       |       | }         |",
                "+-----+-----+------+-------+-------+-----------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_recursive() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));

        // test without recursion
        let sql = r#"SELECT * from flatten('{"a":1, "b":[77,88], "c": {"d":"X"}}','',false,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+------------+--------------+",
                "| SEQ | KEY | PATH | INDEX | VALUE      | THIS         |",
                "+-----+-----+------+-------+------------+--------------+",
                "| 1   | a   | a    |       | 1          | {            |",
                "|     |     |      |       |            |   \"a\": 1,    |",
                "|     |     |      |       |            |   \"b\": [     |",
                "|     |     |      |       |            |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 1   | b   | b    |       | [          | {            |",
                "|     |     |      |       |   77,      |   \"a\": 1,    |",
                "|     |     |      |       |   88       |   \"b\": [     |",
                "|     |     |      |       | ]          |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 1   | c   | c    |       | {          | {            |",
                "|     |     |      |       |   \"d\": \"X\" |   \"a\": 1,    |",
                "|     |     |      |       | }          |   \"b\": [     |",
                "|     |     |      |       |            |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "+-----+-----+------+-------+------------+--------------+",
            ],
            &result
        );

        // test with recursion
        let sql =
            r#"SELECT * from flatten('{"a":1, "b":[77,88], "c": {"d":"X"}}','',false,true,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+------------+--------------+",
                "| SEQ | KEY | PATH | INDEX | VALUE      | THIS         |",
                "+-----+-----+------+-------+------------+--------------+",
                "| 2   | a   | a    |       | 1          | {            |",
                "|     |     |      |       |            |   \"a\": 1,    |",
                "|     |     |      |       |            |   \"b\": [     |",
                "|     |     |      |       |            |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 2   | b   | b    |       | [          | {            |",
                "|     |     |      |       |   77,      |   \"a\": 1,    |",
                "|     |     |      |       |   88       |   \"b\": [     |",
                "|     |     |      |       | ]          |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 2   |     | b[0] | 0     | 77         | [            |",
                "|     |     |      |       |            |   77,        |",
                "|     |     |      |       |            |   88         |",
                "|     |     |      |       |            | ]            |",
                "| 2   |     | b[1] | 1     | 88         | [            |",
                "|     |     |      |       |            |   77,        |",
                "|     |     |      |       |            |   88         |",
                "|     |     |      |       |            | ]            |",
                "| 2   | c   | c    |       | {          | {            |",
                "|     |     |      |       |   \"d\": \"X\" |   \"a\": 1,    |",
                "|     |     |      |       | }          |   \"b\": [     |",
                "|     |     |      |       |            |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 2   | d   | c.d  |       | \"X\"        | {            |",
                "|     |     |      |       |            |   \"d\": \"X\"   |",
                "|     |     |      |       |            | }            |",
                "+-----+-----+------+-------+------------+--------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_path() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = r#"SELECT * from flatten('{"a":1, "b":[77,88]}','b',false,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+-------+-------+",
                "| SEQ | KEY | PATH | INDEX | VALUE | THIS  |",
                "+-----+-----+------+-------+-------+-------+",
                "| 1   |     | b[0] | 0     | 77    | [     |",
                "|     |     |      |       |       |   77, |",
                "|     |     |      |       |       |   88  |",
                "|     |     |      |       |       | ]     |",
                "| 1   |     | b[1] | 1     | 88    | [     |",
                "|     |     |      |       |       |   77, |",
                "|     |     |      |       |       |   88  |",
                "|     |     |      |       |       | ]     |",
                "+-----+-----+------+-------+-------+-------+",
            ],
            &result
        );

        let sql = r#"SELECT * from flatten('{"a":1, "b":{"c":[1,2,3]}}','b.c',false,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+--------+-------+-------+------+",
                "| SEQ | KEY | PATH   | INDEX | VALUE | THIS |",
                "+-----+-----+--------+-------+-------+------+",
                "| 2   |     | b.c[0] | 0     | 1     | [    |",
                "|     |     |        |       |       |   1, |",
                "|     |     |        |       |       |   2, |",
                "|     |     |        |       |       |   3  |",
                "|     |     |        |       |       | ]    |",
                "| 2   |     | b.c[1] | 1     | 2     | [    |",
                "|     |     |        |       |       |   1, |",
                "|     |     |        |       |       |   2, |",
                "|     |     |        |       |       |   3  |",
                "|     |     |        |       |       | ]    |",
                "| 2   |     | b.c[2] | 2     | 3     | [    |",
                "|     |     |        |       |       |   1, |",
                "|     |     |        |       |       |   2, |",
                "|     |     |        |       |       |   3  |",
                "|     |     |        |       |       | ]    |",
                "+-----+-----+--------+-------+-------+------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mode() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = r#"SELECT * from flatten('{"a":1, "b":[77,88], "c": {"d":"X"}}','',false,true,'object')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+------------+--------------+",
                "| SEQ | KEY | PATH | INDEX | VALUE      | THIS         |",
                "+-----+-----+------+-------+------------+--------------+",
                "| 1   | a   | a    |       | 1          | {            |",
                "|     |     |      |       |            |   \"a\": 1,    |",
                "|     |     |      |       |            |   \"b\": [     |",
                "|     |     |      |       |            |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 1   | b   | b    |       | [          | {            |",
                "|     |     |      |       |   77,      |   \"a\": 1,    |",
                "|     |     |      |       |   88       |   \"b\": [     |",
                "|     |     |      |       | ]          |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 1   | c   | c    |       | {          | {            |",
                "|     |     |      |       |   \"d\": \"X\" |   \"a\": 1,    |",
                "|     |     |      |       | }          |   \"b\": [     |",
                "|     |     |      |       |            |     77,      |",
                "|     |     |      |       |            |     88       |",
                "|     |     |      |       |            |   ],         |",
                "|     |     |      |       |            |   \"c\": {     |",
                "|     |     |      |       |            |     \"d\": \"X\" |",
                "|     |     |      |       |            |   }          |",
                "|     |     |      |       |            | }            |",
                "| 1   | d   | c.d  |       | \"X\"        | {            |",
                "|     |     |      |       |            |   \"d\": \"X\"   |",
                "|     |     |      |       |            | }            |",
                "+-----+-----+------+-------+------------+--------------+",
            ],
            &result
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_outer() -> DFResult<()> {
        // outer = true

        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));

        let sql = r#"SELECT * from flatten('{"a":1}','b',true,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 1   |     |      |       |       |      |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = r#"SELECT * from flatten('{"a":[]}','a',true,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 2   |     | a    |       |       | []   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = "SELECT * from flatten('[]','',true,false,'both')";
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 3   |     |      |       |       | []   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = "SELECT * from flatten('{}','',true,false,'both')";
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 4   |     |      |       |       | {}   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = r#"SELECT * from flatten('{"a":{}}','a',true,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 5   |     | a    |       |       | {}   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        Ok(())
    }

    #[tokio::test]
    async fn test_inner_func() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        ctx.register_udf(ParseJsonFunc::new().into());
        let sql = "SELECT * from flatten(parse_json('[1,77]'),'',false,false,'both')";
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+-------+------+",
                "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
                "+-----+-----+------+-------+-------+------+",
                "| 1   |     | [0]  | 0     | 1     | [    |",
                "|     |     |      |       |       |   1, |",
                "|     |     |      |       |       |   77 |",
                "|     |     |      |       |       | ]    |",
                "| 1   |     | [1]  | 1     | 77    | [    |",
                "|     |     |      |       |       |   1, |",
                "|     |     |      |       |       |   77 |",
                "|     |     |      |       |       | ]    |",
                "+-----+-----+------+-------+-------+------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_named_arguments() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = r#"SELECT * from flatten(INPUT => '{"a":1, "b":[77,88]}',PATH=>'b',IS_OUTER=>false,IS_RECURSIVE=>false,MODE=>'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            [
                "+-----+-----+------+-------+-------+-------+",
                "| SEQ | KEY | PATH | INDEX | VALUE | THIS  |",
                "+-----+-----+------+-------+-------+-------+",
                "| 1   |     | b[0] | 0     | 77    | [     |",
                "|     |     |      |       |       |   77, |",
                "|     |     |      |       |       |   88  |",
                "|     |     |      |       |       | ]     |",
                "| 1   |     | b[1] | 1     | 88    | [     |",
                "|     |     |      |       |       |   77, |",
                "|     |     |      |       |       |   88  |",
                "|     |     |      |       |       | ]     |",
                "+-----+-----+------+-------+-------+-------+",
            ],
            &result
        );

        Ok(())
    }
}
