use arrow_array::builder::{ArrayBuilder, StringBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion_common::{plan_err, Result as DFResult, ScalarValue};
use datafusion_expr::Expr;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use utoipa::Path;

#[derive(Debug)]
enum Mode {
    Object,
    Array,
    Both,
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
}

#[derive(Debug, Clone)]
struct FlattenTableFunc;

impl FlattenTableFunc {
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for FlattenTableFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let mut input_str = String::new();
        let mut path = String::new();
        let mut outer = false;
        let mut recursive = false;
        let mut mode = Mode::Both;

        if args.len() != 5 {
            return plan_err!("flatten() expects 5 args: INPUT, PATH, OUTER, RECURSIVE, MODE");
        }
        if let Expr::Literal(ScalarValue::Utf8(v)) = args[0].clone() {
            if let Some(v) = v {
                input_str = v;
            }
        }

        if let Expr::Literal(ScalarValue::Utf8(v)) = args[1].clone() {
            if let Some(v) = v {
                path = v;
            }
        }

        if let Expr::Literal(ScalarValue::Boolean(v)) = args[2].clone() {
            if let Some(v) = v {
                outer = v;
            }
        }

        if let Expr::Literal(ScalarValue::Boolean(v)) = args[3].clone() {
            if let Some(v) = v {
                recursive = v;
            }
        }

        if let Expr::Literal(ScalarValue::Utf8(v)) = args[4].clone() {
            if let Some(v) = v {
                mode = match v.to_lowercase().as_str() {
                    "object" => Mode::Object,
                    "array" => Mode::Array,
                    "both" => Mode::Both,
                    _ => return plan_err!("MODE must be one of: object, array, both"),
                }
            }
        }

        let input: Value = serde_json::from_str(&input_str).map_err(|e| {
            datafusion_common::error::DataFusionError::Plan(format!(
                "Failed to parse array JSON: {}",
                e
            ))
        })?;

        let input = if path.is_empty() {
            &input
        } else {
            if let Some(i) = get_json_value(&input, &path) {
                i
            } else {
                return plan_err!("Failed to get JSON path");
            }
        };

        let out = Rc::new(RefCell::new(Out {
            seq: UInt64Builder::new(),
            key: StringBuilder::new(),
            path: StringBuilder::new(),
            index: UInt64Builder::new(),
            value: StringBuilder::new(),
            this: StringBuilder::new(),
        }));
        flatten(input, vec![], outer, recursive, &mode, Rc::clone(&out))?;

        let mut schema_fields = vec![];
        schema_fields.push(Field::new("SEQ", DataType::UInt64, false));
        schema_fields.push(Field::new("KEY", DataType::Utf8, true));
        schema_fields.push(Field::new("PATH", DataType::Utf8, false));
        schema_fields.push(Field::new("INDEX", DataType::UInt64, true));
        schema_fields.push(Field::new("VALUE", DataType::Utf8, false));
        schema_fields.push(Field::new("THIS", DataType::Utf8, false));

        let mut out = out.borrow_mut();
        let mut cols: Vec<ArrayRef> = vec![];
        cols.push(Arc::new(out.seq.finish()));
        cols.push(Arc::new(out.key.finish()));
        cols.push(Arc::new(out.path.finish()));
        cols.push(Arc::new(out.index.finish()));
        cols.push(Arc::new(out.value.finish()));
        cols.push(Arc::new(out.this.finish()));
        let schema = Arc::new(Schema::new(schema_fields));
        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;

        Ok(Arc::new(table))
    }
}

fn flatten(
    value: &Value,
    path: Vec<PathToken>,
    outer: bool,
    recursive: bool,
    mode: &Mode,
    out: Rc<RefCell<Out>>,
) -> DFResult<()> {
    match value {
        Value::Array(v) => {
            for (i, v) in v.iter().enumerate() {
                let mut p = path.clone();
                p.push(PathToken::Index(i));
                {
                    let mut o = out.borrow_mut();
                    o.seq.append_value(1);
                    o.key.append_null();
                    o.path.append_value(path_to_string(&p));
                    o.index.append_value(i as u64);
                    o.value
                        .append_value(serde_json::to_string_pretty(v).unwrap());
                    o.this
                        .append_value(serde_json::to_string_pretty(value).unwrap());
                }
                if recursive {
                    flatten(v, p.clone(), outer, recursive, mode, Rc::clone(&out))?;
                }
            }
        }
        Value::Object(v) => {
            for (k, v) in v.iter() {
                let mut p = path.clone();
                p.push(PathToken::Key(k.to_owned()));
                {
                    let mut o = out.borrow_mut();
                    o.seq.append_value(1);
                    o.key.append_value(k.to_owned());
                    o.path.append_value(path_to_string(&p));
                    o.index.append_null();
                    o.value
                        .append_value(serde_json::to_string_pretty(v).unwrap());
                    o.this
                        .append_value(serde_json::to_string_pretty(value).unwrap());
                }
                if recursive {
                    flatten(v, p.clone(), outer, recursive, mode, Rc::clone(&out))?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

fn path_to_string(path: &[PathToken]) -> String {
    let mut out = String::new();

    for (idx, token) in path.iter().enumerate() {
        match token {
            PathToken::Key(k) => {
                if idx == 0 {
                    out.push_str(k);
                } else {
                    // out.push_str(&format!(r#"["{k}"]"#))
                    out.push_str(&format!(".{k}"))
                }
            }
            PathToken::Index(idx) => {
                out.push_str(&format!("[{idx}]"));
            }
        }
    }

    out
}

fn get_json_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let tokens = tokenize_path(path)?;
    let mut current = value;

    for token in tokens {
        match token {
            PathToken::Key(k) => {
                current = current.get(&k)?;
            }
            PathToken::Index(i) => {
                current = current.get(i)?;
            }
        }
    }

    Some(current)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::datafusion::functions::table::flatten::FlattenTableFunc;
    use arrow::util::pretty::print_batches;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use std::sync::Arc;

    // fixme
    #[tokio::test]
    async fn test_invalid_json() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = r#"SELECT * from flatten('[1,,77]','',false,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;
        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_array() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        let sql = r#"SELECT * from flatten('[1,77]','',false,false,'both')"#;
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
                "| 1   |     | b[0] | 0     | 77         | [            |",
                "|     |     |      |       |            |   77,        |",
                "|     |     |      |       |            |   88         |",
                "|     |     |      |       |            | ]            |",
                "| 1   |     | b[1] | 1     | 88         | [            |",
                "|     |     |      |       |            |   77,        |",
                "|     |     |      |       |            |   88         |",
                "|     |     |      |       |            | ]            |",
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
}
