use crate::json::{PathToken, tokenize_path};
use crate::table::flatten::provider::{FlattenArgs, FlattenMode, FlattenTableProvider, Out};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion_common::{Result as DFResult, ScalarValue, exec_err};
use datafusion_expr::Expr;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    pub row_id: Arc<AtomicU64>,
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

    pub fn append_null(&self, out: &Rc<RefCell<Out>>) {
        let mut o = out.borrow_mut();
        o.seq.append_value(self.row_id.load(Ordering::Acquire));
        o.key.append_null();
        o.path.append_null();
        o.index.append_null();
        o.value.append_null();
        o.this.append_null();
    }

    #[allow(
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::only_used_in_recursion
    )]
    pub fn flatten(
        &self,
        value: &Value,
        path: &[PathToken],
        outer: bool,
        recursive: bool,
        mode: &FlattenMode,
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

impl PartialEq for FlattenTableFunc {
    fn eq(&self, _other: &Self) -> bool {
        // All instances have identical semantics regardless of internal counter state
        true
    }
}

impl Eq for FlattenTableFunc {}

impl std::hash::Hash for FlattenTableFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash by type identity; internal counter is runtime state and should not affect identity
        "FlattenTableFunc".hash(state);
    }
}

impl TableFunctionImpl for FlattenTableFunc {
    fn call(&self, args: &[(Expr, Option<String>)]) -> DFResult<Arc<dyn TableProvider>> {
        let named_args_count = args.iter().filter(|(_, name)| name.is_some()).count();
        if named_args_count > 0 && named_args_count != args.len() {
            return exec_err!("flatten() supports either all named arguments or positional");
        }

        let flatten_args = if named_args_count > 0 {
            get_named_args(args)?
        } else {
            get_args(
                args.iter()
                    .map(|(expr, _)| expr)
                    .collect::<Vec<_>>()
                    .as_ref(),
            )?
        };
        Ok(Arc::new(FlattenTableProvider::new(flatten_args)?))
    }
}

#[allow(clippy::format_push_string)]
#[must_use]
pub fn path_to_string(path: &[PathToken]) -> String {
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

fn get_named_args(args: &[(Expr, Option<String>)]) -> DFResult<FlattenArgs> {
    let mut path: Vec<PathToken> = vec![];
    let mut is_outer: bool = false;
    let mut is_recursive: bool = false;
    let mut mode = FlattenMode::Both;

    // input
    let Some(input_expr) = get_arg(args, "input") else {
        return exec_err!("Missing required argument: INPUT");
    };

    // path
    if let Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) = get_arg(args, "path") {
        path = if let Some(p) = tokenize_path(&v) {
            p
        } else {
            return exec_err!("Invalid JSON path");
        }
    }

    // is_outer
    if let Some(Expr::Literal(ScalarValue::Boolean(Some(v)), _)) = get_arg(args, "is_outer") {
        is_outer = v;
    }

    // is_recursive
    if let Some(Expr::Literal(ScalarValue::Boolean(Some(v)), _)) = get_arg(args, "is_recursive") {
        is_recursive = v;
    }

    // mode
    if let Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) = get_arg(args, "mode") {
        mode = match v.to_lowercase().as_str() {
            "object" => FlattenMode::Object,
            "array" => FlattenMode::Array,
            "both" => FlattenMode::Both,
            _ => return exec_err!("MODE must be one of: object, array, both"),
        }
    }

    Ok(FlattenArgs {
        input_expr,
        path,
        is_outer,
        is_recursive,
        mode,
    })
}

fn get_args(args: &[&Expr]) -> DFResult<FlattenArgs> {
    if args.is_empty() {
        return exec_err!("flatten() expects at least 1 argument: INPUT");
    }

    // path
    let path = if let Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) = args.get(1) {
        if let Some(p) = tokenize_path(v) {
            p
        } else {
            return exec_err!("Invalid JSON path");
        }
    } else {
        vec![]
    };

    // is_outer
    let is_outer = if let Some(Expr::Literal(ScalarValue::Boolean(Some(v)), _)) = &args.get(2) {
        *v
    } else {
        false
    };

    // is_recursive
    let is_recursive = if let Some(Expr::Literal(ScalarValue::Boolean(Some(v)), _)) = &args.get(3) {
        *v
    } else {
        false
    };

    // mode
    let mode = if let Some(Expr::Literal(ScalarValue::Utf8(Some(v)), _)) = &args.get(4) {
        match v.to_lowercase().as_str() {
            "object" => FlattenMode::Object,
            "array" => FlattenMode::Array,
            _ => FlattenMode::Both,
        }
    } else {
        FlattenMode::Both
    };

    Ok(FlattenArgs {
        input_expr: args[0].clone(),
        path,
        is_outer,
        is_recursive,
        mode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semi_structured::parse_json::ParseJsonFunc;
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
                "| 1   |     | b.c[0] | 0     | 1     | [    |",
                "|     |     |        |       |       |   1, |",
                "|     |     |        |       |       |   2, |",
                "|     |     |        |       |       |   3  |",
                "|     |     |        |       |       | ]    |",
                "| 1   |     | b.c[1] | 1     | 2     | [    |",
                "|     |     |        |       |       |   1, |",
                "|     |     |        |       |       |   2, |",
                "|     |     |        |       |       |   3  |",
                "|     |     |        |       |       | ]    |",
                "| 1   |     | b.c[2] | 2     | 3     | [    |",
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
            "| 1   |     | b    |       |       |      |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = "SELECT * from flatten(NULL,'b',true,false,'both')";
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
            "| 1   |     | a    |       |       | []   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = "SELECT * from flatten('[]','',true,false,'both')";
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 1   |     |      |       |       | []   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = "SELECT * from flatten('{}','',true,false,'both')";
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 1   |     |      |       |       | {}   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        let sql = r#"SELECT * from flatten('{"a":{}}','a',true,false,'both')"#;
        let result = ctx.sql(sql).await?.collect().await?;

        let exp = [
            "+-----+-----+------+-------+-------+------+",
            "| SEQ | KEY | PATH | INDEX | VALUE | THIS |",
            "+-----+-----+------+-------+-------+------+",
            "| 1   |     | a    |       |       | {}   |",
            "+-----+-----+------+-------+-------+------+",
        ];
        assert_batches_eq!(exp, &result);

        Ok(())
    }

    #[tokio::test]
    async fn test_inner_func() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
        ctx.register_udf(ParseJsonFunc::new(false).into());
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
