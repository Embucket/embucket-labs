use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{ObjectName, ObjectNamePart};
use datafusion_expr::sqlparser::ast::{Statement, VisitorMut};
use std::ops::ControlFlow;

/// Visitor that processes `COPY INTO` statements in Snowflake SQL AST.
///
/// It normalizes table identifiers by removing unsupported prefix characters such as:
/// `@`, `~`, `/` (or any combination of them), which are commonly found in stage references:
///
/// Internal stages:
/// - `@~/<path>`
/// - `@<namespace>.%<table_name>/<path>`
/// - `@%<table_name>`
///
/// External stages:
/// - `@<namespace>.<ext_stage_name>/<path>`
///
/// Since those prefixes are not valid table names in many downstream systems,
/// this visitor ensures that `into` and `from_obj` identifiers are cleaned accordingly.
#[derive(Debug, Default)]
pub struct CopyIntoStatementIdentifiers {}

impl VisitorMut for CopyIntoStatementIdentifiers {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        if let Statement::CopyIntoSnowflake { into, from_obj, .. } = statement {
            fn sanitize_identifier(obj_name: &mut ObjectName) {
                if let Some(ObjectNamePart::Identifier(ident)) = obj_name.0.first_mut() {
                    // Remove any leading @, ~, or / characters
                    let sanitized = ident.value.trim_start_matches(['@', '~', '/']).to_string();
                    ident.value = sanitized;
                }
            }
            sanitize_identifier(into);
            if let Some(from_obj) = from_obj {
                sanitize_identifier(from_obj);
            }
        }

        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut CopyIntoStatementIdentifiers {});
}
