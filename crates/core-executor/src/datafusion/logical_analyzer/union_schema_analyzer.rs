use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::{Expr, ExprSchemable, Union};
use std::fmt::Debug;
use std::sync::Arc;

/// Analyzer rule that rewrites `UNION` plans to reconcile schema differences
/// between inputs, especially for timestamp columns.
///
/// Some expressions like `to_timestamp` may implicitly return a timestamp
/// with a different time unit (e.g., nanoseconds vs. microseconds) due to
/// overflow of very large values. If a timestamp in nanoseconds overflows,
/// Arrow will produce an Arithmetic overflow error when trying to cast back
/// from microseconds to nanoseconds. To prevent this, this rule scans the
/// UNION inputs, detects calls to `to_timestamp`, and promotes the field type
/// to `Timestamp(Microsecond, tz)` when needed.
///
/// Example scenario:
/// ```sql
/// SELECT to_timestamp('9999-12-31 00:00:00') AS ts  -- would overflow nanoseconds
/// UNION ALL
/// SELECT to_timestamp('2025-01-01 00:00:00') AS ts
/// ```
/// Without reconciliation, the first branch would produce a
/// `Timestamp(Nanosecond, None)` and cause overflow when coercing inputs.
/// By promoting the type to microseconds, we avoid runtime errors.
///
/// Detection of `to_timestamp` calls is done via `contains_to_timestamp(expr)`,
/// ensuring that any input affected by overflow will influence the resulting
/// UNION schema.
#[derive(Debug, Default)]
pub struct UnionSchemaAnalyzer;

impl UnionSchemaAnalyzer {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for UnionSchemaAnalyzer {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        analyze_internal(&plan)
    }

    fn name(&self) -> &'static str {
        "UnionSchemaAnalyzer"
    }
}

fn analyze_internal(plan: &LogicalPlan) -> DFResult<LogicalPlan> {
    let mut new_plan = plan.clone().transform_up(|node| match node {
        LogicalPlan::Union(union) => Ok(Transformed::yes(rewrite_union(&union)?)),
        _ => Ok(Transformed::no(node.clone())),
    })?;

    // Recompute schemas for Projection and SubqueryAlias nodes above the rewritten Union
    if new_plan.transformed {
        new_plan = new_plan.data.transform_up(|node| match node {
            LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
                Ok(Transformed::yes(node.recompute_schema()?))
            }
            _ => Ok(Transformed::no(node)),
        })?;
    }
    Ok(new_plan.data)
}

fn rewrite_union(union: &Union) -> datafusion_common::Result<LogicalPlan> {
    let union_fields = union.schema.fields();
    let mut new_fields: Vec<Field> = Vec::with_capacity(union_fields.len());

    for (i, base_field) in union_fields.iter().enumerate() {
        let mut chosen_type = base_field.data_type().clone();

        for input in &union.inputs {
            let (expr_for_col, ctx_schema) = resolve_expr_and_schema(input, i, base_field);
            reconcile_field_type(&mut chosen_type, &expr_for_col, &ctx_schema);
        }
        new_fields.push(Field::new(
            base_field.name(),
            chosen_type,
            base_field.is_nullable(),
        ));
    }
    let arrow_schema = Schema::new(new_fields);
    let target_schema = Arc::new(DFSchema::try_from(Arc::new(arrow_schema))?);
    Ok(LogicalPlan::Union(Union {
        inputs: union.inputs.clone(),
        schema: target_schema,
    }))
}

fn resolve_expr_and_schema(
    input: &Arc<LogicalPlan>,
    i: usize,
    base_field: &Field,
) -> (Expr, Arc<DFSchema>) {
    if let LogicalPlan::Projection(proj) = input.as_ref() {
        let proj_expr = proj
            .expr
            .get(i)
            .cloned()
            .unwrap_or_else(|| Expr::Column(Column::from_name(base_field.name())));
        (proj_expr, proj.input.schema().clone())
    } else {
        let col_expr = Expr::Column(Column::from_name(base_field.name()));
        (col_expr, input.schema().clone())
    }
}

fn reconcile_field_type(
    chosen_type: &mut DataType,
    expr_for_col: &Expr,
    ctx_schema: &Arc<DFSchema>,
) {
    if let Ok(dt) = expr_for_col.get_type(ctx_schema)
        && contains_to_timestamp(expr_for_col)
    {
        match (&*chosen_type, &dt) {
            (
                DataType::Timestamp(TimeUnit::Nanosecond, tz),
                DataType::Timestamp(TimeUnit::Microsecond, tz1),
            )
            | (
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                DataType::Timestamp(TimeUnit::Nanosecond, tz1),
            ) => {
                let tz = if tz.is_none() {
                    tz1.clone()
                } else {
                    tz.clone()
                };
                *chosen_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
            }
            _ => {}
        }
    }
}

fn contains_to_timestamp(expr: &Expr) -> bool {
    let mut found = false;
    expr.apply(|e| {
        if let Expr::ScalarFunction(sf) = e
            && sf.func.name() == "to_timestamp"
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    found
}
