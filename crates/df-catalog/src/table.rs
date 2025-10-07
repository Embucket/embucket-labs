use crate::df_error;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::{ViewTable, provider_as_source};
use datafusion::execution::SessionState;
use datafusion_common::Statistics;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableScan, TableType};
use datafusion_physical_plan::ExecutionPlan;
use once_cell::sync::OnceCell;
use snafu::OptionExt;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CachingTable {
    pub schema: OnceCell<SchemaRef>,
    pub name: String,
    pub table: Arc<dyn TableProvider>,
}

impl CachingTable {
    pub fn new(name: String, table: Arc<dyn TableProvider>) -> Self {
        Self {
            schema: OnceCell::new(),
            name,
            table,
        }
    }
    pub fn new_with_schema(name: String, schema: SchemaRef, table: Arc<dyn TableProvider>) -> Self {
        Self {
            schema: OnceCell::from(schema),
            name,
            table,
        }
    }
}

impl std::fmt::Debug for CachingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("schema", &"")
            .field("name", &self.name)
            .field("table", &"")
            .finish()
    }
}

#[async_trait]
impl TableProvider for CachingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.get_or_init(|| self.table.schema()).clone()
    }

    fn table_type(&self) -> TableType {
        self.table.table_type()
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // If this table is a View, we need to ensure it reflects the latest state of its underlying tables.
        // Note: ViewTable contains a logical plan that references the snapshot of the source tables
        // at the time the view was created. Without updating TableScan nodes, the view would continue
        // to reference stale data. Here we reconstruct the logical plan with updated TableScan nodes
        // so that any query on the view sees the latest snapshots of the source tables.
        if self.table.table_type() == TableType::View
            && let Some(view) = self.table.as_any().downcast_ref::<ViewTable>()
        {
            let new_view_plan = rewrite_view_source(state, view.logical_plan().clone()).await?;
            let updated_view = ViewTable::new(new_view_plan, view.definition().cloned());
            return updated_view.scan(state, projection, filters, limit).await;
        }
        self.table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        self.table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.table.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.table.insert_into(state, input, insert_op).await
    }
}

/// Rewrites all `TableScan` nodes in a logical plan to point to the latest state of their source tables.
///
/// This is necessary because a `ViewTable` stores a logical plan referencing the snapshot of its
/// underlying tables at creation time. Without this rewriting step, queries against the view
/// would return stale data. The function:
/// 1. Collects all `TableScan` nodes in the plan.
///    - We do this separately because `transform_up` / `transform_down` cannot be async,
///      so we need to gather the nodes first before performing any async resolution.
/// 2. Asynchronously resolves each table to its current `TableProvider`.
/// 3. Replaces `TableScan` nodes in the logical plan with updated ones pointing to the latest data.
async fn rewrite_view_source(
    state: &dyn Session,
    plan: LogicalPlan,
) -> datafusion_common::Result<LogicalPlan> {
    let state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .context(df_error::SessionDowncastSnafu)?;

    // Collect all table scans in the plan
    let mut scans = vec![];
    plan.clone().transform_up(|plan| {
        if let LogicalPlan::TableScan(ref scan) = plan {
            scans.push(scan.clone());
        }
        Ok(Transformed::no(plan))
    })?;

    // Resolve each table scan to its actual table provider with async calls
    let mut replacements: HashMap<String, TableScan> = HashMap::new();
    for scan in scans {
        let resolved = state.resolve_table_ref(scan.table_name.clone());
        let table = state
            .catalog_list()
            .catalog(&resolved.catalog)
            .context(df_error::CatalogNotFoundSnafu {
                name: resolved.catalog.to_string(),
            })?
            .schema(&resolved.schema)
            .context(df_error::CannotResolveViewReferenceSnafu {
                reference: resolved.to_string(),
            })?
            .table(&resolved.table)
            .await?
            .context(df_error::CannotResolveViewReferenceSnafu {
                reference: resolved.to_string(),
            })?;
        replacements.insert(
            scan.table_name.to_string(),
            TableScan {
                source: provider_as_source(table),
                ..scan
            },
        );
    }

    // Rewrite the plan with the updated table scans
    let new_plan = plan
        .clone()
        .transform_up(|ref plan| {
            if let LogicalPlan::TableScan(scan) = plan
                && let Some(new_scan) = replacements.get(&scan.table_name.to_string())
            {
                Ok(Transformed::yes(LogicalPlan::TableScan(new_scan.clone())))
            } else {
                Ok(Transformed::no(plan.clone()))
            }
        })?
        .data;
    Ok(new_plan)
}
