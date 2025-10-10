//use super::datafusion::functions::geospatial::register_udfs as register_geo_udfs;
use super::datafusion::functions::register_udfs;
use super::datafusion::type_planner::CustomTypePlanner;
use super::dedicated_executor::DedicatedExecutor;
use super::error::{self as ex_error, Result};
// TODO: We need to fix this after geodatafusion is updated to datafusion 47
//use geodatafusion::udf::native::register_native as register_geo_native;
use crate::datafusion::logical_analyzer::analyzer_rules;
use crate::datafusion::logical_optimizer::split_ordered_aggregates::SplitOrderedAggregates;
use crate::datafusion::physical_optimizer::physical_optimizer_rules;
use crate::datafusion::query_planner::CustomQueryPlanner;
use crate::models::QueryContext;
use crate::query::UserQuery;
use crate::running_queries::RunningQueries;
use crate::utils::Config;
use core_history::history_store::HistoryStore;
use core_metastore::Metastore;
use datafusion::config::ConfigOptions;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SessionStateBuilder, SessionStateDefaults};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion_functions_json::register_all as register_json_udfs;
use df_catalog::catalog_list::{DEFAULT_CATALOG, EmbucketCatalogList};
use embucket_functions::expr_planner::CustomExprPlanner;
use embucket_functions::register_udafs;
use embucket_functions::session_params::{SessionParams, SessionProperty};
use embucket_functions::table::register_udtfs;
use snafu::ResultExt;
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::thread::available_parallelism;
use time::{Duration, OffsetDateTime};

pub const SESSION_INACTIVITY_EXPIRATION_SECONDS: i64 = 5 * 60;
static MINIMUM_PARALLEL_OUTPUT_FILES: usize = 1;
static PARALLEL_ROW_GROUP_RATIO: usize = 4;

#[must_use]
pub const fn to_unix(t: OffsetDateTime) -> i64 {
    // unix_timestamp is enough, no need to use nanoseconds precision
    t.unix_timestamp()
}

pub struct UserSession {
    pub metastore: Arc<dyn Metastore>,
    pub history_store: Arc<dyn HistoryStore>,
    // running_queries contains all the queries running across sessions
    pub running_queries: Arc<dyn RunningQueries>,
    pub ctx: SessionContext,
    pub ident_normalizer: IdentNormalizer,
    pub executor: DedicatedExecutor,
    pub config: Arc<Config>,
    pub expiry: AtomicI64,
    pub session_params: Arc<SessionParams>,
}

impl UserSession {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        history_store: Arc<dyn HistoryStore>,
        running_queries: Arc<dyn RunningQueries>,
        config: Arc<Config>,
        catalog_list: Arc<EmbucketCatalogList>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        let sql_parser_dialect = config
            .sql_parser_dialect
            .clone()
            .unwrap_or_else(|| "snowflake".to_string());

        let mut expr_planners = SessionStateDefaults::default_expr_planners();
        // That's a hack to use our custom expr planner first and default ones later. We probably need to get rid of default planners at some point.
        expr_planners.insert(0, Arc::new(CustomExprPlanner));

        let parallelism_opt = available_parallelism().ok().map(NonZero::get);

        let session_params = SessionParams::default();
        let session_params_arc = Arc::new(session_params.clone());
        let mut config_options = ConfigOptions::from_env().context(ex_error::DataFusionSnafu)?;

        // Only set minimum_parallel_output_files if environment variable wasn't set
        if std::env::var_os("DATAFUSION_EXECUTION_MINIMUM_PARALLEL_OUTPUT_FILES").is_none() {
            config_options.execution.minimum_parallel_output_files = MINIMUM_PARALLEL_OUTPUT_FILES;
        }

        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::from(config_options)
                    .with_option_extension(session_params)
                    .with_information_schema(true)
                    // Cannot create catalog (database) automatic since it requires default volume
                    .with_create_default_catalog_and_schema(false)
                    .with_batch_size(16384)
                    .with_coalesce_batches(true)
                    .with_round_robin_repartition(true)
                    .with_target_partitions(16)
                    .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect)
                    .set_str("datafusion.catalog.default_catalog", DEFAULT_CATALOG)
                    .set_bool(
                        "datafusion.execution.skip_physical_aggregate_schema_check",
                        true,
                    )
                    .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
                    // Enable table statistics collection and parquet page-level stats
                    .set_bool("datafusion.execution.collect_statistics", true)
                    .set_str("datafusion.execution.parquet.statistics_enabled", "page")
                    // Make EXPLAIN output as detailed as possible by default
                    .set_bool("datafusion.explain.show_statistics", true)
                    .set_bool("datafusion.explain.show_sizes", true)
                    .set_bool("datafusion.explain.show_schema", true)
                    .set_usize("datafusion.explain.tree_maximum_render_width", 0)
                    .set_usize("datafusion.optimizer.hash_join_single_partition_threshold_rows", 10_000_000)
                    .set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 134217728)
                    .set_bool("datafusion.optimizer.enable_spillable_hash_join", true)
                    .set_usize(
                        "datafusion.execution.minimum_parallel_output_files",
                        MINIMUM_PARALLEL_OUTPUT_FILES,
                    )
                    .set_usize(
                        "datafusion.execution.parquet.maximum_parallel_row_group_writers",
                        parallelism_opt.map_or(1, |x| (x / PARALLEL_ROW_GROUP_RATIO).max(1)),
                    )
                    .set_usize(
                        "datafusion.execution.parquet.maximum_buffered_record_batches_per_stream",
                        parallelism_opt.map_or(1, |x| 1 + (x / PARALLEL_ROW_GROUP_RATIO).max(1)),
                    ),
            )
            .with_default_features()
            .with_runtime_env(runtime_env)
            .with_catalog_list(catalog_list)
            .with_query_planner(Arc::new(CustomQueryPlanner::default()))
            .with_type_planner(Arc::new(CustomTypePlanner::default()))
            .with_analyzer_rules(analyzer_rules(session_params_arc.clone()))
            .with_optimizer_rule(Arc::new(SplitOrderedAggregates::new()))
            .with_physical_optimizer_rules(physical_optimizer_rules())
            .with_expr_planners(expr_planners)
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        register_udfs(&mut ctx, &session_params_arc).context(ex_error::RegisterUDFSnafu)?;
        register_udafs(&mut ctx).context(ex_error::RegisterUDAFSnafu)?;
        register_udtfs(&ctx, history_store.clone());
        register_json_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        //register_geo_native(&ctx);
        //register_geo_udfs(&ctx);

        let enable_ident_normalization = ctx.enable_ident_normalization();
        let session = Self {
            metastore,
            history_store,
            running_queries,
            ctx,
            ident_normalizer: IdentNormalizer::new(enable_ident_normalization),
            executor: DedicatedExecutor::builder().build(),
            config,
            expiry: AtomicI64::new(to_unix(
                OffsetDateTime::now_utc()
                    + Duration::seconds(SESSION_INACTIVITY_EXPIRATION_SECONDS),
            )),
            session_params: session_params_arc,
        };
        Ok(session)
    }

    pub fn query<S>(self: &Arc<Self>, query: S, query_context: QueryContext) -> UserQuery
    where
        S: Into<String>,
    {
        UserQuery::new(self.clone(), query.into(), query_context)
    }

    pub fn set_session_variable(
        &self,
        set: bool,
        params: HashMap<String, SessionProperty>,
    ) -> Result<()> {
        let state = self.ctx.state_ref();
        let mut write = state.write();

        let mut datafusion_params = Vec::new();
        let mut session_params = HashMap::new();

        for (key, prop) in params {
            if key.to_lowercase().starts_with("datafusion.") {
                datafusion_params.push((key.to_ascii_lowercase(), prop.value));
            } else {
                session_params.insert(key.to_ascii_lowercase(), prop);
            }
        }
        let options = write.config_mut().options_mut();
        for (key, value) in datafusion_params {
            options
                .set(&key, &value)
                .context(ex_error::DataFusionSnafu)?;
        }

        let config = options.extensions.get_mut::<SessionParams>();
        if let Some(cfg) = config {
            if set {
                cfg.set_properties(session_params)
                    .context(ex_error::DataFusionSnafu)?;
            } else {
                cfg.remove_properties(session_params)
                    .context(ex_error::DataFusionSnafu)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn get_session_variable(&self, variable: &str) -> Option<String> {
        let state = self.ctx.state();
        let config = state.config().options().extensions.get::<SessionParams>();
        if let Some(cfg) = config {
            return cfg.properties.get(variable).map(|v| v.value.clone());
        }
        None
    }
}
