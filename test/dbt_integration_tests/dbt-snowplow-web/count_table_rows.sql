-- SQL script to count rows for each table mentioned in dbt_output.log
-- Database: dbt_snowplow_web
-- Generated from dbt_output.log analysis

-- Count rows for tables in PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST schema (seeds)
SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_ga4_source_categories' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_ga4_source_categories
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_geo_country_mapping' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_geo_country_mapping
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_rfc_5646_language_mapping' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_rfc_5646_language_mapping
UNION ALL

-- Count rows for tables in PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST schema (incremental models)
SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_quarantined_sessions' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_quarantined_sessions
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_incremental_manifest' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_incremental_manifest
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_sessions_lifecycle_manifest' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_sessions_lifecycle_manifest
UNION ALL

-- Count rows for tables in PUBLIC_SNOWPLOW_MANIFEST_SCRATCH schema (temporary tables)
SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_new_event_limits' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_new_event_limits
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_sessions_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_sessions_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_events_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_events_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_consent_events_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_consent_events_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_engaged_time' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_engaged_time
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_scroll_depth' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_scroll_depth
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_sessions_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_sessions_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vital_events_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vital_events_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_page_views_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_page_views_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vitals_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vitals_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_sessions_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_sessions_this_run
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_aggs' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_aggs
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_lasts' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_lasts
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_this_run' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_this_run
UNION ALL

-- Count rows for tables in PUBLIC_SNOWPLOW_MANIFEST_DERIVED schema (main models)
SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_user_mapping' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_user_mapping
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_log' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_log
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_sessions' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_sessions
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_page_views' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_page_views
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_cmp_stats' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_cmp_stats
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_versions' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_versions
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vitals' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vitals
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_users' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_users
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vital_measurements' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vital_measurements
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_scope_status' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_scope_status
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_totals' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_totals
UNION ALL

SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_users' AS table_name, COUNT(*) AS row_count 
FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_users
ORDER BY table_name;