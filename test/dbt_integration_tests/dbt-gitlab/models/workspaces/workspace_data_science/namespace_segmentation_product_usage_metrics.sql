{{ config(
     materialized = "table",
     tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('fct_event', 'fct_event'),
    ('dim_project', 'dim_project'),
    ('map_gitlab_dotcom_xmau_metrics', 'map_gitlab_dotcom_xmau_metrics'),
    ])

}}

, days_of_usage AS (
  
    SELECT
    
      dim_namespace_id AS namespace_id,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'create' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))    AS days_usage_in_stage_create_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'protect' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))   AS days_usage_in_stage_protect_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'package' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))   AS days_usage_in_stage_package_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'plan' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))      AS days_usage_in_stage_plan_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'secure' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))    AS days_usage_in_stage_secure_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'verify' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))    AS days_usage_in_stage_verify_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'configure' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END)) AS days_usage_in_stage_configure_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN map_gitlab_dotcom_xmau_metrics.stage_name = 'release' THEN fct_event.days_since_namespace_creation_at_event_date ELSE NULL END))   AS days_usage_in_stage_release_all_time_cnt
    FROM fct_event
    INNER JOIN dim_project
      ON dim_project.dim_project_id = fct_event.dim_project_id
    INNER JOIN map_gitlab_dotcom_xmau_metrics
      ON fct_event.event_name = map_gitlab_dotcom_xmau_metrics.common_events_to_include
    WHERE map_gitlab_dotcom_xmau_metrics.stage_name IN ('create', 'protect', 'package', 'plan', 'secure', 'verify', 'configure', 'release')
      AND dim_project.is_learn_gitlab != TRUE 
    GROUP BY 1

)

SELECT *
FROM days_of_usage
