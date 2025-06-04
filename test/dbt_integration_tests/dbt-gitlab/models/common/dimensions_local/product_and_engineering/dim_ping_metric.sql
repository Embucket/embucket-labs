{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}


WITH latest_usage_ping_metrics_source AS (

  SELECT *
  FROM {{ ref('usage_ping_metrics_source') }}
  QUALIFY MAX(uploaded_at) OVER() = uploaded_at

),

latest_stages_source AS (

  SELECT *
  FROM {{ source('gitlab_data_yaml', 'stages') }}
  QUALIFY MAX(uploaded_at) OVER() = uploaded_at

),

group_section_stage_source AS (

  SELECT DISTINCT
    TRIM(grp.value::STRING)             AS group_name,
    stage_json.value['section']::STRING AS section_name,
    stage_json.key                      AS stage_name
  FROM latest_stages_source
  INNER JOIN LATERAL FLATTEN(INPUT => JSONTEXT['stages']) AS stage_json
  INNER JOIN LATERAL FLATTEN(INPUT => OBJECT_KEYS(stage_json.value['groups'])) AS grp

),

ping_metric AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['metrics_path']) }} AS ping_metric_id,
    metrics_path                                             AS metrics_path,
    'raw_usage_data_payload['''
      || REPLACE(metrics_path, '.', '''][''')
      || ''']'                                               AS sql_friendly_path,
    data_source                                              AS data_source,
    LOWER(data_category)                                     AS data_category,
    description                                              AS description,
    IFF(SUBSTRING(product_group, 0, 5) = 'group',
      SPLIT_PART(REPLACE(product_group, ' ', '_'), ':', 3),
      REPLACE(product_group, ' ', '_'))                      AS group_name,
    milestone                                                AS milestone,
    skip_validation                                          AS skip_validation,
    metrics_status                                           AS metrics_status,
    tier                                                     AS tier,
    time_frame                                               AS time_frame,
    value_type                                               AS value_type,
    instrumentation_class                                    AS instrumentation_class,
    performance_indicator_type                               AS performance_indicator_type,
    IFNULL(is_gmau, FALSE)                                   AS is_gmau,
    IFNULL(is_smau, FALSE)                                   AS is_smau,
    IFNULL(is_paid_gmau, FALSE)                              AS is_paid_gmau,
    IFNULL(is_umau, FALSE)                                   AS is_umau,
    IFNULL(is_health_score_metric, FALSE)                    AS is_health_score_metric,
    IFNULL(is_lighthouse_metric, FALSE)                      AS is_lighthouse_metric,
    IFNULL(is_leading_indicator_metric, FALSE)               AS is_leading_indicator_metric,
    snapshot_date                                            AS snapshot_date,
    uploaded_at                                              AS uploaded_at,
    data_by_row
  FROM latest_usage_ping_metrics_source

),

final AS (

  SELECT
    ping_metric.ping_metric_id,
    ping_metric.metrics_path,
    ping_metric.sql_friendly_path,
    ping_metric.data_source,
    ping_metric.data_category,
    ping_metric.description,
    group_section_stage_source.group_name,
    group_section_stage_source.section_name,
    group_section_stage_source.stage_name,
    ping_metric.milestone,
    ping_metric.skip_validation,
    ping_metric.metrics_status,
    ping_metric.tier,
    ping_metric.time_frame,
    ping_metric.value_type,
    ping_metric.instrumentation_class,
    ping_metric.performance_indicator_type,
    ping_metric.is_gmau,
    ping_metric.is_smau,
    ping_metric.is_paid_gmau,
    ping_metric.is_umau,
    ping_metric.is_health_score_metric,
   ping_metric.is_lighthouse_metric,
   ping_metric.is_leading_indicator_metric,
    ping_metric.snapshot_date,
    ping_metric.uploaded_at,
    ping_metric.data_by_row
  FROM ping_metric
  LEFT JOIN group_section_stage_source
    ON ping_metric.group_name = group_section_stage_source.group_name

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@utkarsh060",
    created_date="2022-04-14",
    updated_date="2024-03-12"
) }}
