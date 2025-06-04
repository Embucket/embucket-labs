{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}


WITH ping_metrics_source AS (

  SELECT *
  FROM {{ ref('usage_ping_metrics_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY metrics_path, uploaded_at::DATE ORDER BY uploaded_at DESC) = 1

),

stages_source AS (

  SELECT *
  FROM {{ source('gitlab_data_yaml', 'stages') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY uploaded_at::DATE ORDER BY uploaded_at DESC) = 1

),

group_section_stage_source AS (

  SELECT DISTINCT
    stages_source.uploaded_at::DATE     AS snapshot_date,
    TRIM(grp.value::STRING)             AS group_name,
    stage_json.value['section']::STRING AS section_name,
    stage_json.key                      AS stage_name
  FROM stages_source
  INNER JOIN LATERAL FLATTEN(INPUT => JSONTEXT['stages']) AS stage_json
  INNER JOIN LATERAL FLATTEN(INPUT => OBJECT_KEYS(stage_json.value['groups'])) AS grp
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_date, group_name ORDER BY snapshot_date DESC) = 1

),

snapshot_dates AS (

  SELECT *
  FROM {{ ref('dim_date') }}
  WHERE date_actual >= '2021-04-13' AND date_actual <= CURRENT_DATE

),

ping_metric_hist AS (

  SELECT
    metrics_path                                            AS metrics_path,
    'raw_usage_data_payload['''
      || REPLACE(metrics_path, '.', '''][''')
      || ''']'                                              AS sql_friendly_path,
    data_source                                             AS data_source,
    LOWER(data_category)                                    AS data_category,
    description                                             AS description,
    IFF(SUBSTRING(product_group, 0, 5) = 'group',
      SPLIT_PART(REPLACE(product_group, ' ', '_'), ':', 3),
      REPLACE(product_group, ' ', '_'))                     AS group_name,
    milestone                                               AS milestone,
    skip_validation                                         AS skip_validation,
    metrics_status                                          AS metrics_status,
    tier                                                    AS tier,
    time_frame                                              AS time_frame,
    value_type                                              AS value_type,
    instrumentation_class                                   AS instrumentation_class,
    performance_indicator_type                              AS performance_indicator_type,
    is_gmau                                                 AS is_gmau,
    is_smau                                                 AS is_smau,
    is_paid_gmau                                            AS is_paid_gmau,
    is_umau                                                 AS is_umau,
    uploaded_at                                             AS uploaded_at,
    snapshot_date                                           AS snapshot_date
  FROM ping_metrics_source

),

ping_metric_spined AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['metrics_path', 'snapshot_dates.date_id']) }} AS ping_metric_hist_id,
    snapshot_dates.date_id                                                             AS snapshot_id,
    ping_metric_hist.snapshot_date,
    ping_metric_hist.metrics_path,
    ping_metric_hist.sql_friendly_path,
    ping_metric_hist.data_source,
    ping_metric_hist.data_category,
    ping_metric_hist.description,
    ping_metric_hist.instrumentation_class,
    group_section_stage_source.group_name,
    group_section_stage_source.section_name,
    group_section_stage_source.stage_name,
    ping_metric_hist.milestone,
    ping_metric_hist.skip_validation,
    ping_metric_hist.metrics_status,
    ping_metric_hist.tier,
    ping_metric_hist.time_frame,
    ping_metric_hist.value_type,
    ping_metric_hist.performance_indicator_type,
    ping_metric_hist.is_gmau,
    ping_metric_hist.is_smau,
    ping_metric_hist.is_paid_gmau,
    ping_metric_hist.is_umau,
    ping_metric_hist.uploaded_at
  FROM ping_metric_hist
  INNER JOIN snapshot_dates
    ON ping_metric_hist.snapshot_date = snapshot_dates.date_actual
  LEFT JOIN group_section_stage_source
    ON ping_metric_hist.snapshot_date = group_section_stage_source.snapshot_date
      AND ping_metric_hist.group_name = group_section_stage_source.group_name

)

{{ dbt_audit(
    cte_ref="ping_metric_spined",
    created_by="@chrissharp",
    updated_by="@utkarsh060",
    created_date="2022-05-13",
    updated_date="2024-03-12"
) }}
