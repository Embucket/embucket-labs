WITH source AS (

  SELECT
    PARSE_JSON(payload) AS payload,
    uploaded_at
  FROM EMBUCKET.e2e_metrics.all_test_stats

),

final AS (

  SELECT
    payload:fields.api_fabrication::NUMBER    AS api_fabrication,
    payload:fields.failure_exception::VARCHAR AS failure_exception,
    payload:fields.failure_issue::VARCHAR     AS failure_issue,
    payload:fields.id::VARCHAR                AS test_stats_id,
    payload:fields.job_id::VARCHAR            AS job_id,
    payload:fields.import_time::VARCHAR       AS import_time,
    payload:fields.job_url::VARCHAR           AS job_url,
    payload:fields.pipeline_id::VARCHAR       AS pipeline_id,
    payload:fields.pipeline_url::VARCHAR      AS pipeline_url,
    payload:fields.run_time::NUMBER           AS run_time,
    payload:fields.total_fabrication::NUMBER  AS total_fabrication,
    payload:fields.ui_fabrication::NUMBER     AS ui_fabrication,
    payload:name::VARCHAR                     AS name,
    payload:time::TIMESTAMP                   AS time,
    payload:tags.blocking::BOOLEAN            AS is_blocking,
    payload:tags.file_path::VARCHAR           AS tags_file_path,
    payload:tags.import_repo::VARCHAR         AS tags_import_repo,
    payload:tags.import_type::VARCHAR         AS tags_import_type,
    payload:tags.merge_request::BOOLEAN       AS is_merge_request,
    payload:tags.job_name::VARCHAR            AS tags_job_name,
    payload:tags.name::VARCHAR                AS tags_tags_name,
    payload:tags.product_group::VARCHAR       AS tags_product_group,
    payload:tags.quarantined::BOOLEAN         AS is_quarantined,
    payload:tags.run_type::VARCHAR            AS tags_run_type,
    payload:tags.smoke::BOOLEAN               AS is_smoke,
    payload:tags.stage::VARCHAR               AS tags_stage,
    payload:tags.status::VARCHAR              AS tags_status,
    payload:tags.testcase::VARCHAR            AS tags_testcase,
    uploaded_at                               AS uploaded_at,
    md5(cast(coalesce(cast(TEST_STATS_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_TESTCASE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_FILE_PATH as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_PRODUCT_GROUP as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_STAGE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(JOB_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_JOB_NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(JOB_URL as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PIPELINE_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PIPELINE_URL as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(IS_MERGE_REQUEST as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(IS_SMOKE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(IS_QUARANTINED as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(RUN_TIME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_RUN_TYPE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TAGS_STATUS as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(UI_FABRICATION as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(API_FABRICATION as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TOTAL_FABRICATION as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(UPLOADED_AT as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS combined_composite_keys
  FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY uploaded_at DESC) = 1