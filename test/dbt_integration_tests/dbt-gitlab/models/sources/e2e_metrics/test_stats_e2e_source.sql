WITH source AS (

  SELECT
    PARSE_JSON(payload) AS payload,
    uploaded_at
  FROM {{ source('e2e_metrics','all_test_stats') }}

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
    {{ dbt_utils.generate_surrogate_key(['TEST_STATS_ID', 'TAGS_TESTCASE', 'TAGS_FILE_PATH', 'NAME', 'TAGS_PRODUCT_GROUP', 'TAGS_STAGE', 'JOB_ID', 'TAGS_JOB_NAME', 'JOB_URL', 'PIPELINE_ID', 'PIPELINE_URL', 'IS_MERGE_REQUEST', 'IS_SMOKE', 'IS_QUARANTINED', 'RUN_TIME', 'TAGS_RUN_TYPE', 'TAGS_STATUS', 'UI_FABRICATION', 'API_FABRICATION', 'TOTAL_FABRICATION', 'UPLOADED_AT']) }} AS combined_composite_keys
  FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY uploaded_at DESC) = 1
