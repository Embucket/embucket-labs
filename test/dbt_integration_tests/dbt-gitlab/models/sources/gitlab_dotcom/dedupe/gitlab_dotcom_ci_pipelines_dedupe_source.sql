{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "on_schema_change": "sync_all_columns"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'ci_pipelines') }}

), partitioned AS (

    SELECT *
    FROM source

    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id                        AS id,
      created_at                AS created_at,
      updated_at                AS updated_at,
      ref                       AS ref,
      tag                       AS tag,
      yaml_errors               AS yaml_errors,
      committed_at              AS committed_at,
      project_id                AS project_id,
      status                    AS status,
      started_at                AS started_at,
      finished_at               AS finished_at,
      duration                  AS duration,
      user_id::NUMBER           AS user_id,
      lock_version              AS lock_version,
      auto_canceled_by_id       AS auto_canceled_by_id,
      pipeline_schedule_id      AS pipeline_schedule_id,
      source                    AS source,
      config_source             AS config_source,
      protected                 AS protected,
      failure_reason            AS failure_reason,
      iid                       AS iid,
      merge_request_id::NUMBER  AS merge_request_id,
      _uploaded_at              AS _uploaded_at,
      pgp_is_deleted            AS pgp_is_deleted,
      pgp_is_deleted_updated_at AS pgp_is_deleted_updated_at
    FROM partitioned

)

SELECT *
FROM renamed