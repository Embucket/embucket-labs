WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_job_artifacts_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER                AS ci_job_artifact_id,
    project_id::NUMBER        AS project_id,
    job_id::NUMBER            AS ci_job_id,
    file_type                 AS file_type,
    size                      AS size,
    created_at::TIMESTAMP     AS created_at,
    updated_at::TIMESTAMP     AS updated_at,
    expire_at::TIMESTAMP      AS expire_at,
    file                      AS file,
    file_store                AS file_store,
    file_format               AS file_format,
    file_location             AS file_location,
    locked                    AS locked,
    pgp_is_deleted            AS pgp_is_deleted,
    pgp_is_deleted_updated_at AS pgp_is_deleted_updated_at
  FROM source

)


SELECT *
FROM renamed
