

WITH source AS (

    SELECT *
    FROM EMBUCKET.tap_postgres.gitlab_db_ci_pipelines_internal_only

), partitioned AS (

    SELECT *
    FROM source

    

    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id                       AS id,
      updated_at               AS updated_at,
      ref                      AS ref,
      project_id               AS project_id,
      _uploaded_at             AS _uploaded_at
    FROM partitioned

)

SELECT *
FROM renamed