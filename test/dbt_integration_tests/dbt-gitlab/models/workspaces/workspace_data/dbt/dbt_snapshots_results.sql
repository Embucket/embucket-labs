WITH snapshots_results AS (

    SELECT *
    FROM {{ ref('dbt_snapshots_results_source') }}

), models AS (

    SELECT *
    FROM {{ ref('dbt_model_source') }}

), joined AS (

    SELECT
      snapshots_results.model_execution_time,
      snapshots_results.model_unique_id,
      snapshots_results.status                     AS run_status,
      snapshots_results.message                    AS run_message,
      snapshots_results.compilation_started_at,
      snapshots_results.compilation_completed_at,
      snapshots_results.uploaded_at,
      models.name                                  AS model_name,
      models.alias                                 AS model_alias,
      models.database_name,
      models.schema_name,
      models.package_name,
      models.tags                                  AS model_tags,
      models.references                            AS model_references
    FROM snapshots_results
    INNER JOIN models
      ON snapshots_results.model_unique_id = models.unique_id

)

SELECT *
FROM joined



