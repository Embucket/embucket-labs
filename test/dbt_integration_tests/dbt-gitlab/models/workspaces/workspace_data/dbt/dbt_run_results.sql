WITH run_results AS (

    SELECT *
    FROM {{ ref('dbt_run_results_source') }}

), models AS (

    SELECT *
    FROM {{ ref('dbt_model_source') }}

), joined AS (

    SELECT
      run_results.model_execution_time,
      run_results.model_unique_id,
      run_results.status                     AS run_status,
      run_results.message                    AS run_message,
      run_results.compilation_started_at,
      run_results.compilation_completed_at,
      run_results.uploaded_at,
      models.name                            AS model_name,
      models.alias                           AS model_alias,
      models.database_name,
      models.schema_name,
      models.package_name,
      models.tags                            AS model_tags,
      models.references                      AS model_references
    FROM run_results
    INNER JOIN models
      ON run_results.model_unique_id = models.unique_id

)

SELECT *
FROM joined
