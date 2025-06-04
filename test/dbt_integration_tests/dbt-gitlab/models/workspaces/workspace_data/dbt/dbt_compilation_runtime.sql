WITH dbt_run_results AS (

    SELECT *
    FROM {{ ref('dbt_run_results_source') }}

), dbt_model AS (

	SELECT * 
	FROM {{ ref('dbt_model_source') }}

), current_stats AS (

    SELECT 
      model_unique_id, 
      TIMESTAMPDIFF('ms', compilation_started_at, compilation_completed_at) / 1000 AS compilation_time_seconds_elapsed
    FROM dbt_run_results
    WHERE compilation_started_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY model_unique_id ORDER BY compilation_started_at DESC) = 1
    ORDER BY 2 desc

), current_models AS (

    SELECT 
      unique_id,
      name       AS model_name
    FROM dbt_model
    WHERE generated_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) = 1


), joined AS (
    
    SELECT *
    FROM current_stats
    INNER JOIN current_models
    ON current_stats.model_unique_id = current_models.unique_id


)

SELECT * 
FROM joined
