    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_schedules_dedupe_source') }}
  
), renamed AS (

    SELECT
      id::NUMBER            AS ci_pipeline_schedule_id, 
      ref                    AS ref, 
      next_run_at::TIMESTAMP AS next_run_at, 
      project_id::NUMBER    AS project_id, 
      owner_id::NUMBER      AS owner_id, 
      active                 AS active, 
      created_at::TIMESTAMP  AS created_at, 
      updated_at::TIMESTAMP  AS updated_at


    FROM source

)


SELECT *
FROM renamed
