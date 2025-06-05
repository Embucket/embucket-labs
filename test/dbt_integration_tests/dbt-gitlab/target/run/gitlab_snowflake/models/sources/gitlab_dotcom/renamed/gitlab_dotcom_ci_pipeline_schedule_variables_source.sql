
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_pipeline_schedule_variables_source
    

    
    as (WITH source AS (

  SELECT *
  FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_pipeline_schedule_variables_dedupe_source
  
), renamed AS (

    SELECT
      id::NUMBER                    AS ci_pipeline_schedule_variable_id,
      pipeline_schedule_id::NUMBER  AS ci_pipeline_schedule_id,
      created_at::TIMESTAMP         AS created_at,
      updated_at::TIMESTAMP         AS updated_at,
      variable_type                 AS variable_type

    FROM source

)


SELECT *
FROM renamed
    )
;


  