
  
    

create or replace transient table EMBUCKET.legacy.gitlab_dotcom_ci_sources_pipelines
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_sources_pipelines_source

)

SELECT *
FROM source
    )
;


  