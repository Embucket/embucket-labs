
  
    

create or replace transient table EMBUCKET.legacy.gitlab_dotcom_merge_request_diffs
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_merge_request_diffs_source

)

SELECT *
FROM source
    )
;


  