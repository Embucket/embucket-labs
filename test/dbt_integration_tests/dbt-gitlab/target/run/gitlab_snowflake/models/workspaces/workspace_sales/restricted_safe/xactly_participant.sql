
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_participant
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_participant_source

)

SELECT *
FROM source
    )
;


  