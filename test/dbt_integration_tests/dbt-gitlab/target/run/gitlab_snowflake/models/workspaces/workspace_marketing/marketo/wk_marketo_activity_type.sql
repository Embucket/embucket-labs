
  
    

create or replace transient table EMBUCKET.workspace_marketing.wk_marketo_activity_type
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.marketo.marketo_activity_type_source

)

SELECT *
FROM source
    )
;


  