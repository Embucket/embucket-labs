
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.xactly_attainment_measure
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.legacy.xactly_attainment_measure_source

)

SELECT *
FROM source
    )
;


  