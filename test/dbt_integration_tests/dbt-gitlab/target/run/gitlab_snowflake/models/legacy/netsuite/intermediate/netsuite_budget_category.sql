
  
    

create or replace transient table EMBUCKET.legacy.netsuite_budget_category
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.netsuite.netsuite_budget_category_source

)

SELECT *
FROM source
WHERE is_fivetran_deleted = FALSE
    )
;


  