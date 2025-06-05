
  
    

create or replace transient table EMBUCKET.legacy.monitors_source
    

    
    as (WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.monitors

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed
    )
;


  