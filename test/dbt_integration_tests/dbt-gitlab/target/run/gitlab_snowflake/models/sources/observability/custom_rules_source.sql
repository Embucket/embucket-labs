
  
    

create or replace transient table EMBUCKET.legacy.custom_rules_source
    

    
    as (WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.custom_rules

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed
    )
;


  