
  
    

create or replace transient table EMBUCKET.legacy.cleanup_suggestions_source
    

    
    as (WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.cleanup_suggestions

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed
    )
;


  