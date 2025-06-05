
  
    

create or replace transient table EMBUCKET.legacy.incident_history_source
    

    
    as (WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.incident_history

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed
    )
;


  