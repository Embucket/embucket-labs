
  
    

create or replace transient table EMBUCKET.legacy.dashboard_analytics_source
    

    
    as (WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.dashboard_analytics

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed
    )
;


  