
  
    

create or replace transient table EMBUCKET.snowplow.snowplow_fishtown_bad_events_source
    

    
    as (WITH source as (

    SELECT *
    FROM EMBUCKET.snowplow.bad_events

)

SELECT *
FROM source
    )
;


  