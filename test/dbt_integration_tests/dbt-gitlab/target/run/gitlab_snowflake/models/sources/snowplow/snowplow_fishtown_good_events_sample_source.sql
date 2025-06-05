
  
    

create or replace transient table EMBUCKET.snowplow.snowplow_fishtown_good_events_sample_source
    

    
    as (WITH source as (

    SELECT *
    FROM EMBUCKET.snowplow.fishtown_events_sample

)

SELECT *
FROM source
    )
;


  