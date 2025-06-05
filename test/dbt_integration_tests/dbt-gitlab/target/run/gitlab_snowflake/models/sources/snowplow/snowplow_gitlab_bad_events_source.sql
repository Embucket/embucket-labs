
  
    

create or replace transient table EMBUCKET.snowplow.snowplow_gitlab_bad_events_source
    

    
    as (WITH source as (

    SELECT *
    FROM EMBUCKET.snowplow.gitlab_bad_events

)

SELECT *
FROM source
    )
;


  