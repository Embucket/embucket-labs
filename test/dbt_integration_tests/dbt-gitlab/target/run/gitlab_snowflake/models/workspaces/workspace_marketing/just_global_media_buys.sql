
  
    

create or replace transient table EMBUCKET.workspace_marketing.just_global_media_buys
    

    
    as (WITH
source AS (
  SELECT * FROM

    EMBUCKET.just_global_campaigns.media_buys_source
)

SELECT *
FROM source
    )
;


  