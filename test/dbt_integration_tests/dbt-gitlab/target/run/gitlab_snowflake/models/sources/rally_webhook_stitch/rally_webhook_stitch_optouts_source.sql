
  
    

create or replace transient table EMBUCKET.rally_initial_export.rally_webhook_stitch_optouts_source
    

    
    as (WITH source AS (
  SELECT
    email,
    optedout  AS opted_out,
    updatedat AS updated_at

  FROM
    EMBUCKET.rally_webhook_stitch.data
)

SELECT *
FROM source
    )
;


  