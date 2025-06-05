
  
    

create or replace transient table EMBUCKET.rally_initial_export.rally_initial_export_optouts_source
    

    
    as (WITH source AS (
  SELECT *
  FROM
    EMBUCKET.rally_initial_export.rally_initial_export_optouts
)

SELECT *
FROM source
    )
;


  