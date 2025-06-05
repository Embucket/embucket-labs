
  
    

create or replace transient table EMBUCKET.restricted_safe_common_prep.prep_mart_available_to_renew_snapshot_base
    

    
    as (WITH base AS (

    SELECT *
    FROM "EMBUCKET".snapshots.mart_available_to_renew_snapshot

)

SELECT *
FROM base
    )
;


  