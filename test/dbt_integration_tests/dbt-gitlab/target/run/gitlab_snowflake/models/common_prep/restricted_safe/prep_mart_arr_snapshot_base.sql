
  
    

create or replace transient table EMBUCKET.restricted_safe_common_prep.prep_mart_arr_snapshot_base
    

    
    as (WITH base AS (

    SELECT *
    FROM EMBUCKET.snapshots.mart_arr_snapshot
    
)

SELECT *
FROM base
    )
;


  