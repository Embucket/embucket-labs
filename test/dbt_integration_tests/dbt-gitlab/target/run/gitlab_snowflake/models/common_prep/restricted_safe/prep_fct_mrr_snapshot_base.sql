
  
    

create or replace transient table EMBUCKET.restricted_safe_common_prep.prep_fct_mrr_snapshot_base
    

    
    as (WITH base AS (

    SELECT *
    FROM "EMBUCKET".snapshots.fct_mrr_snapshot
    
)

SELECT *
FROM base
    )
;


  