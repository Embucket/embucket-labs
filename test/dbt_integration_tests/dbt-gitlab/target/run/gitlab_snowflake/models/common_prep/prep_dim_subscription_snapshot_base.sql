
  
    

create or replace transient table EMBUCKET.common_prep.prep_dim_subscription_snapshot_base
    

    
    as (WITH base AS (

    SELECT *
    FROM "EMBUCKET".snapshots.dim_subscription_snapshot
    
)

SELECT *
FROM base
    )
;


  