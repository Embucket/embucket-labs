
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.ssa_quarterly_aggregated_metrics_for_coverage
    

    
    as (WITH base AS (

    SELECT *
    FROM EMBUCKET.driveload.driveload_ssa_quarterly_aggregated_metrics_for_coverage_source

)

SELECT *
FROM base
    )
;


  