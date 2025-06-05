
  
    

create or replace transient table EMBUCKET.dbt_artifacts.snapshot_executions
    

    
    as (/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as TEXT) as command_invocation_id,
    cast(null as TEXT) as node_id,
    cast(null as timestamp) as run_started_at,
    cast(null as boolean) as was_full_refresh,
    cast(null as TEXT) as thread_id,
    cast(null as TEXT) as status,
    cast(null as timestamp) as compile_started_at,
    cast(null as timestamp) as query_completed_at,
    cast(null as float) as total_node_runtime,
    cast(null as integer) as rows_affected,
    cast(null as TEXT) as materialization,
    cast(null as TEXT) as  schema
    ,
    cast(null as TEXT) as name,
    cast(null as TEXT) as alias,
    cast(null as TEXT) as message,
    cast(null as 
   object
) as adapter_response
from dummy_cte
where 1 = 0
    )
;


  