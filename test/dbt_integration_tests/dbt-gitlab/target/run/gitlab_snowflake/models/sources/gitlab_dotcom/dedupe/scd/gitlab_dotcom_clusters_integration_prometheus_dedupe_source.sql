
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_clusters_integration_prometheus_dedupe_source
    

    
    as (WITH base AS (

    SELECT *
    FROM EMBUCKET.tap_postgres.gitlab_db_clusters_integration_prometheus

)

, max_task_instance AS (
    SELECT MAX(_task_instance) AS max_column_value
    FROM base
    WHERE RIGHT( _task_instance, 8) = (

                                SELECT MAX(RIGHT( _task_instance, 8))
                                FROM base )

), filtered AS (

    SELECT *
    FROM base
    WHERE _task_instance = (

                            SELECT max_column_value
                            FROM max_task_instance

                            )
    -- Keep only the latest state of the data,
    -- if we have multiple records per day
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY _uploaded_at DESC) = 1
)

SELECT *
FROM filtered
    )
;


  