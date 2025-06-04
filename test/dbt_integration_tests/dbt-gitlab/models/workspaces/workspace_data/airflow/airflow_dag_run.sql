WITH source AS (

    SELECT *
    FROM {{ ref('airflow_dag_run_source') }}

), renamed AS (

    SELECT
      dag_id,
      execution_date,
      run_state
    FROM source

)

SELECT *
FROM renamed