WITH source AS (

    SELECT *
    FROM {{ source('airflow', 'dag_run') }}

), renamed AS (

    SELECT
      dag_id::VARCHAR           AS dag_id,
      execution_date::TIMESTAMP AS execution_date,
      state::VARCHAR            AS run_state
    FROM source

)

SELECT *
FROM renamed