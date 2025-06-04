WITH source AS (

    SELECT *
    FROM {{ source('airflow', 'dag') }}

), renamed AS (

    SELECT
      dag_id::VARCHAR               AS dag_id,
      is_active::BOOLEAN            AS is_active,
      is_paused::VARCHAR            AS is_paused,
      schedule_interval::VARCHAR    AS schedule_interval
    FROM source

)

SELECT *
FROM renamed