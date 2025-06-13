WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','incident_task') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                AS sys_id,
    incident_value::VARCHAR        AS incident_value,
    sys_created_on::TIMESTAMP      AS sys_created_on,
    sys_updated_on::TIMESTAMP      AS sys_updated_on,
    _fivetran_synced::TIMESTAMP    AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN     AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed