WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','problem_task') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                AS sys_id,
    started_by_value::VARCHAR      AS started_by_value,
    started_at::TIMESTAMP          AS started_at,
    fix_notes::VARCHAR             AS fix_notes,
    vendor_value::VARCHAR          AS vendor_value,
    reopen_count::NUMBER           AS reopen_count,
    sys_updated_on::TIMESTAMP      AS sys_updated_on,
    close_code::VARCHAR            AS close_code,
    sys_created_on::TIMESTAMP      AS sys_created_on,
    reopened_by_value::VARCHAR     AS reopened_by_value,
    problem_task_type::VARCHAR     AS problem_task_type,
    reopened_at::TIMESTAMP         AS reopened_at,
    problem_value::VARCHAR         AS problem_value,
    cause_code::VARCHAR            AS cause_code,
    _fivetran_synced::TIMESTAMP    AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN     AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed