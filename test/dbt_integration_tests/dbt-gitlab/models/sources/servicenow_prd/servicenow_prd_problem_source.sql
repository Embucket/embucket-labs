WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','problem') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                           AS sys_id,
    rfc_value::VARCHAR                        AS rfc_value,
    confirmed_by_value::VARCHAR               AS confirmed_by_value,
    confirmed_at::TIMESTAMP                   AS confirmed_at,
    sys_updated_on::TIMESTAMP                 AS sys_updated_on,
    duplicate_of_value::VARCHAR               AS duplicate_of_value,
    workaround_communicated_at::TIMESTAMP     AS workaround_communicated_at,
    fix_at::TIMESTAMP                         AS fix_at,
    resolved_at::TIMESTAMP                    AS resolved_at,
    fix_communicated_by_value::VARCHAR        AS fix_communicated_by_value,
    resolved_by_value::VARCHAR                AS resolved_by_value,
    sys_created_on::TIMESTAMP                 AS sys_created_on,
    fix_by_value::VARCHAR                     AS fix_by_value,
    workaround_applied::BOOLEAN               AS workaround_applied,
    fix_communicated_at::TIMESTAMP            AS fix_communicated_at,
    related_incidents::NUMBER                 AS related_incidents,
    reopen_count::NUMBER                      AS reopen_count,
    subcategory::VARCHAR                      AS subcategory,
    known_error::BOOLEAN                      AS known_error,
    first_reported_by_task_value::VARCHAR     AS first_reported_by_task_value,
    workaround_communicated_by_value::VARCHAR AS workaround_communicated_by_value,
    category::VARCHAR                         AS category,
    reopened_by_value::VARCHAR                AS reopened_by_value,
    reopened_at::TIMESTAMP                    AS reopened_at,
    resolution_code::VARCHAR                  AS resolution_code,
    major_problem::BOOLEAN                    AS major_problem,
    _fivetran_synced::TIMESTAMP               AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN                AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed