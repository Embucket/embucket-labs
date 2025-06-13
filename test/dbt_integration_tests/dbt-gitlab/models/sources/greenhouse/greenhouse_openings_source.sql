WITH source AS (

  SELECT *
  FROM {{ source('greenhouse', 'openings') }}

),

renamed AS (

  SELECT
    --keys
    id::NUMBER                   AS job_opening_id,
    job_id::NUMBER               AS job_id,
    opening_id::VARCHAR          AS opening_id,
    hired_application_id::NUMBER AS hired_application_id,

    --info
    status::VARCHAR              AS opening_status,
    opened_at::TIMESTAMP         AS job_opened_at,
    closed_at::TIMESTAMP         AS job_closed_at,
    close_reason::VARCHAR        AS close_reason,
    created_at::TIMESTAMP        AS job_opening_created_at,
    updated_at::TIMESTAMP        AS job_opening_updated_at,
    target_start_date::DATE      AS target_start_date

  FROM source

)

SELECT *
FROM renamed
