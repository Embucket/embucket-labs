WITH source AS (

  SELECT *
  FROM {{ source('greenhouse', 'jobs') }}

),

renamed AS (

  SELECT

    --keys
    id::NUMBER              AS job_id,
    organization_id::NUMBER AS organization_id,
    requisition_id::VARCHAR AS requisition_id,
    department_id::NUMBER   AS department_id,

    --info
    name::VARCHAR           AS job_name,
    status::VARCHAR         AS job_status,
    opened_at::TIMESTAMP    AS job_opened_at,
    closed_at::TIMESTAMP    AS job_closed_at,
    level::VARCHAR          AS job_level,
    confidential::BOOLEAN   AS is_confidential,
    is_template::BOOLEAN    AS is_template,
    created_at::TIMESTAMP   AS job_created_at,
    notes::VARCHAR          AS job_notes,
    updated_at::TIMESTAMP   AS job_updated_at


  FROM source

)

SELECT *
FROM renamed
