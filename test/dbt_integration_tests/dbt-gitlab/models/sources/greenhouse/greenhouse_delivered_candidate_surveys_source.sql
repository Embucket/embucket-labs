WITH source AS (

  SELECT *

  FROM {{ source('greenhouse', 'delivered_candidate_surveys') }}

),

renamed AS (

  SELECT
    --keys
    id::NUMBER              AS id,
    organization_id::NUMBER AS organization_id,
    department_id::NUMBER   AS department_id,
    office_id::NUMBER       AS office_id,

    --info
    department_name::VARCHAR AS department_name,
    office_name::VARCHAR AS office_name,
    submitted_at::TIMESTAMP   AS submitted_at,
    created_at::TIMESTAMP   AS created_at,
    updated_at::TIMESTAMP   AS updated_at

  FROM source

)

SELECT *
FROM renamed
