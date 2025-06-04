WITH survey_builder_surveys AS (

  SELECT *

  FROM {{ source('greenhouse', 'survey_builder_surveys') }}

),

renamed AS (

  SELECT
    --keys
    id::NUMBER              AS survey_id,
    organization_id::NUMBER AS organization_id,

    --info
    key::VARCHAR            AS survey_key,
    active:BOOLEAN          AS is_survey_active,
    activated_at::TIMESTAMP AS activated_at,
    created_at::TIMESTAMP   AS created_at,
    updated_at::TIMESTAMP   AS updated_at

  FROM survey_builder_surveys

)

SELECT *
FROM renamed
