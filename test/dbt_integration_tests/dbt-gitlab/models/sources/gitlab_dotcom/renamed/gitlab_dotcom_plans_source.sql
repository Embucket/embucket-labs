
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_plans_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                      AS plan_id,
      created_at::TIMESTAMP           AS created_at,
      updated_at::TIMESTAMP           AS updated_at,
      name::VARCHAR                   AS plan_name,
      title::VARCHAR                  AS plan_title,
      id IN (2, 3, 4, 100, 101)       AS plan_is_paid

    FROM source

)

SELECT *
FROM renamed
