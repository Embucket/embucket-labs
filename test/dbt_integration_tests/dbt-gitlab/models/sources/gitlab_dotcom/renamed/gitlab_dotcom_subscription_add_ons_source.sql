WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_subscription_add_ons_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                AS id,
      name::NUMBER              AS name,
      description::VARCHAR      AS description,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at

    FROM source

)

SELECT  *
FROM renamed
