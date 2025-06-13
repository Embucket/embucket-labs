    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_identities_dedupe_source') }}
  
),
renamed AS (

    SELECT
      id::NUMBER                 AS identity_id,
      provider::VARCHAR           AS identity_provider,
      user_id::NUMBER            AS user_id,
      created_at::TIMESTAMP       AS created_at,
      updated_at::TIMESTAMP       AS updated_at,
      saml_provider_id::NUMBER   AS saml_provider_id
    FROM source

)

SELECT *
FROM renamed
