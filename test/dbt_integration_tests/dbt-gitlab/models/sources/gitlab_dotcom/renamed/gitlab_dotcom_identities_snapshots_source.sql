WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_identities_snapshots') }}

), renamed AS (

    SELECT
      id::NUMBER AS identity_id,
      provider::VARCHAR AS identity_provider,
      user_id::NUMBER AS user_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      saml_provider_id::NUMBER AS saml_provider_id,
      dbt_valid_from::TIMESTAMP AS dbt_valid_from,
      dbt_valid_to::TIMESTAMP AS dbt_valid_to
    FROM source
    
)

SELECT  *
FROM renamed