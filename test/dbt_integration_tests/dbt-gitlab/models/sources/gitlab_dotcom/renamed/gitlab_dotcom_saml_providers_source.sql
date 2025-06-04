WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_saml_providers_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                              AS saml_provider_id,
      group_id::NUMBER                        AS group_id,
      enabled::BOOLEAN                         AS is_enabled,
      certificate_fingerprint::VARCHAR         AS certificate_fingerprint,
      sso_url::VARCHAR                         AS sso_url,
      enforced_sso::BOOLEAN                    AS is_enforced_sso,
      enforced_group_managed_accounts::BOOLEAN AS is_enforced_group_managed_accounts,
      prohibited_outer_forks::BOOLEAN          AS is_prohibited_outer_forks,
      default_membership_role::NUMBER          AS default_membership_role_id
    FROM source

)

SELECT *
FROM renamed
