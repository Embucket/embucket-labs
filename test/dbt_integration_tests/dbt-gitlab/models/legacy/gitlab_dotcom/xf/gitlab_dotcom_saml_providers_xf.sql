

WITH saml_providers AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_saml_providers') }}

)

, identities AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_identities') }}

)

, joined AS (
  
    SELECT 
      saml_providers.*,
      COUNT(DISTINCT user_id) AS saml_provider_user_count,
      MIN(created_at)         AS first_saml_provider_created_at
    FROM saml_providers
    LEFT JOIN identities 
      ON saml_providers.saml_provider_id = identities.saml_provider_id
      AND {{ filter_out_blocked_users('identities', 'user_id') }}
    {{ dbt_utils.group_by(n=9) }}
)

SELECT *
FROM joined
