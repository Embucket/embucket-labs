

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_saml_providers_source') }}

)

SELECT *
FROM source
