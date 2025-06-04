

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_identities_source') }}

)

SELECT *
FROM source
