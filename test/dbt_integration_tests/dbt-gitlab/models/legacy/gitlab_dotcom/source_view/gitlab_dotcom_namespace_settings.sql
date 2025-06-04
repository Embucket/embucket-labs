

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_settings_source') }}

)

SELECT *
FROM source
