WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_zoekt_enabled_namespaces_source') }}

)

SELECT *
FROM source
