WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_settings_source') }}

)

SELECT *
FROM source
