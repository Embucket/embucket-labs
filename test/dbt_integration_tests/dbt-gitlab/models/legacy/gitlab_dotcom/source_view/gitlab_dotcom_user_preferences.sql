

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_preferences_source') }}

)

SELECT *
FROM source
