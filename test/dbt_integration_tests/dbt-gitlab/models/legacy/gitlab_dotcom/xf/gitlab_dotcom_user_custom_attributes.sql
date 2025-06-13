

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_custom_attributes_source') }}

)

SELECT *
FROM source
