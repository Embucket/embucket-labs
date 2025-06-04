

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_details_source') }}

)

SELECT *
FROM source
