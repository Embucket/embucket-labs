WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_banned_users_source') }}

)

SELECT *
FROM source
