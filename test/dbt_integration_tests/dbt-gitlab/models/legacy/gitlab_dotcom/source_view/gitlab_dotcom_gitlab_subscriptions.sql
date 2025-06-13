

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_source') }}

)

SELECT *
FROM source
