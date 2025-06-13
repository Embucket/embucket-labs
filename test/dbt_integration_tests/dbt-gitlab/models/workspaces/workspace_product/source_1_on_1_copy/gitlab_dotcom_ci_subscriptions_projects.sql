WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_subscriptions_projects_source') }}

)

SELECT *
FROM source
