WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_subscription_user_add_on_assignments_source') }}

)

SELECT *
FROM source
