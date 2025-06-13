WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_subscription_add_ons_source') }}

)

SELECT *
FROM source
