WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_subscription_add_on_purchases_source') }}

)

SELECT *
FROM source
