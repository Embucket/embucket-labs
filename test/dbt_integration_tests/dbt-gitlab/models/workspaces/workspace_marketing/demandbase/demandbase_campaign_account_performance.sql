WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_campaign_account_performance_source') }}

)

SELECT *
FROM source