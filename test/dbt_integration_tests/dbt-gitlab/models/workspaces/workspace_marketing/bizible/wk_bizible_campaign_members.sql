WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_campaign_members_source') }}
    FROM {{ ref('bizible_campaign_members_source') }}

)

SELECT *
FROM source