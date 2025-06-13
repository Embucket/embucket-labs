WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_campaign_members_source', 'campaign_member_id') }}
    FROM {{ ref('bizible_campaign_members_source') }}

)

SELECT *
FROM source