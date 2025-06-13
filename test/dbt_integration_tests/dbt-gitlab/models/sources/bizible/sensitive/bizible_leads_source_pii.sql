WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_leads_source', 'lead_id') }}
    FROM {{ ref('bizible_leads_source') }}

)

SELECT *
FROM source