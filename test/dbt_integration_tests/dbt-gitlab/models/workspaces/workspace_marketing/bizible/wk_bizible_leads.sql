WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_leads_source') }}
    FROM {{ ref('bizible_leads_source') }}

)

SELECT *
FROM source