WITH source AS (
    SELECT
    {{ hash_sensitive_columns('sfdc_lead_source') }}
    FROM {{ ref('sfdc_lead_source') }}
    WHERE is_deleted = FALSE
)
SELECT *
FROM source