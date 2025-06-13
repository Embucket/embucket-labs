WITH source AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_contact_source') }}
    FROM {{ ref('sfdc_contact_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source