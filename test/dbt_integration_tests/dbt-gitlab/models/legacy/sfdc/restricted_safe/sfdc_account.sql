WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL
    AND is_deleted = FALSE

)
SELECT *
FROM source
