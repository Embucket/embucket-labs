WITH source AS (

    SELECT *
    FROM {{ ref('zuora_invoice_item_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
