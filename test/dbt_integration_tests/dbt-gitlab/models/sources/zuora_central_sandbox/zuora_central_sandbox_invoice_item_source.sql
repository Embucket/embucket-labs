WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'invoice_item') }}

), renamed AS (

    SELECT
      -- keys
      id                            AS invoice_item_id,
      invoice_id                    AS invoice_id,
      applied_to_invoice_item_id    AS applied_to_invoice_item_id,
      rate_plan_charge_id           AS rate_plan_charge_id,
      subscription_id               AS subscription_id,


      -- invoice item metadata
      accounting_code               AS accounting_code,
      product_id                    AS product_id,
      product_rate_plan_charge_id   AS product_rate_plan_charge_id,
      service_end_date              AS service_end_date,
      service_start_date            AS service_start_date,


      -- financial info
      charge_amount                 AS charge_amount,
      charge_date                   AS charge_date,
      charge_name                   AS charge_name,
      processing_type               AS processing_type,
      quantity                      AS quantity,
      sku                           AS sku,
      tax_amount                    AS tax_amount,
      tax_code                      AS tax_code,
      tax_exempt_amount             AS tax_exempt_amount,
      tax_mode                      AS tax_mode,
      uom                           AS unit_of_measure,
      unit_price                    AS unit_price,

      -- metadata
      created_by_id                 AS created_by_id,
      created_date                  AS created_date,
      updated_by_id                 AS updated_by_id,
      updated_date                  AS updated_date,
      _FIVETRAN_DELETED             AS is_deleted


    FROM source

)

SELECT *
FROM renamed
