WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'invoice_item') }}

), renamed AS (

    SELECT
      -- keys
      id                      AS invoice_item_id,
      invoiceid               AS invoice_id,
      appliedtoinvoiceitemid  AS applied_to_invoice_item_id,
      rateplanchargeid        AS rate_plan_charge_id,
      subscriptionid          AS subscription_id,


      -- invoice item metadata
      accountingcode          AS accounting_code,
      productid               AS product_id,
      productrateplanchargeid AS product_rate_plan_charge_id,

      --revrecstartdate        AS revenue_recognition_start_date,
      serviceenddate          AS service_end_date,
      servicestartdate        AS service_start_date,


      -- financial info
      chargeamount            AS charge_amount,
      chargedate              AS charge_date,
      chargename              AS charge_name,
      processingtype          AS processing_type,
      quantity                AS quantity,
      sku                     AS sku,
      taxamount               AS tax_amount,
      taxcode                 AS tax_code,
      taxexemptamount         AS tax_exempt_amount,
      taxmode                 AS tax_mode,
      uom                     AS unit_of_measure,
      unitprice               AS unit_price,

      -- metadata
      createdbyid             AS created_by_id,
      createddate             AS created_date,
      updatedbyid             AS updated_by_id,
      updateddate             AS updated_date,
      deleted                 AS is_deleted


    FROM source

)

SELECT *
FROM renamed
