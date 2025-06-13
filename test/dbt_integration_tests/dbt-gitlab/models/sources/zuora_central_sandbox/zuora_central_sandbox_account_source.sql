WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'account') }}

), renamed AS(

    SELECT
      id                                                     AS account_id,
      -- keys
      communication_profile_id                               AS communication_profile_id,
      nullif("{{this.database}}".{{target.schema}}.id15to18(IFNULL(crm_id, '')), '')
                                                             AS crm_id,

      default_payment_method_id                              AS default_payment_method_id,
      invoice_template_id                                    AS invoice_template_id,
      parent_id                                              AS parent_id,
      sold_to_contact_id                                     AS sold_to_contact_id,
      bill_to_contact_id                                     AS bill_to_contact_id,
      tax_exempt_certificate_id                              AS tax_exempt_certificate_id,
      tax_exempt_certificate_type                            AS tax_exempt_certificate_type,

      -- account info
      account_number                                         AS account_number,
      name                                                   AS account_name,
      notes                                                  AS account_notes,
      purchase_order_number                                  AS purchase_order_number,
      account_code_c                                         AS sfdc_account_code,
      status                                                 AS status,
      entity_c                                               AS sfdc_entity,

      auto_pay                                               AS auto_pay,
      balance                                                AS balance,
      credit_balance                                         AS credit_balance,
      bill_cycle_day                                         AS bill_cycle_day,
      currency                                               AS currency,
      conversion_rate_c                                      AS sfdc_conversion_rate,
      payment_term                                           AS payment_term,

      allow_invoice_edit                                     AS allow_invoice_edit,
      batch                                                  AS batch,
      invoice_delivery_prefs_email                           AS invoice_delivery_prefs_email,
      invoice_delivery_prefs_print                           AS invoice_delivery_prefs_print,
      payment_gateway                                        AS payment_gateway,

      customer_service_rep_name                              AS customer_service_rep_name,
      sales_rep_name                                         AS sales_rep_name,
      additional_email_addresses                             AS additional_email_addresses,
      parent_c                                               AS sfdc_parent,
      sspchannel_c                                           AS ssp_channel,
      porequired_c                                           AS po_required,

      -- financial info
      last_invoice_date                                      AS last_invoice_date,

      -- metadata
      created_by_id                                          AS created_by_id,
      created_date                                           AS created_date,
      updated_by_id                                          AS updated_by_id,
      updated_date                                           AS updated_date,
      _FIVETRAN_DELETED                                      AS is_deleted

    FROM source

)

SELECT *
FROM renamed
