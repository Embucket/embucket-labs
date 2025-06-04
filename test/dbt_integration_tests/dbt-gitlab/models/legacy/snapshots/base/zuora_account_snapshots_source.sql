-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'zuora_account_snapshots') }}

), zuora_account AS (

    SELECT *
    FROM {{ source('zuora', 'account') }}

), renamed AS(

    SELECT
      id                                                     AS account_id,
      -- keys
      communicationprofileid                                 AS communication_profile_id,
      nullif("{{this.database}}".{{target.schema}}.id15to18(crmid), '')  AS crm_id,
      defaultpaymentmethodid                                 AS default_payment_method_id,
      invoicetemplateid                                      AS invoice_template_id,
      parentid                                               AS parent_id,
      soldtocontactid                                        AS sold_to_contact_id,
      billtocontactid                                        AS bill_to_contact_id,
      taxexemptcertificateid                                 AS tax_exempt_certificate_id,
      taxexemptcertificatetype                               AS tax_exempt_certificate_type,

      -- account info
      accountnumber                                          AS account_number,
      name                                                   AS account_name,
      notes                                                  AS account_notes,
      purchaseordernumber                                    AS purchase_order_number,
      accountcode__c                                         AS sfdc_account_code,
      status,
      entity__c                                              AS sfdc_entity,

      autopay                                                AS auto_pay,
      balance                                                AS balance,
      creditbalance                                          AS credit_balance,
      billcycleday                                           AS bill_cycle_day,
      currency                                               AS currency,
      conversionrate__c                                      AS sfdc_conversion_rate,
      paymentterm                                            AS payment_term,

      allowinvoiceedit                                       AS allow_invoice_edit,
      batch,
      invoicedeliveryprefsemail                              AS invoice_delivery_prefs_email,
      invoicedeliveryprefsprint                              AS invoice_delivery_prefs_print,
      paymentgateway                                         AS payment_gateway,

      customerservicerepname                                 AS customer_service_rep_name,
      salesrepname                                           AS sales_rep_name,
      additionalemailaddresses                               AS additional_email_addresses,
      --billtocontact                   as bill_to_contact,
      parent__c                                              AS sfdc_parent,

      sspchannel__c                                          AS ssp_channel,
      porequired__c                                          AS po_required,

      -- financial info
      lastinvoicedate                                        AS last_invoice_date,

      -- metadata
      createdbyid                                            AS created_by_id,
      createddate                                            AS created_date,
      updatedbyid                                            AS updated_by_id,
      updateddate                                            AS updated_date,
      deleted                                                AS is_deleted,

      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to

    FROM source

), final AS (

    SELECT
      renamed.*,
      zuora_account.batch AS live_batch
    FROM renamed
    LEFT JOIN zuora_account
      ON renamed.account_id = zuora_account.id
)

SELECT *
FROM final
