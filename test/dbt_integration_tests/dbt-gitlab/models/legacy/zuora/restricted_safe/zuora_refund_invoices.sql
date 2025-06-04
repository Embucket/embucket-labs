WITH invoices AS (

    SELECT * FROM {{ref('zuora_invoice')}}

), zuora_account AS (

    SELECT * FROM {{ref('zuora_account')}}

/* Self join invoices before and after the invoice date to get a sum of other transactions paid. */
), joined AS (

    SELECT DISTINCT
      invoices.invoice_id,
      invoices.amount                                  AS invoice_amount,
      invoices.account_id,
      invoices.invoice_date,
      zuora_account.crm_id,
      zuora_account.sfdc_entity,
      zuora_account.account_name,
      zuora_account.account_number,
      zuora_account.currency
    FROM invoices
      LEFT JOIN zuora_account
        ON invoices.account_id = zuora_account.account_id
      LEFT JOIN invoices AS before_and_after -- 60 day window before and after the invoice date.
        ON invoices.account_id = before_and_after.account_id
        AND before_and_after.invoice_date BETWEEN DATEADD('days', -60, invoices.invoice_date) AND DATEADD('days', 60, invoices.invoice_date)
    {{ dbt_utils.group_by(9) }}
    -- To count as a refund, the customer must up even ($0) or better (<$0)
    HAVING SUM(COALESCE(before_and_after.amount, 0)) <= 0

)

SELECT *
FROM joined
WHERE invoice_amount < 0 -- Only include the rows that are actually negative
ORDER BY
  invoice_date,
  invoice_amount
