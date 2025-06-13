{%
set age_of_invoice = "datediff(day, zuora_invoice.due_date, CURRENT_DATE)"
%}

WITH zuora_invoice as (

  SELECT * FROM {{ref('zuora_invoice')}}

), zuora_account as (

  SELECT * FROM {{ref('zuora_account')}}

), open_invoices as (

  SELECT zuora_account.account_name,
       CASE
          WHEN {{ age_of_invoice }} < 30 THEN '1: <30'
          WHEN {{ age_of_invoice }} >= 30 
            AND {{ age_of_invoice }} <= 60 THEN '2: 30-60'
          WHEN {{ age_of_invoice }} >= 61 
            AND {{ age_of_invoice }} <= 90 THEN '3: 61-90'
          WHEN {{ age_of_invoice }} >= 91 THEN '4: >90'
          ELSE 'Unknown'
        END AS day_range,
        LISTAGG(zuora_invoice.invoice_number, ', ') 
          OVER (partition by zuora_account.account_name) as open_invoices
  FROM zuora_invoice
  INNER JOIN zuora_account
    ON zuora_invoice.account_id = zuora_account.account_id
  WHERE {{ age_of_invoice }} >= 31

)

SELECT account_name,
        day_range,
        max(open_invoices) as list_of_open_invoices 
FROM open_invoices
GROUP BY 1, 2
