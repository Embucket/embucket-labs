{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('dim_invoice', 'dim_invoice'),
    ('fct_invoice', 'fct_invoice'),
    ('fct_invoice_item', 'fct_invoice_item'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('dim_crm_user', 'dim_crm_user'),
    ('dim_product_detail', 'dim_product_detail'),
    ('driveload_pending_invoices_report_source', 'driveload_pending_invoices_report_source'),
    ('dim_billing_account', 'dim_billing_account')
]) }},

issued_invoices_basis AS (

-- pulling all respective potential bad debt invoices with the related opportunity IDs - invoices are posted, have balance more than 0 and are more than 75 days overdue

  SELECT
    dim_invoice.invoice_number,
    DATE(dim_invoice.invoice_date)                                                                        AS invoice_date,
    DATE(dim_invoice.due_date)                                                                            AS due_date,
    DATEDIFF(DAY, due_date, CURRENT_DATE())                                                               AS days_overdue,
    fct_invoice.amount_without_tax,
    fct_invoice.tax_amount,
    fct_invoice.payment_amount,
    fct_invoice.balance,
    dim_subscription.subscription_name,
    dim_subscription.dim_crm_opportunity_id,
    dim_subscription.dim_billing_account_id, -- SUBSCRIPTION OWNER
    dim_subscription.dim_crm_account_id, -- SUBSCRIPTION OWNER
    dim_crm_account.crm_account_name,
    dim_crm_account.crm_account_owner,
    mart_crm_opportunity.opportunity_name,
    mart_crm_opportunity.opportunity_owner,
    dim_crm_user.user_name                                                                                AS renewal_manager,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.subscription_type,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.resale_partner_name,
    mart_crm_opportunity.net_arr,
    mart_crm_opportunity.arr_basis,
    mart_crm_opportunity.amount                                                                           AS opportunity_amount,
    mart_crm_opportunity.report_segment,
    mart_crm_opportunity.report_geo,
    ROW_NUMBER() OVER (PARTITION BY dim_invoice.invoice_number ORDER BY mart_crm_opportunity.amount DESC) AS row_num
  FROM dim_invoice
  LEFT JOIN fct_invoice
    ON dim_invoice.dim_invoice_id = fct_invoice.dim_invoice_id
  LEFT JOIN fct_invoice_item
    ON dim_invoice.dim_invoice_id = fct_invoice_item.dim_invoice_id
  LEFT JOIN dim_subscription
    ON fct_invoice_item.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN mart_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_crm_account
    ON dim_subscription.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_user
    ON mart_crm_opportunity.renewal_manager = dim_crm_user.dim_crm_user_id
  WHERE days_overdue > 75
    AND fct_invoice.balance != 0
    AND fct_invoice.amount > 0
    AND dim_invoice.status = 'Posted'
  ORDER BY dim_invoice.invoice_number DESC, row_num ASC

),

possible_opportunities AS (

-- listing other possible opportunities related to the invoice

  SELECT
    issued_invoices_basis.invoice_number,
    ARRAY_AGG(DISTINCT mart_crm_opportunity.dim_crm_opportunity_id) AS possible_opportunity_id
  FROM issued_invoices_basis
  LEFT JOIN mart_crm_opportunity
    ON issued_invoices_basis.invoice_number = mart_crm_opportunity.invoice_number
  GROUP BY issued_invoices_basis.invoice_number

),

professional_services_invoices AS (

-- list of potential bad debt professional services invoices

  SELECT DISTINCT fct_invoice_item.invoice_number
  FROM fct_invoice_item
  LEFT JOIN dim_product_detail
    ON fct_invoice_item.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  INNER JOIN issued_invoices_basis
    ON fct_invoice_item.invoice_number = issued_invoices_basis.invoice_number
  WHERE dim_product_detail.product_name LIKE 'Professional%'

),

pending_invoices_basis AS (

-- adding invoices in preview that haven't been billed yet for billing account with invoices that were issued but are potential bad debt

  SELECT
    SUM(driveload_pending_invoices_report_source.invoice_amount) AS pending_invoice_amount,
    driveload_pending_invoices_report_source.subscription_name,
    driveload_pending_invoices_report_source.invoice_start_date,
    driveload_pending_invoices_report_source.account_id,
    driveload_pending_invoices_report_source.opportunity_id,
    dim_billing_account.dim_crm_account_id
  FROM driveload_pending_invoices_report_source
  INNER JOIN issued_invoices_basis
    ON driveload_pending_invoices_report_source.account_id = issued_invoices_basis.dim_billing_account_id
  LEFT JOIN dim_billing_account
    ON driveload_pending_invoices_report_source.account_id = dim_billing_account.dim_billing_account_id
  WHERE issued_invoices_basis.row_num = 1
    AND driveload_pending_invoices_report_source.invoice_amount > 0
  GROUP BY 2, 3, 4, 5, 6

),

issued_invoices_final AS (

-- removing duplicated lines due to joining with fct_invoice_item and adding SFDC information

  SELECT
    issued_invoices_basis.invoice_number,
    issued_invoices_basis.invoice_date,
    issued_invoices_basis.due_date,
    issued_invoices_basis.days_overdue,
    issued_invoices_basis.amount_without_tax,
    issued_invoices_basis.tax_amount,
    issued_invoices_basis.payment_amount,
    issued_invoices_basis.balance,
    issued_invoices_basis.subscription_name,
    issued_invoices_basis.dim_crm_opportunity_id,
    issued_invoices_basis.opportunity_name,
    issued_invoices_basis.opportunity_owner,
    issued_invoices_basis.renewal_manager,
    issued_invoices_basis.crm_account_name,
    issued_invoices_basis.crm_account_owner,
    issued_invoices_basis.close_date,
    issued_invoices_basis.subscription_type,
    issued_invoices_basis.deal_path_name,
    issued_invoices_basis.resale_partner_name,
    CASE
      WHEN issued_invoices_basis.resale_partner_name LIKE 'Amazon%' OR issued_invoices_basis.resale_partner_name LIKE 'Google%'
        THEN 'TRUE'
      ELSE 'FALSE'
    END AS is_marketplace,
    CASE
      WHEN professional_services_invoices.invoice_number IS NOT NULL
        THEN 'TRUE'
      ELSE 'FALSE'
    END AS is_professional_services,
    issued_invoices_basis.net_arr,
    issued_invoices_basis.arr_basis,
    issued_invoices_basis.opportunity_amount,
    issued_invoices_basis.report_segment,
    issued_invoices_basis.report_geo,
    possible_opportunities.possible_opportunity_id
  FROM issued_invoices_basis
  LEFT JOIN possible_opportunities
    ON issued_invoices_basis.invoice_number = possible_opportunities.invoice_number
  LEFT JOIN professional_services_invoices
    ON issued_invoices_basis.invoice_number = professional_services_invoices.invoice_number
  WHERE issued_invoices_basis.row_num = 1
  ORDER BY issued_invoices_basis.invoice_number

),

pending_invoices_final AS (

-- adding SFDC information to the pending invoices

  SELECT
    'pending invoice'                             AS invoice_number,
    pending_invoices_basis.invoice_start_date     AS invoice_date,
    NULL                                          AS due_date,
    NULL                                          AS days_overdue,
    pending_invoices_basis.pending_invoice_amount AS amount_without_tax,
    NULL                                          AS tax_amount,
    NULL                                          AS payment_amount,
    pending_invoices_basis.pending_invoice_amount AS balance,
    pending_invoices_basis.subscription_name,
    pending_invoices_basis.opportunity_id         AS dim_crm_opportunity_id,
    mart_crm_opportunity.opportunity_name,
    mart_crm_opportunity.opportunity_owner,
    dim_crm_user.user_name                        AS renewal_manager,
    dim_crm_account.crm_account_name,
    dim_crm_account.crm_account_owner,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.subscription_type,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.resale_partner_name,
    CASE
      WHEN mart_crm_opportunity.resale_partner_name LIKE 'Amazon%' OR mart_crm_opportunity.resale_partner_name LIKE 'Google%'
        THEN 'TRUE'
      ELSE 'FALSE'
    END                                           AS is_marketplace,
    'FALSE'                                       AS is_professional_services,
    mart_crm_opportunity.net_arr,
    mart_crm_opportunity.arr_basis,
    mart_crm_opportunity.amount                   AS opportunity_amount,
    mart_crm_opportunity.report_segment,
    mart_crm_opportunity.report_geo,
    NULL                                          AS possible_opportunity_id
  FROM pending_invoices_basis
  LEFT JOIN mart_crm_opportunity
    ON pending_invoices_basis.opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_crm_account
    ON pending_invoices_basis.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_user
    ON mart_crm_opportunity.renewal_manager = dim_crm_user.dim_crm_user_id
  ORDER BY pending_invoices_basis.pending_invoice_amount DESC

),

final AS (

-- joining the data for the already issued and pending invoices

  SELECT *
  FROM issued_invoices_final

  UNION ALL

  SELECT *
  FROM pending_invoices_final
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-10-21",
updated_date="2024-10-21"
) }}
