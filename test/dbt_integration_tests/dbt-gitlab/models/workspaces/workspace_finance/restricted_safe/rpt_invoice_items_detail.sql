{{ simple_cte([
    ('fct_invoice_item', 'fct_invoice_item'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail')
]) }},

invoice_detail AS (

  SELECT
    fct_invoice_item.invoice_number,
    fct_invoice_item.invoice_item_id,
    fct_invoice_item.dim_crm_account_id_invoice,
    dim_crm_account.crm_account_name,
    dim_crm_account.crm_account_type,
    fct_invoice_item.invoice_date,
    dim_product_detail.product_name,
    dim_product_detail.product_rate_plan_name,
    dim_product_detail.annual_billing_list_price,
    fct_invoice_item.quantity,
    fct_invoice_item.invoice_item_unit_price,
    fct_invoice_item.invoice_item_charge_amount,
    fct_invoice_item.annual_billing_invoice_price,
    fct_invoice_item.arr,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_date.first_day_of_month                         AS invoice_month,
    dim_date.fiscal_quarter_name_fy
  FROM fct_invoice_item
    INNER JOIN dim_date ON fct_invoice_item.invoice_date = dim_date.date_actual
    LEFT JOIN dim_crm_account ON fct_invoice_item.dim_crm_account_id_invoice = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_product_detail ON fct_invoice_item.dim_product_detail_id = dim_product_detail.dim_product_detail_id
)

SELECT *
FROM invoice_detail
