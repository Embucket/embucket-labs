WITH dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}

), dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}
    WHERE subscription_status IN ('Active', 'Cancelled')

), next_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id,
      product_tier_name                                                                                               AS product_category,
      MIN(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id, product_tier_name)    AS next_renewal_month_product
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = fct_mrr.dim_crm_account_id
    INNER JOIN dim_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    LEFT JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
      AND subscription_end_month <= DATEADD('year', 1, date_actual)
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    WHERE subscription_end_month >= DATE_TRUNC('month',CURRENT_DATE)

), last_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id,
      product_tier_name                                                                                               AS product_category,
      MAX(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id, product_tier_name)    AS last_renewal_month_product
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = fct_mrr.dim_crm_account_id
    INNER JOIN dim_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    LEFT JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
      AND subscription_end_month <= DATEADD('year', 1, date_actual)
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    WHERE subscription_end_month < DATE_TRUNC('month',CURRENT_DATE)

), parent_account_mrrs AS (

    SELECT
      dim_crm_account.dim_parent_crm_account_id,
      dim_product_detail.product_tier_name              AS product_category,
      dim_product_detail.product_ranking,
      dim_date.date_actual                              AS mrr_month,
      dateadd('year', 1, date_actual)                   AS retention_month,
      next_renewal_month_product,
      last_renewal_month_product,
      SUM(ZEROIFNULL(mrr))                              AS mrr_total,
      SUM(ZEROIFNULL(arr))                              AS arr_total,
      SUM(ZEROIFNULL(quantity))                         AS quantity_total
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = fct_mrr.dim_crm_account_id
    LEFT JOIN next_renewal_month
      ON next_renewal_month.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id
      AND next_renewal_month.product_category = dim_product_detail.product_tier_name
    LEFT JOIN last_renewal_month
      ON last_renewal_month.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id
      AND last_renewal_month.product_category = dim_product_detail.product_tier_name
    WHERE dim_crm_account.is_jihu_account != 'TRUE'
    {{ dbt_utils.group_by(n=7) }}

), retention_subs AS (

    SELECT
      current_mrr.dim_parent_crm_account_id,
      current_mrr.product_category,
      current_mrr.product_ranking,
      current_mrr.mrr_month          AS current_mrr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total          AS current_mrr,
      future_mrr.mrr_total           AS future_mrr,
      current_mrr.arr_total          AS current_arr,
      future_mrr.arr_total           AS future_arr,
      current_mrr.quantity_total     AS current_quantity,
      future_mrr.quantity_total      AS future_quantity,
      current_mrr.last_renewal_month_product,
      current_mrr.next_renewal_month_product,
      --The type of arr change requires a row_number. Row_number = 1 indicates new in the macro; however, for retention, new is not a valid option since retention starts in month 12, well after the First Order transaction.
      2                              AS row_number
    FROM parent_account_mrrs AS current_mrr
    LEFT JOIN parent_account_mrrs AS future_mrr
      ON current_mrr.dim_parent_crm_account_id = future_mrr.dim_parent_crm_account_id
      AND current_mrr.product_category = future_mrr.product_category
      AND current_mrr.retention_month = future_mrr.mrr_month

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['retention_subs.dim_parent_crm_account_id', 'product_category', 'retention_month']) }} AS primary_key,
      retention_subs.dim_parent_crm_account_id,
      dim_crm_account.crm_account_name          AS parent_crm_account_name,
      product_category,
      product_ranking,
      retention_month,
      dim_date.fiscal_year                      AS retention_fiscal_year,
      dim_date.fiscal_quarter                   AS retention_fiscal_quarter,
      retention_subs.last_renewal_month_product,
      retention_subs.next_renewal_month_product,
      current_mrr                               AS prior_year_mrr,
      COALESCE(future_mrr, 0)                   AS net_retention_mrr,
      CASE WHEN net_retention_mrr > 0
        THEN least(net_retention_mrr, current_mrr)
        ELSE 0 END                              AS gross_retention_mrr,
      current_arr                               AS prior_year_arr,
      COALESCE(future_arr, 0)                   AS net_retention_arr,
      CASE WHEN net_retention_arr > 0
        THEN least(net_retention_arr, current_arr)
        ELSE 0 END                              AS gross_retention_arr,
      current_quantity                          AS prior_year_quantity,
      COALESCE(future_quantity, 0)              AS net_retention_quantity,
      {{ reason_for_quantity_change_seat_change('net_retention_quantity', 'prior_year_quantity') }},
      {{ type_of_arr_change('net_retention_arr', 'prior_year_arr','row_number') }},
      {{ reason_for_arr_change_seat_change('net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr') }},
      {{ reason_for_arr_change_price_change('product_category', 'product_category', 'net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr', 'product_ranking','product_ranking') }},
      {{ annual_price_per_seat_change('net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr') }}
    FROM retention_subs
    INNER JOIN dim_date
      ON dim_date.date_actual = retention_subs.retention_month
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = retention_subs.dim_parent_crm_account_id
    WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)

)

SELECT *
FROM final
