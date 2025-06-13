{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_subscription', 'dim_subscription')
]) }}

, fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}
    WHERE subscription_status IN ('Active', 'Cancelled')

), next_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_crm_account_id,
      MIN(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_crm_account_id)    AS next_renewal_month
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
    WHERE subscription_end_month >= DATE_TRUNC('month',CURRENT_DATE)

), last_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_crm_account_id,
      MAX(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_crm_account_id)    AS last_renewal_month
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
    WHERE subscription_end_month < DATE_TRUNC('month',CURRENT_DATE)

), crm_account_mrrs AS (

    SELECT
      dim_crm_account.dim_crm_account_id,
      dim_date.date_actual                                      AS mrr_month,
      dateadd('year', 1, date_actual)                           AS retention_month,
      next_renewal_month,
      last_renewal_month,
      COUNT(DISTINCT dim_crm_account.dim_crm_account_id)
                                                                AS crm_customer_count,
      SUM(ZEROIFNULL(mrr))                                      AS mrr_total,
      SUM(ZEROIFNULL(arr))                                      AS arr_total,
      SUM(ZEROIFNULL(quantity))                                 AS quantity_total,
      ARRAY_AGG(product_tier_name)                              AS product_category,
      MAX(product_ranking)                                      AS product_ranking
    FROM fct_mrr
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = fct_mrr.dim_crm_account_id
    LEFT JOIN next_renewal_month
      ON next_renewal_month.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN last_renewal_month
      ON last_renewal_month.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE is_jihu_account = FALSE
    {{ dbt_utils.group_by(n=5) }}

), retention_subs AS (

    SELECT
      current_mrr.dim_crm_account_id,
      current_mrr.mrr_month             AS current_mrr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total             AS current_mrr,
      future_mrr.mrr_total              AS future_mrr,
      current_mrr.arr_total             AS current_arr,
      future_mrr.arr_total              AS future_arr,
      current_mrr.crm_customer_count    AS current_crm_customer_count,
      future_mrr.crm_customer_count     AS future_crm_customer_count,
      current_mrr.quantity_total        AS current_quantity,
      future_mrr.quantity_total         AS future_quantity,
      current_mrr.product_category      AS current_product_category,
      future_mrr.product_category       AS future_product_category,
      current_mrr.product_ranking       AS current_product_ranking,
      future_mrr.product_ranking        AS future_product_ranking,
      current_mrr.last_renewal_month,
      current_mrr.next_renewal_month,
      --The type of arr change requires a row_number. Row_number = 1 indicates new in the macro; however, for retention, new is not a valid option since retention starts in month 12, well after the First Order transaction.
      2                              AS row_number
    FROM crm_account_mrrs AS current_mrr
    LEFT JOIN crm_account_mrrs AS future_mrr
      ON current_mrr.dim_crm_account_id = future_mrr.dim_crm_account_id
        AND current_mrr.retention_month = future_mrr.mrr_month

), final AS (

    SELECT
    {{ dbt_utils.generate_surrogate_key(['retention_subs.dim_crm_account_id','retention_month']) }}
                                                AS fct_retention_id,
      retention_subs.dim_crm_account_id         AS dim_crm_account_id,
      dim_crm_account.crm_account_name         AS crm_account_name,
      retention_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS retention_fiscal_quarter,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS retention_fiscal_year,
      retention_subs.last_renewal_month,
      retention_subs.next_renewal_month,
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
      current_crm_customer_count                AS prior_year_crm_customer_count,
      COALESCE(future_crm_customer_count, 0)    AS net_retention_crm_customer_count,
      {{ reason_for_quantity_change_seat_change('net_retention_quantity', 'prior_year_quantity') }},
      future_product_category                   AS net_retention_product_category,
      current_product_category                  AS prior_year_product_category,
      future_product_ranking                    AS net_retention_product_ranking,
      current_product_ranking                   AS prior_year_product_ranking,
      {{ type_of_arr_change('net_retention_arr', 'prior_year_arr','row_number') }},
      {{ reason_for_arr_change_seat_change('net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr') }},
      {{ reason_for_arr_change_price_change('net_retention_product_category', 'prior_year_product_category', 'net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr', 'net_retention_product_ranking','prior_year_product_ranking') }},
      {{ reason_for_arr_change_tier_change('net_retention_product_ranking', 'prior_year_product_ranking', 'net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr') }},
      {{ annual_price_per_seat_change('net_retention_quantity', 'prior_year_quantity', 'net_retention_arr', 'prior_year_arr') }}
    FROM retention_subs
    INNER JOIN dim_date
      ON dim_date.date_actual = retention_subs.retention_month
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = retention_subs.dim_crm_account_id
    WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@lisvinueza",
    created_date="2021-10-22",
    updated_date="2022-08-11"
) }}
