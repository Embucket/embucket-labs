{{ simple_cte([
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_date','dim_date'),
    ('dim_product_detail','dim_product_detail'),
    ('dim_subscription','dim_subscription'),
    ('fct_mrr', 'fct_mrr')
]) }}

, mart_arr AS (

    SELECT
      dim_date.date_actual                                                                          AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)               AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)                             AS fiscal_year,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      COALESCE(dim_crm_account.merged_to_account_id, dim_crm_account.dim_crm_account_id)            AS dim_crm_account_id,
      dim_subscription.subscription_name,
      dim_subscription.dim_subscription_id                                                          AS subscription_id,
      dim_product_detail.product_tier_name                                                          AS product_category,
      dim_product_detail.product_delivery_type                                                      AS delivery,
      dim_product_detail.product_ranking,
      fct_mrr.mrr,
      fct_mrr.quantity
    FROM fct_mrr
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr.dim_billing_account_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE fct_mrr.subscription_status IN ('Active', 'Cancelled')
      AND dim_crm_account.is_jihu_account != 'TRUE'

), max_min_month AS (

    SELECT
      parent_crm_account_name,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      subscription_name,
      subscription_id,
      product_category,
      delivery,
      product_ranking,
      MIN(arr_month)                      AS date_month_start,
      --add 1 month to generate churn month
      DATEADD('month',1,MAX(arr_month))   AS date_month_end
    FROM mart_arr
    {{ dbt_utils.group_by(n=8) }}

), base AS (

    SELECT
      parent_crm_account_name,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      subscription_name,
      subscription_id,
      product_category,
      delivery,
      product_ranking,
      dim_date.date_actual AS arr_month,
      dim_date.fiscal_quarter_name_fy,
      dim_date.fiscal_year
    FROM max_min_month
    INNER JOIN dim_date
      -- all months after start date
      ON  dim_date.date_actual >= max_min_month.date_month_start
      -- up to and including end date
      AND dim_date.date_actual <=  max_min_month.date_month_end
      AND day_of_month = 1

), monthly_arr_subscription_level AS (

    SELECT
      base.arr_month,
      base.parent_crm_account_name,
      base.dim_parent_crm_account_id,
      base.dim_crm_account_id,
      base.subscription_name,
      base.subscription_id,
      base.product_category,
      base.delivery,
      base.product_ranking,
      SUM(ZEROIFNULL(quantity))                                                               AS quantity,
      SUM(ZEROIFNULL(mrr)*12)                                                                 AS arr
    FROM base
    LEFT JOIN mart_arr
      ON base.arr_month = mart_arr.arr_month
      AND base.subscription_id = mart_arr.subscription_id
      AND base.product_category = mart_arr.product_category
      AND base.delivery = mart_arr.delivery
    {{ dbt_utils.group_by(n=9) }}

), prior_month AS (

    SELECT
      monthly_arr_subscription_level.*,
      COALESCE(LAG(quantity) OVER (PARTITION BY subscription_id, product_category, delivery ORDER BY arr_month),0) AS previous_quantity,
      COALESCE(LAG(arr) OVER (PARTITION BY subscription_id, product_category, delivery ORDER BY arr_month),0) AS previous_arr,
      ROW_NUMBER() OVER (PARTITION BY subscription_id, product_category, delivery ORDER BY arr_month) AS row_number
    FROM monthly_arr_subscription_level

), type_of_arr_change AS (

    SELECT
      prior_month.*,
      {{ type_of_arr_change('arr','previous_arr','row_number') }}
    FROM prior_month

), reason_for_arr_change_beg AS (

    SELECT
      arr_month,
      subscription_id,
      product_category,
      delivery,
      previous_arr      AS beg_arr,
      previous_quantity AS beg_quantity
    FROM type_of_arr_change

), reason_for_arr_change_seat_change AS (

    SELECT
      arr_month,
      subscription_id,
      product_category,
      delivery,
      {{ reason_for_arr_change_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      {{ reason_for_quantity_change_seat_change('quantity', 'previous_quantity') }}
    FROM type_of_arr_change

), reason_for_arr_change_price_change AS (

    SELECT
      arr_month,
      subscription_id,
      product_category,
      delivery,
      {{ reason_for_arr_change_price_change('product_category', 'product_category', 'quantity', 'previous_quantity', 'arr', 'previous_arr', 'product_ranking',' product_ranking') }}
    FROM type_of_arr_change

), reason_for_arr_change_end AS (

    SELECT
      arr_month,
      subscription_id,
      product_category,
      delivery,
      arr                   AS end_arr,
      quantity              AS end_quantity
    FROM type_of_arr_change

), annual_price_per_seat_change AS (

    SELECT
      arr_month,
      subscription_id,
      product_category,
      delivery,
      {{ annual_price_per_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }}
    FROM type_of_arr_change

), combined AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['type_of_arr_change.arr_month', 'type_of_arr_change.subscription_id','type_of_arr_change.product_category', 'type_of_arr_change.delivery']) }}
                                                                    AS primary_key,
      type_of_arr_change.arr_month,
      type_of_arr_change.parent_crm_account_name,
      type_of_arr_change.dim_parent_crm_account_id,
      type_of_arr_change.dim_crm_account_id,
      type_of_arr_change.subscription_name,
      type_of_arr_change.subscription_id,
      type_of_arr_change.product_category,
      type_of_arr_change.delivery,
      type_of_arr_change.product_ranking,
      type_of_arr_change.type_of_arr_change,
      reason_for_arr_change_beg.beg_arr,
      reason_for_arr_change_beg.beg_quantity,
      reason_for_arr_change_seat_change.seat_change_arr,
      reason_for_arr_change_seat_change.seat_change_quantity,
      reason_for_arr_change_price_change.price_change_arr,
      reason_for_arr_change_end.end_arr,
      reason_for_arr_change_end.end_quantity,
      annual_price_per_seat_change.annual_price_per_seat_change
    FROM type_of_arr_change
    LEFT JOIN reason_for_arr_change_beg
      ON type_of_arr_change.subscription_id = reason_for_arr_change_beg.subscription_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_beg.arr_month
      AND type_of_arr_change.product_category = reason_for_arr_change_beg.product_category
      AND type_of_arr_change.delivery = reason_for_arr_change_beg.delivery
    LEFT JOIN reason_for_arr_change_seat_change
      ON type_of_arr_change.subscription_id = reason_for_arr_change_seat_change.subscription_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_seat_change.arr_month
      AND type_of_arr_change.product_category = reason_for_arr_change_seat_change.product_category
      AND type_of_arr_change.delivery = reason_for_arr_change_seat_change.delivery
    LEFT JOIN reason_for_arr_change_price_change
      ON type_of_arr_change.subscription_id = reason_for_arr_change_price_change.subscription_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_price_change.arr_month
      AND type_of_arr_change.product_category = reason_for_arr_change_price_change.product_category
      AND type_of_arr_change.delivery = reason_for_arr_change_price_change.delivery
    LEFT JOIN reason_for_arr_change_end
      ON type_of_arr_change.subscription_id = reason_for_arr_change_end.subscription_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_end.arr_month
      AND type_of_arr_change.product_category = reason_for_arr_change_end.product_category
      AND type_of_arr_change.delivery = reason_for_arr_change_end.delivery
    LEFT JOIN annual_price_per_seat_change
      ON type_of_arr_change.subscription_id = annual_price_per_seat_change.subscription_id
      AND type_of_arr_change.arr_month = annual_price_per_seat_change.arr_month
      AND type_of_arr_change.product_category = annual_price_per_seat_change.product_category
      AND type_of_arr_change.delivery = annual_price_per_seat_change.delivery

)

SELECT *
FROM combined
