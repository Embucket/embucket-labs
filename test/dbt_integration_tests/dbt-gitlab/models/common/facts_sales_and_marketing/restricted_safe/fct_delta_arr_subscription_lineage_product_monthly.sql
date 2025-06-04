{{ simple_cte([
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_date','dim_date'),
    ('dim_product_detail','dim_product_detail'),
    ('dim_subscription','dim_subscription'),
    ('fct_mrr', 'fct_mrr')
]) }}

, data_quality_filter_subscription_slugify AS (
    
    /*
    There is a data quality issue where a subscription_name_slugify can be mapped to more than one subscription_name. 
    There are 5 subscription_name_slugifys and 10 subscription_names that this impacts as of 2023-02-20. This CTE is 
    used to filter out these subscriptions from the model. The data quality issue causes a fanout with the subscription 
    lineages that are used to group on in the data model.
    */

    SELECT 
      subscription_name_slugify,
      COUNT(subscription_name) AS nbr_records
    FROM dim_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')
    GROUP BY 1
    HAVING nbr_records > 1

)

, oldest_subscription_in_cohort AS (

    /*
    This CTE fetches the subscription id, crm account id, and subscription lineage for the oldest subscription in the lineage a given subscription is a part of. 
    This information is used to group linked subscriptions together in the model and provide a connected view of delta arr changes in a subscription lineage.
    */

    SELECT 
      sub.dim_subscription_id,
      sub_w_org_id.dim_subscription_id AS dim_oldest_subscription_in_cohort_id,
      sub_w_org_id.dim_crm_account_id AS dim_oldest_crm_account_in_cohort_id,
      sub_w_org_id.subscription_lineage,
      sub_w_org_id.subscription_cohort_month AS oldest_subscription_cohort_month
    FROM dim_subscription sub
    LEFT JOIN dim_subscription sub_w_org_id
      ON sub.oldest_subscription_in_cohort = sub_w_org_id.subscription_name_slugify
    WHERE sub.subscription_status IN ('Active', 'Cancelled')
      AND sub_w_org_id.subscription_status IN ('Active', 'Cancelled')
      AND sub.subscription_name_slugify NOT IN (SELECT subscription_name_slugify FROM data_quality_filter_subscription_slugify)
      AND sub_w_org_id.subscription_name_slugify NOT IN (SELECT subscription_name_slugify FROM data_quality_filter_subscription_slugify)

)

, mart_arr AS (

    SELECT
      dim_date.date_actual                                                                          AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)               AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)                             AS fiscal_year,
      dim_crm_account.dim_parent_crm_account_id,
      oldest_subscription_in_cohort.subscription_lineage,
      oldest_subscription_in_cohort.dim_oldest_subscription_in_cohort_id,
      dim_product_detail.product_tier_name,
      dim_product_detail.product_delivery_type,
      dim_product_detail.product_ranking,
      fct_mrr.mrr,
      fct_mrr.quantity
    FROM fct_mrr
    INNER JOIN oldest_subscription_in_cohort
      ON oldest_subscription_in_cohort.dim_subscription_id = fct_mrr.dim_subscription_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id    
    LEFT JOIN dim_crm_account
      ON oldest_subscription_in_cohort.dim_oldest_crm_account_in_cohort_id = dim_crm_account.dim_crm_account_id
    WHERE fct_mrr.subscription_status IN ('Active', 'Cancelled')
      AND dim_crm_account.is_jihu_account != 'TRUE'
      --Filter out storage charges. This fact model focuses on the tier plans.
      AND LOWER(dim_product_detail.product_tier_name) != 'storage' 

)

, max_min_month AS (

    SELECT
      dim_parent_crm_account_id,
      subscription_lineage,
      dim_oldest_subscription_in_cohort_id,
      MIN(arr_month)                      AS date_month_start,
      --add 1 month to generate churn month
      DATEADD('month',1,MAX(arr_month))   AS date_month_end
    FROM mart_arr
    {{ dbt_utils.group_by(n=3) }}

)

, base AS (

    SELECT
      dim_parent_crm_account_id,
      subscription_lineage,
      dim_oldest_subscription_in_cohort_id,
      dim_date.date_id AS dim_date_month_id,
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

)

, monthly_arr_subscription_level AS (

    SELECT
      base.dim_date_month_id,
      base.arr_month,
      base.dim_parent_crm_account_id,
      base.subscription_lineage,
      base.dim_oldest_subscription_in_cohort_id,
      ARRAY_AGG(DISTINCT mart_arr.product_tier_name ) WITHIN GROUP (ORDER BY mart_arr.product_tier_name  ASC) AS product_tier_name,
      ARRAY_AGG(DISTINCT mart_arr.product_delivery_type) WITHIN GROUP (ORDER BY mart_arr.product_delivery_type ASC) AS product_delivery_type,
      MAX(mart_arr.product_ranking) AS product_ranking,
      SUM(ZEROIFNULL(mart_arr.quantity)) AS quantity,
      SUM(ZEROIFNULL(mart_arr.mrr)*12) AS arr
    FROM base
    LEFT JOIN mart_arr
      ON base.arr_month = mart_arr.arr_month
      AND base.subscription_lineage = mart_arr.subscription_lineage
    {{ dbt_utils.group_by(n=5) }}

)

, prior_month AS (

    SELECT
      monthly_arr_subscription_level.*,
      LAG(product_tier_name ) OVER (PARTITION BY subscription_lineage ORDER BY arr_month) AS previous_product_tier_name,
      LAG(product_delivery_type) OVER (PARTITION BY subscription_lineage ORDER BY arr_month) AS previous_product_delivery_type,
      COALESCE(LAG(product_ranking) OVER (PARTITION BY subscription_lineage ORDER BY arr_month),0) AS previous_product_ranking,
      COALESCE(LAG(quantity) OVER (PARTITION BY subscription_lineage ORDER BY arr_month),0) AS previous_quantity,
      COALESCE(LAG(arr) OVER (PARTITION BY subscription_lineage ORDER BY arr_month),0) AS previous_arr,
      ROW_NUMBER() OVER (PARTITION BY subscription_lineage ORDER BY arr_month) AS row_number
    FROM monthly_arr_subscription_level

)

, type_of_arr_change_cte AS (

    SELECT DISTINCT
      {{ dbt_utils.generate_surrogate_key(['arr_month', 'dim_oldest_subscription_in_cohort_id']) }}
                                                                    AS delta_arr_subscription_lineage_product_monthly_pk,
      prior_month.*,
      {{ type_of_arr_change('arr','previous_arr','row_number') }},
      previous_arr      AS beg_arr,
      previous_quantity AS beg_quantity,
      {{ reason_for_arr_change_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      {{ reason_for_quantity_change_seat_change('quantity', 'previous_quantity') }},
      {{ reason_for_arr_change_price_change('product_tier_name ', 'previous_product_tier_name ', 'quantity', 'previous_quantity', 'arr', 'previous_arr', 'product_ranking',' previous_product_ranking') }},
      {{ reason_for_arr_change_tier_change('product_ranking', 'previous_product_ranking', 'quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      {{ annual_price_per_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      arr                   AS end_arr,
      quantity              AS end_quantity
    FROM prior_month

)

, final AS (

    SELECT
      --primary key
      delta_arr_subscription_lineage_product_monthly_pk,

      --foreign keys
      dim_date_month_id,
      dim_parent_crm_account_id,
      dim_oldest_subscription_in_cohort_id,

      --degenerate dimensions 
      product_tier_name,
      previous_product_tier_name,
      product_delivery_type,
      previous_product_delivery_type,
      product_ranking,
      previous_product_ranking,
      type_of_arr_change,

      --facts
      beg_arr,
      seat_change_arr,
      tier_change_arr,
      price_change_arr,
      end_arr,
      annual_price_per_seat_change,
      beg_quantity,
      seat_change_quantity,
      end_quantity
    FROM type_of_arr_change_cte

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2023-02-22",
    updated_date="2023-02-22"
) }}
