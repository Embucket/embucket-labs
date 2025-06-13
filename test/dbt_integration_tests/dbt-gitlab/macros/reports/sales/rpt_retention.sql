{%- macro rpt_retention(fields) -%}

{% set fields_length = fields|length %}
{% set surrogate_key_array = [] %}

{% for field in fields %}
    {% set new_field = 'retention_subs.' + field %}
    {% do surrogate_key_array.append(new_field) %}
{% endfor %}
{% do surrogate_key_array.append('retention_subs.retention_month') %} -- This is the array that will be used to create the unique key of the table
 -- It is made out of the fields passed to the macro. We also add to it the retention_month as that is part of the grain
 -- We add the retention_subs. prefix to each field since this is the alias that will be used in the last CTE to indicate where the fields are coming.


{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('rpt_arr', 'rpt_arr_snapshot_combined')
]) }}

, finalized_arr_months AS (

    SELECT DISTINCT arr_month, is_arr_month_finalized
    FROM rpt_arr

), parent_account_mrrs AS (

    SELECT
      {% for field in fields %}
        rpt_arr.{{field}},
      {% endfor %}
      rpt_arr.is_arr_month_finalized,
      rpt_arr.arr_month,
      DATEADD('year', 1, rpt_arr.arr_month)                     AS retention_month,
      SUM(ZEROIFNULL(rpt_arr.mrr))                              AS mrr_total,
      SUM(ZEROIFNULL(rpt_arr.arr))                              AS arr_total,
      SUM(ZEROIFNULL(rpt_arr.quantity))                         AS quantity_total,
      ARRAY_AGG(rpt_arr.product_tier_name)                      AS product_category,
      MAX(rpt_arr.product_ranking)                              AS product_ranking
    FROM rpt_arr
    {{ dbt_utils.group_by(n=3 + fields_length) }}

), retention_subs AS (

    SELECT
      {% for field in fields %}
        current_mrr.{{field}},
      {% endfor %}
      future_mrr.is_arr_month_finalized AS is_arr_month_finalized,
      current_mrr.arr_month             AS current_arr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total             AS current_mrr,
      future_mrr.mrr_total              AS future_mrr,
      current_mrr.arr_total             AS current_arr,
      future_mrr.arr_total              AS future_arr,
      current_mrr.quantity_total        AS current_quantity,
      future_mrr.quantity_total         AS future_quantity,
      current_mrr.product_category      AS current_product_category,
      future_mrr.product_category       AS future_product_category,
      current_mrr.product_ranking       AS current_product_ranking,
      future_mrr.product_ranking        AS future_product_ranking
    FROM parent_account_mrrs AS current_mrr
    LEFT JOIN parent_account_mrrs AS future_mrr
    -- We use the fields array created above. Since these fields define the grain of the table, we need to join on all of them
    -- In the first iteration of the loop we use an ON condition, after we use the AND condition
      {% for field in fields %}
        {% if loop.first %} ON {% else %} AND {% endif %}
        current_mrr.{{field}} = future_mrr.{{field}}
      {% endfor %}
      AND current_mrr.retention_month = future_mrr.arr_month

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(surrogate_key_array) }}                              AS primary_key,
      {% for field in fields %}
        retention_subs.{{field}}                                                      AS {{field}},
      {% endfor %}
      dim_crm_account.crm_account_name                                                AS parent_crm_account_name,
      finalized_arr_months.is_arr_month_finalized,
      retention_subs.retention_month,
      IFF(dim_date.is_first_day_of_last_month_of_fiscal_quarter, dim_date.fiscal_quarter_name_fy, NULL)
                                                                                      AS retention_fiscal_quarter_name_fy,
      IFF(dim_date.is_first_day_of_last_month_of_fiscal_year, dim_date.fiscal_year, NULL)
                                                                                      AS retention_fiscal_year,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_sales_segment_grouped,
      dim_crm_account.parent_crm_account_geo,
      CASE
        WHEN retention_subs.current_arr > 1000000 THEN '1. ARR > $1M'
        WHEN retention_subs.current_arr > 100000 THEN '2. ARR $100K-$1M'
        WHEN retention_subs.current_arr > 5000 THEN '3. ARR $5K-100K'
        WHEN retention_subs.current_arr <= 5000 THEN '4. ARR <= $5K'
      END                                                                             AS retention_arr_band,
      retention_subs.current_arr                                                      AS prior_year_arr,
      retention_subs.current_mrr                                                      AS prior_year_mrr,
      COALESCE(retention_subs.future_arr, 0)                                          AS net_retention_arr,
      COALESCE(retention_subs.future_mrr, 0)                                          AS net_retention_mrr,
      CASE 
        WHEN net_retention_arr = 0
          THEN prior_year_arr
        ELSE 0
      END                                                                             AS churn_arr,
      CASE WHEN net_retention_arr > 0
        THEN LEAST(net_retention_arr, retention_subs.current_arr)
        ELSE 0 END                                                                    AS gross_retention_arr,
      retention_subs.current_quantity                                                 AS prior_year_quantity,
      COALESCE(retention_subs.future_quantity, 0)                                     AS net_retention_quantity,
      retention_subs.future_product_category                                          AS net_retention_product_category,
      retention_subs.current_product_category                                         AS prior_year_product_category,
      retention_subs.future_product_ranking                                           AS net_retention_product_ranking,
      retention_subs.current_product_ranking                                          AS prior_year_product_ranking
    FROM retention_subs
    INNER JOIN dim_date
      ON dim_date.date_actual = retention_subs.retention_month
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = retention_subs.dim_parent_crm_account_id
    LEFT JOIN finalized_arr_months
      ON finalized_arr_months.arr_month = retention_subs.retention_month
    WHERE retention_month <= DATE_TRUNC('month', CURRENT_DATE::DATE)

)

SELECT *
FROM final

{%- endmacro %}
