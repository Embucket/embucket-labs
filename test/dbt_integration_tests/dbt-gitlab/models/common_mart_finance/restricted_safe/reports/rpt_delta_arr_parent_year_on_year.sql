{{ simple_cte([
    ('dim_date','dim_date'),
    ('dim_crm_account_daily_snapshot', 'dim_crm_account_daily_snapshot'),
    ('dim_crm_account', 'dim_crm_account')
]) }},

rpt_arr AS (
    -- exclude charges billed by the minute https://gitlab.com/gitlab-data/analytics/-/issues/incident/22849
    SELECT *
    FROM {{ ref('rpt_arr_snapshot_combined')}}
    WHERE IFNULL(product_rate_plan_charge_name, 'not_applicable') NOT LIKE ('Hosted Runners%')

),

dim_crm_account_live AS (

  SELECT
    dim_crm_account_id                       AS dim_crm_account_id_live,
    crm_account_name                         AS parent_crm_account_name_live,
    parent_crm_account_sales_segment         AS parent_crm_account_sales_segment_live,
    parent_crm_account_sales_segment_grouped AS parent_crm_account_sales_segment_grouped_live,
    parent_crm_account_geo                   AS parent_crm_account_geo_live
  FROM dim_crm_account

),

finalized_arr_months AS (

  SELECT DISTINCT
    arr_month,
    is_arr_month_finalized
  FROM rpt_arr

),

child_account_arrs AS (

  SELECT
    dim_crm_account_id                  AS child_account_id,
    dim_parent_crm_account_id           AS parent_account_id,
    arr_month,
    product_tier_name                   AS product_category,
    COALESCE(
      product_ranking,
      CASE
        WHEN product_tier_name LIKE '%Ultimate%' THEN 3
        WHEN product_tier_name IN (
            'SaaS - Silver',
            'Self-Managed - Premium',
            'SaaS - Premium'
        ) THEN 2
        WHEN product_tier_name IN ('SaaS - Bronze', 'Self-Managed - Starter') THEN 1
        ELSE 0
      END)                              AS product_ranking,
    is_arpu,
    MIN(parent_account_cohort_month)    AS parent_account_cohort_month,
    SUM(ZEROIFNULL(mrr))                AS mrr,
    SUM(ZEROIFNULL(arr))                AS arr,
    SUM(ZEROIFNULL(quantity))           AS quantity
  FROM rpt_arr
  WHERE mrr != 0
  {{ dbt_utils.group_by(n=6) }}

),

py_arr_with_cy_parent AS (

  SELECT
    child_account_arrs.arr_month                             AS py_arr_month,
    DATEADD('year', 1, child_account_arrs.arr_month)         AS delta_arr_month,
    dim_crm_account_daily_snapshot.dim_parent_crm_account_id AS parent_account_id_in_delta_arr_month,
    DATEADD('year', 1, dim_date.snapshot_date_fpa)           AS delta_arr_period_snapshot_date,
    MIN(is_arpu)                                             AS is_arpu,
    MIN(child_account_arrs.parent_account_cohort_month)      AS parent_account_cohort_month,
    ARRAY_AGG(product_category)                              AS py_product_category,
    MAX(product_ranking)                                     AS py_product_ranking,
    SUM(ZEROIFNULL(child_account_arrs.mrr))                  AS py_mrr,
    SUM(ZEROIFNULL(child_account_arrs.arr))                  AS py_arr,
    SUM(ZEROIFNULL(child_account_arrs.quantity))             AS py_quantity
  FROM child_account_arrs
  LEFT JOIN dim_date
    ON child_account_arrs.arr_month::DATE = dim_date.date_day
  LEFT JOIN dim_crm_account_daily_snapshot
    ON child_account_arrs.child_account_id = dim_crm_account_daily_snapshot.dim_crm_account_id
    AND dim_crm_account_daily_snapshot.snapshot_date = CASE WHEN delta_arr_month < '2024-03-01'
                                                            THEN DATEADD('year', 1, snapshot_date_fpa)
                                                            ELSE DATEADD('year', 1, snapshot_date_fpa_fifth) 
                                                            END
  -- join logic for the switch between 8th and 5th day snapshot from 2024-03-01
  {{ dbt_utils.group_by(n=4) }}

),

cy_arr_with_cy_parent AS (

  SELECT
    parent_account_id,
    arr_month,
    MIN(is_arpu)                AS is_arpu,
    SUM(ZEROIFNULL(mrr))        AS retained_mrr,
    SUM(ZEROIFNULL(arr))        AS retained_arr,
    SUM(ZEROIFNULL(quantity))   AS retained_quantity,
    ARRAY_AGG(product_category) AS retained_product_category,
    MAX(product_ranking)        AS retained_product_ranking
  FROM child_account_arrs
  {{ dbt_utils.group_by(n=2) }}

),

final_arr AS (

  SELECT
    py_arr_with_cy_parent.parent_account_id_in_delta_arr_month                                                                                                                                 AS dim_parent_crm_account_id_2,
    COALESCE(py_arr_with_cy_parent.parent_account_id_in_delta_arr_month, cy_arr_with_cy_parent.parent_account_id)                                                                              AS dim_parent_crm_account_id,
    py_arr_with_cy_parent.parent_account_cohort_month,
    finalized_arr_months.is_arr_month_finalized,
    COALESCE(py_arr_with_cy_parent.delta_arr_month, cy_arr_with_cy_parent.arr_month)                                                                                                           AS delta_arr_month,
    IFF(dim_date.is_first_day_of_last_month_of_fiscal_quarter, dim_date.fiscal_quarter_name_fy, NULL)                                                                                          AS fiscal_quarter_name_fy,
    IFF(dim_date.is_first_day_of_last_month_of_fiscal_year, dim_date.fiscal_year, NULL)                                                                                                        AS fiscal_year,
    dim_crm_account_live.parent_crm_account_name_live,
    dim_crm_account_live.parent_crm_account_sales_segment_live,
    dim_crm_account_live.parent_crm_account_sales_segment_grouped_live,
    dim_crm_account_live.parent_crm_account_geo_live,
    cy_arr_with_cy_parent.retained_product_category                                                                                                                                            AS current_year_product_category,
    py_arr_with_cy_parent.py_product_category                                                                                                                                                  AS prior_year_product_category,
    cy_arr_with_cy_parent.retained_product_ranking                                                                                                                                             AS current_year_product_ranking,
    py_arr_with_cy_parent.py_product_ranking                                                                                                                                                   AS prior_year_product_ranking,
    CASE
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) > 100000 THEN '1. ARR > $100K'
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) > 5000 THEN '2. ARR $5K-100K'
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) <= 5000 THEN '3. ARR <= $5K'
    END                                                                                                                                                                                        AS prior_year_arr_band,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_mrr))                                                                                                                                              AS prior_year_mrr,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_mrr))                                                                                                                                        AS current_year_mrr,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr))                                                                                                                                              AS prior_year_arr,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr))                                                                                                                                        AS current_year_arr,
    CASE
      WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) > 100000 THEN '1. ARR > $100K'
      WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) > 5000 THEN '2. ARR $5K-100K'
      WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) <= 5000 THEN '3. ARR <= $5K'
    END                                                                                                                                                                                        AS current_year_arr_band,
    CASE
      WHEN py_arr_with_cy_parent.parent_account_id_in_delta_arr_month IS NULL
        THEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr))
      ELSE 0
    END                                                                                                                                                                                        AS new_arr,
    CASE
      WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) = 0 AND (SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) != 0)
        THEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) * -1
      ELSE 0
    END                                                                                                                                                                                        AS churn_arr,
    CASE WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) > 0
        THEN LEAST(SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)), SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)))
      ELSE 0
    END                                                                                                                                                                                        AS gross_retention_arr,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_quantity))                                                                                                                                         AS prior_year_quantity,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_quantity))                                                                                                                                   AS current_year_quantity

  FROM py_arr_with_cy_parent
  FULL OUTER JOIN cy_arr_with_cy_parent
    ON py_arr_with_cy_parent.parent_account_id_in_delta_arr_month = cy_arr_with_cy_parent.parent_account_id
      AND py_arr_with_cy_parent.delta_arr_month = cy_arr_with_cy_parent.arr_month
  LEFT JOIN finalized_arr_months
    ON py_arr_with_cy_parent.delta_arr_month = finalized_arr_months.arr_month
  INNER JOIN dim_date
    ON dim_date.date_actual = COALESCE(py_arr_with_cy_parent.delta_arr_month, cy_arr_with_cy_parent.arr_month)
  LEFT JOIN dim_crm_account_live
    ON dim_crm_account_live.dim_crm_account_id_live = COALESCE(py_arr_with_cy_parent.parent_account_id_in_delta_arr_month, cy_arr_with_cy_parent.parent_account_id)

  GROUP BY ALL

),

final_with_delta AS (
  SELECT
    final_arr.*,

    CASE
      WHEN py_arr_with_cy_parent.parent_account_id_in_delta_arr_month IS NULL THEN 'New'
      WHEN current_year_arr = 0
        AND ABS(prior_year_arr) > 0 THEN 'Churn'
      WHEN current_year_arr < prior_year_arr
        AND current_year_arr > 0 THEN 'Contraction'
      WHEN current_year_arr > prior_year_arr THEN 'Expansion'
      WHEN current_year_arr = prior_year_arr THEN 'No Impact'
    END AS type_of_arr_change,
    CASE
      WHEN
        type_of_arr_change IN ('Expansion', 'Contraction')
        AND prior_year_quantity != current_year_quantity
        AND prior_year_quantity > 0
        THEN ZEROIFNULL(
            prior_year_arr / NULLIF(prior_year_quantity, 0) * (current_year_quantity - prior_year_quantity)
          )
      WHEN prior_year_quantity != current_year_quantity
        AND prior_year_quantity = 0 THEN current_year_arr
      ELSE 0
    END AS seat_change_arr,
    CASE
      WHEN
        prior_year_product_category = current_year_product_category
        THEN current_year_quantity * (
            current_year_arr / NULLIF(current_year_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
          )
      WHEN
        prior_year_product_category != current_year_product_category
        AND prior_year_product_ranking = current_year_product_ranking
        THEN current_year_quantity * (
            current_year_arr / NULLIF(current_year_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
          )
      ELSE 0
    END AS price_change_arr,
    CASE
      WHEN
        prior_year_product_ranking != current_year_product_ranking
        THEN ZEROIFNULL(
            current_year_quantity * (
              current_year_arr / NULLIF(current_year_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
            )
          )
      ELSE 0
    END AS tier_change_arr,
    (
      CASE
        WHEN
          prior_year_product_ranking < current_year_product_ranking
          THEN ZEROIFNULL(
              current_year_quantity * (
                current_year_arr / NULLIF(current_year_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
              )
            )
        ELSE 0
      END
    )   AS uptier_change_arr,
    (
      CASE
        WHEN
          prior_year_product_ranking > current_year_product_ranking
          THEN ZEROIFNULL(
              current_year_quantity * (
                current_year_arr / NULLIF(current_year_quantity, 0) - prior_year_quantity / NULLIF(prior_year_quantity, 0)
              )
            )
        ELSE 0
      END
    )   AS downtier_change_arr
  FROM final_arr
  LEFT JOIN py_arr_with_cy_parent
    ON final_arr.dim_parent_crm_account_id = py_arr_with_cy_parent.parent_account_id_in_delta_arr_month
      AND final_arr.delta_arr_month = py_arr_with_cy_parent.delta_arr_month
)

SELECT
  dim_parent_crm_account_id,
  is_arr_month_finalized,
  delta_arr_month,
  fiscal_quarter_name_fy,
  fiscal_year,
  parent_crm_account_name_live,
  parent_crm_account_sales_segment_live,
  parent_crm_account_sales_segment_grouped_live,
  parent_crm_account_geo_live,
  parent_account_cohort_month,
  current_year_product_category,
  prior_year_product_category,
  current_year_product_ranking,
  prior_year_product_ranking,
  prior_year_arr_band,
  prior_year_mrr,
  current_year_mrr,
  prior_year_arr,
  current_year_arr,
  current_year_arr_band,
  new_arr,
  churn_arr,
  gross_retention_arr,
  prior_year_quantity,
  current_year_quantity,
  type_of_arr_change,
  CASE WHEN type_of_arr_change IN ('Expansion','Contraction') THEN seat_change_arr ELSE 0 END AS seat_change_arr,
  CASE WHEN type_of_arr_change IN ('Expansion','Contraction') THEN price_change_arr ELSE 0 END AS price_change_arr,
  CASE WHEN type_of_arr_change IN ('Expansion','Contraction') THEN tier_change_arr ELSE 0 END AS tier_change_arr,
  CASE WHEN type_of_arr_change IN ('Expansion','Contraction') THEN uptier_change_arr ELSE 0 END AS uptier_change_arr,
  CASE WHEN type_of_arr_change IN ('Expansion','Contraction') THEN downtier_change_arr ELSE 0 END AS downtier_change_arr
FROM final_with_delta
