{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_ping_instance', 'mart_ping_instance'),
    ('mart_arr', 'mart_arr'),
    ('dim_date', 'dim_date')
    ])
}},

user_counts AS (

  SELECT
    ping_created_date_month  AS reporting_month,
    ping_delivery_type       AS delivery_type,
    ping_deployment_type     AS deployment_type,
    SUM(instance_user_count) AS total_user_count
  FROM mart_ping_instance
  WHERE ping_created_date_month BETWEEN '2022-01-01' AND DATE_TRUNC('month', CURRENT_DATE) - 1 --arbitrary date range, exclude current month
    AND is_last_ping_of_month = TRUE
  GROUP BY 1, 2, 3

),

paid_seats AS (

  SELECT
    arr_month,
    product_delivery_type,
    product_deployment_type,
    SUM(quantity) AS seat_count
  FROM mart_arr
  WHERE arr_month BETWEEN '2022-01-01' AND DATE_TRUNC('month', CURRENT_DATE) - 1 --arbitrary date range, exclude current month
    AND (product_category = 'Base Products' --do not pull in add-ons, could cause double-counting of users
      OR product_rate_plan_name ILIKE '%enterprise agile planning%') --these seats are incremental on top of base product seats
    AND subscription_status IN ('Active', 'Cancelled')
  GROUP BY 1, 2, 3

),

counts_combined AS (

  SELECT
    user_counts.reporting_month,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy,
    user_counts.delivery_type,
    user_counts.deployment_type,
    user_counts.total_user_count,
    CASE
      WHEN paid_seats.seat_count > user_counts.total_user_count THEN user_counts.total_user_count --handle cases where more seats are sold than there are reported registered users (ex: Dedicated)
      ELSE IFNULL(paid_seats.seat_count, 0) --using seat count to define paid
    END                                AS paid_user_count,
    total_user_count - paid_user_count AS free_user_count --defining free as total-paid
  FROM user_counts
  LEFT JOIN paid_seats --LEFT join because the first several months of reported Dedicated users did not have any paid seats
    ON user_counts.reporting_month = paid_seats.arr_month
      AND user_counts.delivery_type = paid_seats.product_delivery_type
      AND user_counts.deployment_type = paid_seats.product_deployment_type
  INNER JOIN dim_date
    ON user_counts.reporting_month = dim_date.date_actual

)

SELECT *
FROM counts_combined
