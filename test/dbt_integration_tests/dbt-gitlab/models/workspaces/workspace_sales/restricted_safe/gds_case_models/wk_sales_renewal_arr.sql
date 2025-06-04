{{ simple_cte([
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_arr_snapshot_model', 'mart_arr_snapshot_model')
    ])

}},

previous_data AS (
  SELECT
    dim_crm_account_id,
    DATE_TRUNC('month', snapshot_date) AS snapshot_month,
    arr_month,
    dim_subscription_id,
    product_rate_plan_name,
    SUM(arr)                           AS arr,
    SUM(quantity)                      AS quantity
  FROM
    mart_arr_snapshot_model
  WHERE
    product_rate_plan_name LIKE ANY ('%Premium%', '%Ultimate%', '%Silver%', '%Gold%', '%Starter%', '%Bronze%')
    AND
    arr_month = DATE_TRUNC('month', snapshot_date)
    AND snapshot_date = DATE_TRUNC('month', snapshot_date)
    AND snapshot_date >= '2021-01-01'
  GROUP BY 1, 2, 3, 4, 5
),

output AS (
  SELECT
    mart_crm_opportunity.dim_crm_account_id,
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.subscription_type,
    mart_crm_opportunity.order_type,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.close_month,
    current_data.arr_month,
    ROW_NUMBER() OVER (PARTITION BY mart_crm_opportunity.dim_crm_account_id, mart_crm_opportunity.dim_crm_opportunity_id ORDER BY current_data.arr DESC) AS arr_rank,
    current_data.arr                                                                                                                                     AS total_new_arr,
    previous_data.arr                                                                                                                                    AS total_previous_arr,
    previous_data.quantity                                                                                                                               AS total_previous_quantity,
    current_data.quantity                                                                                                                                AS total_new_quantity,
    total_previous_arr / total_previous_quantity                                                                                                         AS total_previous_price,
    total_new_arr / total_new_quantity                                                                                                                   AS total_new_price
    ,
    (total_new_price - total_previous_price) * total_previous_quantity                                                                                   AS price_increase_arr,
    (total_new_quantity - total_previous_quantity) * total_new_price                                                                                     AS quantity_increase_arr
    ,
    current_data.product_rate_plan_name                                                                                                                  AS new_product,
    previous_data.product_rate_plan_name                                                                                                                 AS previous_product

  FROM mart_crm_opportunity
  LEFT JOIN previous_data
    ON mart_crm_opportunity.dim_crm_account_id = previous_data.dim_crm_account_id
      AND
      (
        (
          mart_crm_opportunity.close_month < DATE_TRUNC('month', CURRENT_DATE)
          AND ((
            mart_crm_opportunity.order_type NOT LIKE '%Churn%'
            AND previous_data.arr_month = DATEADD('month', -1, mart_crm_opportunity.close_month)
          )
          OR
          (
            mart_crm_opportunity.order_type LIKE '%Churn%'
            AND previous_data.arr_month = DATEADD('month', -3, mart_crm_opportunity.close_month)
          )
          )
        )
        OR
        (
          mart_crm_opportunity.close_month >= DATE_TRUNC('month', CURRENT_DATE)
          AND previous_data.arr_month = DATE_TRUNC('month', CURRENT_DATE)
        )
      )
  LEFT JOIN previous_data AS current_data
    ON mart_crm_opportunity.dim_crm_account_id = current_data.dim_crm_account_id
      AND current_data.arr_month = DATEADD('month', 1, mart_crm_opportunity.close_month)
  WHERE mart_crm_opportunity.subscription_type = 'Renewal'
    AND mart_crm_opportunity.close_month >= '2022-02-01'
    AND mart_crm_opportunity.close_month < DATE_TRUNC('month', CURRENT_DATE)
    AND mart_crm_opportunity.arr_basis_for_clari > 0
  QUALIFY (arr_rank = 1 OR arr_rank IS NULL)
)

{{ dbt_audit(
    cte_ref="output",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2024-07-15"
) }}
