{{ simple_cte([
    ('rpt_arr_snapshot_combined','rpt_arr_snapshot_combined')
]) }},

parent_arr AS (

  SELECT
    arr_month,
    dim_parent_crm_account_id,
    SUM(arr)      AS parent_arr,
    SUM(CASE WHEN is_licensed_user= TRUE THEN quantity END) AS parent_quantity
  FROM rpt_arr_snapshot_combined
  {{ dbt_utils.group_by(n=2) }}
),

delta_changes AS (

  SELECT
    main.arr_month,
    main.dim_parent_crm_account_id,
    main.parent_arr,
    main.parent_quantity,
    (main.parent_arr - month.parent_arr)             AS parent_arr_delta_month,
    (main.parent_arr - quarter.parent_arr)           AS parent_arr_delta_quarter,
    (main.parent_arr - year.parent_arr)              AS parent_arr_delta_year,
    (main.parent_quantity - month.parent_quantity)   AS parent_quantity_delta_month,
    (main.parent_quantity - quarter.parent_quantity) AS parent_quantity_delta_quarter,
    (main.parent_quantity - year.parent_quantity)    AS parent_quantity_delta_year
  FROM parent_arr AS main
  LEFT JOIN parent_arr AS month
    ON main.dim_parent_crm_account_id = month.dim_parent_crm_account_id
      AND month.arr_month = DATEADD('month', -1, main.arr_month)
  LEFT JOIN parent_arr AS quarter
    ON main.dim_parent_crm_account_id = quarter.dim_parent_crm_account_id
      AND quarter.arr_month = DATEADD('month', -3, main.arr_month)
  LEFT JOIN parent_arr AS year
    ON main.dim_parent_crm_account_id = year.dim_parent_crm_account_id
      AND year.arr_month = DATEADD('month', -12, main.arr_month)
)


SELECT *
FROM delta_changes
