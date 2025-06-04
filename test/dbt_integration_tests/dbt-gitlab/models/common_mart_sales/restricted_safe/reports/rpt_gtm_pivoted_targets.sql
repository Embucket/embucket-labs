{{ simple_cte([
    ('dim_date','dim_date'),
    ('targets', 'fct_sales_funnel_target_daily')
]) }},

targets_dates AS (
  SELECT DISTINCT
    targets.target_date,
    targets.target_date_id,
    targets.report_target_date,
    dim_date.fiscal_quarter_name_fy,
    targets.dim_crm_user_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    targets.kpi_name,
    CONCAT(TO_VARCHAR(targets.target_date_id), TO_CHAR(targets.dim_crm_user_hierarchy_sk), TO_CHAR(targets.dim_sales_qualified_source_id), TO_CHAR(targets.dim_order_type_id)) AS key,
    targets.daily_allocated_target,
    targets.mtd_allocated_target,
    targets.qtd_allocated_target,
    targets.ytd_allocated_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = dim_date.date_id

),

pivoted_targets AS (
  SELECT
    target_date,
    target_date_id,
    report_target_date,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    "'Net ARR'"                  AS net_arr_daily_allocated_target,
    "'Deals'"                    AS deals_daily_allocated_target,
    "'New Logos'"                AS new_logos_daily_allocated_target,
    "'Stage 1 Opportunities'"    AS saos_daily_allocated_target,
    "'Net ARR Pipeline Created'" AS pipeline_created_daily_allocated_target
  FROM (
    SELECT *
    FROM targets_dates
    PIVOT (
      SUM(daily_allocated_target)
      FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
  )

),

mtd_pivoted_targets AS (
  SELECT
    target_date,
    target_date_id,
    report_target_date,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    key,
    "'Net ARR'"                  AS mtd_net_arr_target,
    "'Deals'"                    AS mtd_deals_target,
    "'New Logos'"                AS mtd_new_logos_target,
    "'Stage 1 Opportunities'"    AS mtd_sao_target,
    "'Net ARR Pipeline Created'" AS mtd_pipeline_created_target
  FROM (
    SELECT *
    FROM targets_dates
    PIVOT (
      SUM(mtd_allocated_target)
      FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
  )

),

qtd_pivoted_targets AS (
  SELECT
    target_date,
    target_date_id,
    report_target_date,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    key,
    "'Net ARR'"                  AS qtd_net_arr_target,
    "'Deals'"                    AS qtd_deals_target,
    "'New Logos'"                AS qtd_new_logos_target,
    "'Stage 1 Opportunities'"    AS qtd_saos_target,
    "'Net ARR Pipeline Created'" AS qtd_pipeline_created_target
  FROM (
    SELECT *
    FROM targets_dates
    PIVOT (
      SUM(qtd_allocated_target)
      FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
  )

),

ytd_pivoted_targets AS (
  SELECT
    target_date,
    target_date_id,
    report_target_date,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    key,
    "'Net ARR'"                  AS ytd_net_arr_target,
    "'Deals'"                    AS ytd_deals_target,
    "'New Logos'"                AS ytd_new_logos_target,
    "'Stage 1 Opportunities'"    AS ytd_saos_target,
    "'Net ARR Pipeline Created'" AS ytd_pipeline_created_target
  FROM (
    SELECT *
    FROM targets_dates
    PIVOT (
      SUM(ytd_allocated_target)
      FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
  )

),

final AS ( -- need to use the MAX function in order to deal with the nulls from the various CTE's otherwise we get four of the same key per row
  SELECT DISTINCT
    --dimensions
    pivoted_targets.target_date,
    pivoted_targets.target_date_id,
    pivoted_targets.fiscal_quarter_name_fy,
    pivoted_targets.dim_crm_user_hierarchy_sk,
    pivoted_targets.dim_sales_qualified_source_id,
    pivoted_targets.dim_order_type_id,
    -- daily allocated targets
    MAX(pivoted_targets.net_arr_daily_allocated_target)          AS net_arr_daily_allocated_target,
    MAX(pivoted_targets.deals_daily_allocated_target)            AS deals_daily_allocated_target,
    MAX(pivoted_targets.new_logos_daily_allocated_target)        AS new_logos_daily_allocated_target,
    MAX(pivoted_targets.saos_daily_allocated_target)             AS saos_daily_allocated_target,
    MAX(pivoted_targets.pipeline_created_daily_allocated_target) AS pipeline_created_daily_allocated_target,
    --mtd targets
    MAX(mtd_pivoted_targets.mtd_net_arr_target)                  AS mtd_net_arr_daily_allocated_target,
    MAX(mtd_pivoted_targets.mtd_deals_target)                    AS mtd_deals_daily_allocated_target,
    MAX(mtd_pivoted_targets.mtd_new_logos_target)                AS mtd_new_logos_daily_allocated_target,
    MAX(mtd_pivoted_targets.mtd_sao_target)                      AS mtd_saos_daily_allocated_target,
    MAX(mtd_pivoted_targets.mtd_pipeline_created_target)         AS mtd_pipeline_created_daily_allocated_target,
    --qtd targets
    MAX(qtd_pivoted_targets.qtd_net_arr_target)                  AS qtd_net_arr_daily_allocated_target,
    MAX(qtd_pivoted_targets.qtd_deals_target)                    AS qtd_deals_daily_allocated_target,
    MAX(qtd_pivoted_targets.qtd_new_logos_target)                AS qtd_new_logos_daily_allocated_target,
    MAX(qtd_pivoted_targets.qtd_saos_target)                     AS qtd_saos_daily_allocated_target,
    MAX(qtd_pivoted_targets.qtd_pipeline_created_target)         AS qtd_pipeline_created_daily_allocated_target,
    -- ytd targets
    MAX(ytd_pivoted_targets.ytd_net_arr_target)                  AS ytd_net_arr_daily_allocated_target,
    MAX(ytd_pivoted_targets.ytd_deals_target)                    AS ytd_deals_daily_allocated_target,
    MAX(ytd_pivoted_targets.ytd_new_logos_target)                AS ytd_new_logos_daily_allocated_target,
    MAX(ytd_pivoted_targets.ytd_saos_target)                     AS ytd_saos_daily_allocated_target,
    MAX(ytd_pivoted_targets.ytd_pipeline_created_target)         AS ytd_pipeline_created_daily_allocated_target
  FROM pivoted_targets
  LEFT JOIN mtd_pivoted_targets
    ON pivoted_targets.target_date = mtd_pivoted_targets.target_date
      AND pivoted_targets.dim_sales_qualified_source_id = mtd_pivoted_targets.dim_sales_qualified_source_id
      AND pivoted_targets.dim_order_type_id = mtd_pivoted_targets.dim_order_type_id
      AND pivoted_targets.dim_crm_user_hierarchy_sk = mtd_pivoted_targets.dim_crm_user_hierarchy_sk
  LEFT JOIN qtd_pivoted_targets
    ON pivoted_targets.target_date = qtd_pivoted_targets.target_date
      AND pivoted_targets.dim_sales_qualified_source_id = qtd_pivoted_targets.dim_sales_qualified_source_id
      AND pivoted_targets.dim_order_type_id = qtd_pivoted_targets.dim_order_type_id
      AND pivoted_targets.dim_crm_user_hierarchy_sk = qtd_pivoted_targets.dim_crm_user_hierarchy_sk
  LEFT JOIN ytd_pivoted_targets
    ON pivoted_targets.target_date = ytd_pivoted_targets.target_date
      AND pivoted_targets.dim_sales_qualified_source_id = ytd_pivoted_targets.dim_sales_qualified_source_id
      AND pivoted_targets.dim_order_type_id = ytd_pivoted_targets.dim_order_type_id
      AND pivoted_targets.dim_crm_user_hierarchy_sk = ytd_pivoted_targets.dim_crm_user_hierarchy_sk
  GROUP BY ALL
)

SELECT *
FROM final
