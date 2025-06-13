WITH prep_crm_user_daily_snapshot AS (

  SELECT
    dim_crm_user_id,
    employee_number,
    user_role_name,
    user_email,
    user_name,
    title,
    department,
    team,
    manager_id,
    manager_name,
    is_active,
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_region,
    crm_user_area,
    user_role_level_1,
    user_role_level_2,
    user_role_level_3,
    user_role_level_4,
    snapshot_date
  FROM {{ ref('prep_crm_user_daily_snapshot') }}

),

prep_crm_user_snapshot_base AS (

  SELECT
    dim_crm_user_id,
    employee_number,
    user_role_name,
    user_email,
    user_name,
    title,
    department,
    team,
    manager_id,
    manager_name,
    is_active,
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_region,
    crm_user_area,
    snapshot_date
  FROM prep_crm_user_daily_snapshot
  WHERE snapshot_date >= '2022-10-11' --since this date we are observing improved data quality in terms of associated employee_id with the record

),

prep_team_member_base AS (

  SELECT
    employee_id,
    first_name,
    last_name
  FROM {{ ref('prep_team_member') }}
  WHERE is_current

),

prep_date AS (

  SELECT
    date_id,
    date_actual,
    year_actual,
    current_fiscal_year,
    fiscal_year
  FROM {{ ref('prep_date') }}

),

sales_dev_opps AS (

  SELECT DISTINCT
    dim_crm_account_id,
    dim_crm_opportunity_id,
    created_date.date_actual                                AS opp_created_date,
    is_sao,
    net_arr,
    sales_accepted_date.date_actual                         AS sales_accepted_date,
    stage_1_discovery_date.date_actual                      AS stage_1_discovery_date,
    order_type,
    is_net_arr_closed_deal,
    COALESCE(crm_business_dev_rep_id, crm_sales_dev_rep_id) AS sdr_bdr_user_id
  FROM {{ ref('prep_crm_opportunity') }} AS prep_crm_opportunity
  LEFT JOIN prep_date AS created_date
    ON prep_crm_opportunity.created_date_id = created_date.date_id
  LEFT JOIN prep_date AS sales_accepted_date
    ON prep_crm_opportunity.sales_accepted_date_id = sales_accepted_date.date_id
  LEFT JOIN prep_date AS stage_1_discovery_date
    ON prep_crm_opportunity.stage_1_discovery_date_id = stage_1_discovery_date.date_id
  WHERE sdr_bdr_user_id IS NOT NULL
    AND opp_created_date >= '2022-10-11' --since this date we are observing improved data quality in terms of associated employee_id with the record
    AND is_live = TRUE

),

last_user_employee_id AS (

  SELECT DISTINCT
    dim_crm_user_id,
    LAST_VALUE(employee_number IGNORE NULLS) OVER (PARTITION BY dim_crm_user_id ORDER BY snapshot_date) AS last_e_number
  FROM prep_crm_user_daily_snapshot
  INNER JOIN sales_dev_opps
    ON prep_crm_user_daily_snapshot.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id

),

sales_dev_hierarchy_prep AS (

  SELECT
    sales_dev_rep.dim_crm_user_id     AS sales_dev_rep_user_id,
    sales_dev_rep.user_role_name      AS sales_dev_rep_role_name,
    sales_dev_rep.user_email          AS sales_dev_rep_email,
    sales_dev_rep.user_name           AS sales_dev_rep_user_name,
    sales_dev_rep.title               AS sales_dev_rep_title,
    sales_dev_rep.department          AS sales_dev_rep_department,
    sales_dev_rep.team                AS sales_dev_rep_team,
    sales_dev_rep.manager_id          AS sales_dev_rep_direct_manager_id,
    sales_dev_rep.manager_name        AS sales_dev_rep_direct_manager_name,
    sales_dev_rep.is_active           AS sales_dev_rep_is_active,
    sales_dev_rep.crm_user_sales_segment,
    sales_dev_rep.crm_user_geo,
    sales_dev_rep.crm_user_region,
    sales_dev_rep.crm_user_area,
    sales_dev_rep.user_role_level_1,
    sales_dev_rep.user_role_level_2,
    sales_dev_rep.user_role_level_3,
    sales_dev_rep.user_role_level_4,
    rep_employee_id.last_e_number     AS sales_dev_rep_employee_number,
    sales_dev_rep.snapshot_date,
    prep_date.fiscal_year             AS snapshot_fiscal_year,
    manager.department                AS sales_dev_manager_department,
    manager.user_role_name            AS sales_dev_manager_user_role_name,
    manager.team                      AS sales_dev_manager_team,
    manager_employee_id.last_e_number AS sales_dev_manager_employee_number,
    manager.user_email                AS sales_dev_manager_email,
    leader.department                 AS sales_dev_leader_department,
    leader.dim_crm_user_id            AS sales_dev_leader_id,
    leader.user_name                  AS sales_dev_leader_name,
    leader.user_role_name             AS sales_dev_leader_user_role_name,
    leader.team                       AS sales_dev_leader_team,
    leader_employee_id.last_e_number  AS sales_dev_leader_employee_number,
    leader.user_email                 AS sales_dev_leader_email
  FROM prep_crm_user_daily_snapshot AS sales_dev_rep
  INNER JOIN sales_dev_opps
    ON sales_dev_rep.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
  LEFT JOIN last_user_employee_id AS rep_employee_id
    ON sales_dev_rep.dim_crm_user_id = rep_employee_id.dim_crm_user_id
  LEFT JOIN prep_crm_user_daily_snapshot AS manager
    ON sales_dev_rep.manager_id = manager.dim_crm_user_id
      AND sales_dev_rep.snapshot_date = manager.snapshot_date
  LEFT JOIN last_user_employee_id AS manager_employee_id
    ON sales_dev_rep.manager_id = manager_employee_id.dim_crm_user_id
  LEFT JOIN prep_crm_user_daily_snapshot AS leader
    ON manager.manager_id = leader.dim_crm_user_id
      AND manager.snapshot_date = leader.snapshot_date
  LEFT JOIN last_user_employee_id AS leader_employee_id
    ON manager.manager_id = leader_employee_id.dim_crm_user_id
  LEFT JOIN prep_date
    ON sales_dev_rep.snapshot_date = prep_date.date_actual

),

fy_role_levels AS ( -- We want to get the current fiscal year hierarchy for each employee by their role name and apply it back to using their employee id. 

  SELECT
    sales_dev_hierarchy_prep.sales_dev_rep_employee_number,
    snapshot_fiscal_year,
    sheetload_sales_dev_role_hierarchy_source.role_level_1,
    sheetload_sales_dev_role_hierarchy_source.role_level_2,
    sheetload_sales_dev_role_hierarchy_source.role_level_3,
    sheetload_sales_dev_role_hierarchy_source.fiscal_year AS hierarchy_fiscal_year
  FROM sales_dev_hierarchy_prep
  INNER JOIN "MY_SNOWFLAKE_PREP_DATABASE".sheetload.sheetload_sales_dev_role_hierarchy_source
    ON sales_dev_hierarchy_prep.sales_dev_rep_role_name = sheetload_sales_dev_role_hierarchy_source.user_role_name
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sales_dev_rep_employee_number, snapshot_fiscal_year
    ORDER BY sheetload_sales_dev_role_hierarchy_source.fiscal_year DESC
  ) = 1

),

final AS (

  SELECT DISTINCT
    sales_dev_hierarchy_prep.sales_dev_rep_user_id                                                         AS dim_crm_user_id,
    sales_dev_hierarchy_prep.sales_dev_rep_role_name,
    COALESCE(fy_role_levels.role_level_1, sheetload_sales_dev_role_hierarchy_source.role_level_1)          AS sales_dev_rep_user_role_level_1,
    COALESCE(fy_role_levels.role_level_2, sheetload_sales_dev_role_hierarchy_source.role_level_2)          AS sales_dev_rep_user_role_level_2,
    COALESCE(fy_role_levels.role_level_3, sheetload_sales_dev_role_hierarchy_source.role_level_3)          AS sales_dev_rep_user_role_level_3,
    sheetload_sales_dev_role_hierarchy_source.fiscal_year                                                  AS sales_dev_rep_user_role_hierarchy_fiscal_year,
    sales_dev_hierarchy_prep.sales_dev_rep_email,
    COALESCE(rep.first_name || ' ' || rep.last_name, sales_dev_hierarchy_prep.sales_dev_rep_user_name)     AS sales_dev_rep_user_full_name,
    sales_dev_hierarchy_prep.sales_dev_rep_title,
    sales_dev_hierarchy_prep.sales_dev_rep_department,
    sales_dev_hierarchy_prep.sales_dev_rep_team,
    sales_dev_hierarchy_prep.sales_dev_rep_is_active,
    sales_dev_hierarchy_prep.crm_user_sales_segment,
    sales_dev_hierarchy_prep.crm_user_geo,
    sales_dev_hierarchy_prep.crm_user_region,
    sales_dev_hierarchy_prep.crm_user_area,
    sales_dev_hierarchy_prep.sales_dev_rep_employee_number,
    sales_dev_hierarchy_prep.snapshot_date,
    sales_dev_hierarchy_prep.snapshot_fiscal_year,
    sales_dev_hierarchy_prep.sales_dev_rep_direct_manager_id,
    COALESCE(manager.first_name || ' ' || manager.last_name, sales_dev_rep_direct_manager_name)            AS sales_dev_rep_manager_full_name,
    sales_dev_hierarchy_prep.sales_dev_manager_email,
    sales_dev_hierarchy_prep.sales_dev_manager_employee_number,
    sales_dev_hierarchy_prep.sales_dev_manager_user_role_name,
    sales_dev_hierarchy_prep.sales_dev_leader_id,
    sales_dev_hierarchy_prep.sales_dev_leader_user_role_name,
    COALESCE(leader.first_name || ' ' || leader.last_name, sales_dev_hierarchy_prep.sales_dev_leader_name) AS sales_dev_rep_leader_full_name,
    sales_dev_hierarchy_prep.sales_dev_leader_employee_number,
    sales_dev_hierarchy_prep.sales_dev_leader_email,
    sales_dev_hierarchy_prep.user_role_level_1,
    sales_dev_hierarchy_prep.user_role_level_2,
    sales_dev_hierarchy_prep.user_role_level_3
  FROM sales_dev_hierarchy_prep
  LEFT JOIN prep_team_member_base AS rep
    ON sales_dev_rep_employee_number = rep.employee_id
  LEFT JOIN prep_team_member_base AS manager
    ON sales_dev_manager_employee_number = manager.employee_id
  LEFT JOIN prep_team_member_base AS leader
    ON sales_dev_leader_employee_number = leader.employee_id
  LEFT JOIN {{ ref('sheetload_sales_dev_role_hierarchy_source') }}
    ON sales_dev_hierarchy_prep.sales_dev_rep_role_name = sheetload_sales_dev_role_hierarchy_source.user_role_name
  LEFT JOIN fy_role_levels
    ON sales_dev_hierarchy_prep.sales_dev_rep_employee_number = fy_role_levels.sales_dev_rep_employee_number
      AND sales_dev_hierarchy_prep.snapshot_fiscal_year = fy_role_levels.snapshot_fiscal_year

)

SELECT *
FROM final
