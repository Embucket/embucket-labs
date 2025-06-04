{{ config(
    tags=["mnpi_exception"]
) }}

WITH final AS (

    SELECT
    --primary key
        {{ dbt_utils.generate_surrogate_key(['dim_crm_user_id','snapshot_date']) }} AS sales_dev_rep_user_hierarchy_pk,

    --secondary keys
        dim_crm_user_id,
        sales_dev_rep_direct_manager_id,
        sales_dev_leader_id,

    --sales dev rep info
        sales_dev_rep_role_name,
        sales_dev_rep_user_role_level_1,
        sales_dev_rep_user_role_level_2,
        sales_dev_rep_user_role_level_3,
        sales_dev_rep_user_role_hierarchy_fiscal_year,
        sales_dev_rep_email,
        sales_dev_rep_user_full_name,
        sales_dev_rep_title,
        sales_dev_rep_department,
        sales_dev_rep_team,
        sales_dev_rep_is_active,
        sales_dev_rep_employee_number,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        
    --sales dev manager info
        sales_dev_rep_manager_full_name,
        sales_dev_manager_email,
        sales_dev_manager_employee_number,
        sales_dev_manager_user_role_name,

    --sales dev leader info
        sales_dev_leader_user_role_name,
        sales_dev_rep_leader_full_name,
        sales_dev_leader_employee_number,
        sales_dev_leader_email,

    --other     
        snapshot_date
    FROM {{ ref('prep_sales_dev_user_hierarchy') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-07-31",
    updated_date="2024-07-31"
  ) }}