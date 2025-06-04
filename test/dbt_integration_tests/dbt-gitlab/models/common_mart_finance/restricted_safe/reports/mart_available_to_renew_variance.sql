{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([


    ('fct_available_to_renew', 'fct_available_to_renew'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('dim_date', 'dim_date')
]) }},

-- Calculate available to renew amounts by fiscal quarter and account

available_to_renew AS (

  SELECT
    fiscal_quarter_name_fy,
    dim_crm_account_id,
    SUM(arr) AS available_to_renew
  FROM fct_available_to_renew
  {{ dbt_utils.group_by(n=2) }}
  ORDER BY fiscal_quarter_name_fy

),

-- Get renewal opportunity information

opportunity_information AS (

  SELECT
    dim_crm_account_id,
    (CASE WHEN stage_name ILIKE '%Close%' THEN subscription_start_date
      WHEN subscription_renewal_date IS NOT NULL THEN subscription_renewal_date
      ELSE subscription_start_date
    END)                     AS subscription_renewal_date,
    SUM(arr_basis_for_clari) AS arr_basis_for_clari
  FROM mart_crm_opportunity
  WHERE subscription_type = 'Renewal'
    AND stage_name != '10-Duplicate'
    AND is_jihu_account != 'TRUE'
  {{ dbt_utils.group_by(n=2) }}

),

-- Join with date dimension to get fiscal quarters

dates AS (

  SELECT
    dim_date.fiscal_quarter_name_fy,
    opportunity_information.dim_crm_account_id,
    SUM(opportunity_information.arr_basis_for_clari) AS arr_basis
  FROM opportunity_information
  LEFT JOIN dim_date
    ON opportunity_information.subscription_renewal_date = dim_date.date_day
  WHERE opportunity_information.arr_basis_for_clari != 0.0
    AND opportunity_information.arr_basis_for_clari != 0
  {{ dbt_utils.group_by(n=2) }}
  ORDER BY fiscal_quarter_name_fy ASC

),

-- Final calculations with variance

final AS (

  SELECT
    COALESCE(available_to_renew.fiscal_quarter_name_fy, dates.fiscal_quarter_name_fy)    AS fiscal_quarter_name_fy,
    COALESCE(available_to_renew.dim_crm_account_id, dates.dim_crm_account_id)            AS dim_crm_account_id,
    available_to_renew.available_to_renew,
    dates.arr_basis
  FROM available_to_renew
  FULL OUTER JOIN dates
    ON available_to_renew.dim_crm_account_id = dates.dim_crm_account_id
      AND available_to_renew.fiscal_quarter_name_fy = dates.fiscal_quarter_name_fy

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-01-29",
updated_date="2024-01-29"
) }}
