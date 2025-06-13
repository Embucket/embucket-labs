SELECT
  mart_crm_person.*,
  '1. New - First Order'                   AS order_type,
  '1) New - First Order'                   AS order_type_grouped,
  'MQLs & Trials'                          AS sales_qualified_source_name,
  dim_date.fiscal_month_name_fy,
  dim_date.fiscal_quarter_name_fy,  
  dim_date.fiscal_year,
  dim_date.fiscal_year                     AS date_range_year,
  dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  dim_date.date_id                         AS date_range_id
FROM {{ ref('mart_crm_person') }}
LEFT JOIN {{ ref('dim_date') }} 
  ON mart_crm_person.mql_date_first_pt = dim_date.date_day
WHERE is_mql = 1
