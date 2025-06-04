WITH header AS (
  SELECT *
  FROM {{ ref('coupa_order_header_source') }}
),

line AS (
  SELECT *
  FROM {{ ref('coupa_order_line_source') }}
),

account AS (
  SELECT *
  FROM {{ ref('coupa_account_source') }}
),

commodity AS (
  SELECT *
  FROM {{ ref('coupa_commodity_source') }}
),

supplier AS (
  SELECT *
  FROM {{ ref('coupa_supplier_source') }}
),

netsuite_department AS (
  SELECT
    department_id,
    department_full_name,
    TRIM(SPLIT_PART(department_full_name, ':', 1)) AS department,
    TRIM(SPLIT_PART(department_full_name, ':', 2)) AS subdepartment
  FROM {{ ref('netsuite_departments_source') }}
)

SELECT
  header.po_id                                                                       AS po_number,
  header.po_status,
  supplier.supplier_name                                                             AS vendor,
  MAX(line.line_end_date) > CURRENT_DATE                                             AS is_forward_looking,
  LISTAGG(line.line_description, ' | ') WITHIN GROUP (ORDER BY line.line_number)     AS description,
  ARRAY_AGG(netsuite_department.subdepartment) 
    WITHIN GROUP (ORDER BY line.line_usd_price DESC, line.line_number)[0]::VARCHAR   AS budget_owner,
  LISTAGG(DISTINCT netsuite_department.department, ' | ')                            AS departments,
  LISTAGG(DISTINCT netsuite_department.subdepartment, ' | ')                         AS subdepartments,
  LISTAGG(DISTINCT commodity.commodity_name, ' | ')                                  AS commodities,
  MIN(line.line_start_date)                                                          AS po_start_date,
  MAX(line.line_end_date)                                                            AS po_end_date,
  SUM(line.line_usd_price)                                                           AS total_contract_value
FROM header
LEFT JOIN line
  ON header.po_number = line.order_header_number
LEFT JOIN supplier
  ON header.supplier_id = supplier.supplier_id
LEFT JOIN commodity
  ON line.commodity_id = commodity.commodity_id
LEFT JOIN account
  ON line.account_id = account.account_id
LEFT JOIN netsuite_department
  ON account.account_department_code = netsuite_department.department_id
GROUP BY 1, 2, 3
