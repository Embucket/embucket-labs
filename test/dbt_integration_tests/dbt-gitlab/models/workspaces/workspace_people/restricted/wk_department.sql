WITH lkup_div AS (

  SELECT DISTINCT
    CASE
      WHEN division = 'Legal'
        THEN 'LACA'
      ELSE division
    END AS division,
    cost_center,
    department,
    MIN(date_actual) AS range_start,
    MAX(date_actual) AS range_end,
    CASE
      WHEN MAX(date_actual) = current_date THEN 'Y'
      ELSE 'N'
    END AS is_active,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY range_end DESC, range_start DESC, cost_center ASC) AS dept_rank
  FROM {{ ref('employee_directory_analysis') }}
  WHERE department IS NOT NULL
  AND date_actual <= current_date
  GROUP BY 1, 2, 3

) 
  
SELECT 
  division,
  CASE
    WHEN department = 'Sales Development'
      THEN 'Marketing (SDR)'
    WHEN department = 'Meltano'
      THEN 'Engineering'
    WHEN department = 'UX'
      THEN 'Product'
    WHEN department in ('Data', 'Data (inactive)')
      THEN 'Marketing' 
    WHEN department = 'Security'
      THEN 'Security'
    WHEN department = 'Product'
      THEN 'Product' 
    ELSE division
  END AS modified_division,
  cost_center,
  department,
  CASE
    WHEN department IN ('eCommerce', 'MM', 'SMB')
      THEN 'Commercial Sales'
    WHEN department IN ('Channel-Indirect', 'Channel-Program')
      THEN 'Channel'
    WHEN department IN ('Consulting Delivery', 'Education Delivery', 'Education Services', 'Practice Management', 'SA', 'TAM', 'CSM')
      THEN 'Customer Success'
    WHEN department IN ('ENTR', 'PubSec')
      THEN 'Enterprise Sales'
    WHEN department IN ('Enablement', 'Field Ops - Child')
      THEN 'Field Operations'
    WHEN department = 'Development'
      THEN 'Core Development'
    WHEN department IN ('Quality', 'Platforms')
      THEN 'Internal Infrastructure'
    WHEN department = 'Infrastructure'
      THEN 'Infrastructure Platforms'
    WHEN department = 'Incubation Engineering'
      THEN 'Sec & Monetization'
    WHEN department = 'Expansion'
      THEN 'Sec & Monetization'      
    WHEN department = 'Assigned Support Engineer'
      THEN 'ASE'
    WHEN department = 'Product Management'
      THEN 'Product Leadership'
    WHEN department = 'Product Monetization'
      THEN 'Monetization'
    WHEN department = 'UX'
      THEN 'UX Research'
    WHEN department = 'Marketing Strategy and Analytics'
      THEN 'Marketing Analytics'  
    WHEN department = 'Security'
      THEN 'Office of CISO'
    WHEN department in ('Field Marketing', 'Partner Marketing', 'Partner Marketing (inactive)')
      THEN 'Regional Marketing'
    WHEN department in ('Account Based Marketing', 'Account Based Marketing (inactive)', 'Campaigns', 'Search Marketing', 'Search Marketing (inactive)')
      THEN 'Digital Marketing'
    WHEN department in ('Data', 'Data (inactive)')
      THEN 'Enterprise Data'
    WHEN department = 'Community Relations'
      THEN 'Developer Relations'
    WHEN department = 'Accounting'
      THEN 'Accounting Operations'
    WHEN department = 'Finance'
      THEN 'FP&A'      
    ELSE department
  END AS modified_department,
  CASE
    WHEN department IN ('Commercial Sales', 'Enterprise Sales')
      THEN 'Y'
      ELSE 'N'
  END AS is_quota_carrying,
  is_active,
  current_timestamp AS last_updated
FROM lkup_div 
WHERE dept_rank = 1
ORDER BY division ASC, cost_center ASC, department ASC
