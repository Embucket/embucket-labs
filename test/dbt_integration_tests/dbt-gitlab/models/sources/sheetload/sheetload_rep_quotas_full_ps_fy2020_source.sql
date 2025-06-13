WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','rep_quotas_full_ps_fy2020') }}
    
), final AS (

    SELECT 
      sales_rep,
      type, 
      team,                                                 
      fiscal_year::NUMBER                               AS fiscal_year,
      ZEROIFNULL(NULLIF("PS_QUOTA",'')::NUMBER(16,5))   AS ps_quota, 
      bamboo_employee_id::NUMBER                        AS bamboo_employee_id
    FROM source
      
) 

SELECT * 
FROM final
