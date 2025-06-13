WITH zuora_revenue_organization AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_organization')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 
    
      id::VARCHAR                               AS zuora_revenue_organization_id,
      org_id::VARCHAR                           AS organization_id,
      org_name::VARCHAR                         AS organization_name,
      crtd_by::VARCHAR                          AS organization_created_by,
      crtd_dt::DATETIME                         AS organization_created_date,
      updt_by::VARCHAR                          AS organization_updated_by,
      updt_dt::DATETIME                         AS organization_updated_date,
      client_id::VARCHAR                        AS client_id,
      CONCAT(crtd_prd_id::VARCHAR, '01')        AS organization_created_period_id,
      incr_updt_dt::DATETIME                    AS incremental_update_date,
      org_code::VARCHAR                         AS organization_code,
      entity_id::VARCHAR                        AS entity_id

    FROM zuora_revenue_organization

)

SELECT *
FROM renamed