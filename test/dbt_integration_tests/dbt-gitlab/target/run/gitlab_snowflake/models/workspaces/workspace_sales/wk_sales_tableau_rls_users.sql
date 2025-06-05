
  
    

create or replace transient table EMBUCKET.workspace_sales.wk_sales_tableau_rls_users
    

    
    as (

WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_sales_analytics_tableau_rls_users_source

)


SELECT *
FROM source
    )
;


  