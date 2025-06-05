
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_sales_analytics_tableau_rls_users_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sales_analytics_tableau_rls_users

), renamed AS (

    SELECT
      email::VARCHAR                              AS email,
      username::VARCHAR                           AS username,
      role::VARCHAR                               AS role

    FROM source

)

SELECT *
FROM renamed
    )
;


  