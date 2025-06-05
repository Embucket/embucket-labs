
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_sales_analytics_tableau_rls_roles_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sales_analytics_tableau_rls_roles

), renamed AS (

    SELECT
      role::VARCHAR                              AS role,
      key_segment_geo::VARCHAR                  AS key_segment_geo

    FROM source

)

SELECT *
FROM renamed
    )
;


  