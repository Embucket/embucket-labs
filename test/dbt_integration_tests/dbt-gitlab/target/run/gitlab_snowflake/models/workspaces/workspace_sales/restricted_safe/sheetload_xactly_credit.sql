
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_sales.sheetload_xactly_credit
    

    
    as (WITH final AS (

  SELECT * 
  FROM EMBUCKET.sheetload.sheetload_xactly_credit_source

)
SELECT
      *,
      '@michellecooper'::VARCHAR       AS created_by,
      '@michellecooper'::VARCHAR       AS updated_by,
      '2022-06-16'::DATE        AS model_created_date,
      '2022-06-16'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM final
    )
;


  