
  
    

create or replace transient table EMBUCKET.legacy.sheetload_books
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.sheetload.sheetload_books_source

)

SELECT
      *,
      '@michellecooper'::VARCHAR       AS created_by,
      '@michellecooper'::VARCHAR       AS updated_by,
      '2022-05-26'::DATE        AS model_created_date,
      '2022-05-26'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM source
    )
;


  