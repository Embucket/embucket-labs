
  
    

create or replace transient table EMBUCKET.legacy.mart_arr_clone_rollup_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.full_table_clones.mart_arr_rollup

)

SELECT *
FROM source
    )
;


  