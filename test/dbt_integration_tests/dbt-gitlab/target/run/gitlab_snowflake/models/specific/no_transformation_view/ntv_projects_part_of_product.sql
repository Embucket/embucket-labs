
  
    

create or replace transient table EMBUCKET.specific.ntv_projects_part_of_product
    

    
    as (WITH source AS (
  SELECT * 
  FROM EMBUCKET.seed_engineering.projects_part_of_product
)

SELECT *
FROM source
    )
;


  