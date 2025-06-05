
  
    

create or replace transient table EMBUCKET.specific.ntv_handbook_file_classification_mapping
    

    
    as (WITH source AS (
  SELECT *
  FROM EMBUCKET.seed_engineering.handbook_file_classification_mapping
)

SELECT *
FROM source
    )
;


  