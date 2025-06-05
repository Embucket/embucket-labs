
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_ci_runner_machine_type_mapping_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.sheetload.ci_runner_machine_type_mapping

)

SELECT * 
FROM source
    )
;


  