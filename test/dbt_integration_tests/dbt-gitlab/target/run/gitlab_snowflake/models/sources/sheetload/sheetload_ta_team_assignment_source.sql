
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_ta_team_assignment_source
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.ta_team_assignment

        )
        SELECT * 
        FROM source
    )
;


  