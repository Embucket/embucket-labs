
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_team_health_data_source
    

    
    as (WITH source AS (

        SELECT * 
        FROM EMBUCKET.sheetload.team_health_data

        )
        SELECT * 
        FROM source
    )
;


  