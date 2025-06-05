
  create or replace   view EMBUCKET.restricted_workspace_people.wk_sheetload_ta_team_assignment
  
   as (
    WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_ta_team_assignment_source

)

SELECT *
FROM source
  );

