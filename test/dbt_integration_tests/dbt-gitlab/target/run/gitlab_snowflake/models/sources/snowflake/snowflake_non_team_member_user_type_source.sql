
  
    

create or replace transient table EMBUCKET.snowflake.snowflake_non_team_member_user_type_source
    

    
    as (WITH source AS (

  SELECT *
  FROM EMBUCKET.seed_data.snowflake_non_team_member_user_types

),

renamed AS (

  SELECT
    user_type::VARCHAR  AS user_type,
    user_name::VARCHAR  AS user_name,
    division::VARCHAR   AS division,
    department::VARCHAR AS department
  FROM source

)

SELECT *
FROM renamed
    )
;


  