
  
    

create or replace transient table EMBUCKET.gitlab_data_yaml.director_location_factor_seed_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.seed_people.director_location_factors

), formated AS (

    SELECT
      country::VARCHAR                                   AS country, 
      locality::VARCHAR                                  AS locality, 
      factor::NUMBER(6, 3)                               AS factor, 
      valid_from::DATE                                   AS valid_from, 
      COALESCE(valid_to, DATEADD('day',1,CURRENT_DATE()))::DATE    AS valid_to, 
      is_current::BOOLEAN                                AS is_current
    FROM source

)

SELECT *
FROM formated
    )
;


  