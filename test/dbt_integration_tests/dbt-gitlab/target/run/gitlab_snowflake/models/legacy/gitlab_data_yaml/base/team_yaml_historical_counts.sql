
  
    

create or replace transient table EMBUCKET.legacy.team_yaml_historical_counts
    

    
    as (-- This file is loaded through dbt seed, your local runs will break unless you run dbt seed first.

WITH source AS (

    SELECT *
    FROM EMBUCKET.seed_engineering.historical_counts_maintainers_engineers
)

SELECT * FROM source
    )
;


  