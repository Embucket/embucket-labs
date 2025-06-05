
  
    

create or replace transient table EMBUCKET.snowflake.snowflake_databases_source
    

    
    as (SELECT *
FROM snowflake.account_usage.databases
    )
;


  