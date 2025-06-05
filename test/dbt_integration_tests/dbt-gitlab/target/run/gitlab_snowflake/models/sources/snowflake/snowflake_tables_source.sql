
  
    

create or replace transient table EMBUCKET.snowflake.snowflake_tables_source
    

    
    as (SELECT *
FROM snowflake.account_usage.tables
    )
;


  