
  
    

create or replace transient table EMBUCKET.legacy.sheetload_zero_dollar_subscription_to_paid_subscription
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_zero_dollar_subscription_to_paid_subscription_source

)

SELECT *
FROM source
    )
;


  