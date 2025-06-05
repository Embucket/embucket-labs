
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_zero_dollar_subscription_to_paid_subscription_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.zero_dollar_subscription_to_paid_subscription

)

SELECT *
FROM source
    )
;


  