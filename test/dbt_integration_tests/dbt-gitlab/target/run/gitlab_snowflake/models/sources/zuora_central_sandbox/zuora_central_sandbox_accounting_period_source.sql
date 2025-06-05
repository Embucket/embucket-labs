
  
    

create or replace transient table EMBUCKET.zuora_central_sandbox.zuora_central_sandbox_accounting_period_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.zuora_central_sandbox_fivetran.accounting_period

), renamed AS (

    SELECT
      --Primary Keys
     id::VARCHAR                       AS accounting_period_id,

      --Info
      end_date::TIMESTAMP_TZ           AS end_date,
      fiscal_year::NUMBER              AS fiscal_year,
      name::VARCHAR                    AS accounting_period_name,
      start_date::TIMESTAMP_TZ         AS accounting_period_start_date,
      status::VARCHAR                  AS accounting_period_status,
      updated_by_id::VARCHAR           AS updated_by_id,
      updated_date::TIMESTAMP_TZ       AS updated_date

    FROM source

)

SELECT *
FROM renamed
    )
;


  