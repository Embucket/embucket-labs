 WITH source AS(
 SELECT *
  FROM {{ source('coupa', 'order_line') }}


),

renamed AS (

  SELECT
    id::INT                                                          AS line_id,
    line_num::INT                                                    AS line_number,
    order_header_id::INT                                             AS order_header_id,
    order_header_number::INT                                         AS order_header_number,
    description::VARCHAR                                             AS line_description,
    NULLIF(custom_fields['amort-start-date'], '')::DATE              AS line_start_date,
    NULLIF(custom_fields['amort-end-date'], '')::DATE                AS line_end_date,
    commodity_id::VARCHAR                                            AS commodity_id,
    account_id::VARCHAR                                              AS account_id,
    accounting_total_currency_id::INT                                AS accounting_total_currency_id,
    reporting_total::FLOAT                                           AS line_usd_price 
  FROM source

)

SELECT *
FROM renamed
