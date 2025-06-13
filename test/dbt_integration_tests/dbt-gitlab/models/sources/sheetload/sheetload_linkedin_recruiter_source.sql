WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','linkedin_recruiter') }}
    
), renamed AS (

    SELECT 
      seat_holder::VARCHAR                                 AS sourcer,
      ZEROIFNULL(NULLIF("MESSAGES_SENT",''))::NUMBER       AS messages_sent,
      ZEROIFNULL(NULLIF("RESPONSES_RECEIVED",''))::NUMBER  AS responses_received,
      ZEROIFNULL(NULLIF("ACCEPTANCES",''))::NUMBER         AS acceptances,
      ZEROIFNULL(NULLIF("DECLINES",''))::NUMBER            AS declines,
      ZEROIFNULL(NULLIF("NO_RESPONSE",''))::NUMBER         AS no_response,
      ZEROIFNULL(NULLIF("RESPONSES_RATE",''))::NUMBER      AS responses_rate,
      ZEROIFNULL(NULLIF("ACCEPT_RATE",''))::NUMBER         AS accept_rate,
      month::DATE                                          AS data_downloaded_month
    FROM source
      
) 

SELECT *
FROM renamed
WHERE data_downloaded_month IS NOT NULL 
  AND sourcer IS NOT NULL
