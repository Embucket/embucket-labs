WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','abuse_mitigation') }}

), final AS (
    
    SELECT 
        NULLIF(email_domain, '')::VARCHAR                                 AS email_domain,
        NULLIF(account_creation_date, '')::VARCHAR::DATE                  AS account_creation_date,
        NULLIF(account_creation_time, '')::VARCHAR::TIME                  AS account_creation_time,
        NULLIF(account_creation_timestamp, '')::VARCHAR::TIMESTAMP        AS account_creation_timestamp,
        NULLIF(category, '')::VARCHAR                                     AS category,
        NULLIF(description, '')::VARCHAR                                  AS description,
        NULLIF(automation, '')::VARCHAR                                   AS automation,
        TRY_TO_NUMBER(mitigation_week)                                    AS mitigation_week,
        TRY_TO_NUMBER(mitigation_month)                                   AS mitigation_month,
        NULLIF(mitigation_date, '')::VARCHAR::DATE                        AS mitigation_date,
        NULLIF(mitigation_time, '')::VARCHAR::TIME                        AS mitigation_time,
        NULLIF(mitigation_timestamp, '')::VARCHAR::TIMESTAMP              AS mitigation_timestamp,
        NULLIF(timezone, '')::VARCHAR                                     AS timezone,
        TRY_TO_DECIMAL(time_to_mitigate)                                  AS time_to_mitigate,
        TRY_TO_NUMBER(rule_id)                                            AS rule_id,
        NULLIF(rule_name, '')::VARCHAR                                    AS rule_name
    FROM source

) 

SELECT * 
FROM final
