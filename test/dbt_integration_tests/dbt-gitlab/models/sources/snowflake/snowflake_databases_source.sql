
SELECT *
FROM {{ source('snowflake_account_usage','databases') }}