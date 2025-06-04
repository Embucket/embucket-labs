WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account') }}

), renamed AS (

    SELECT DISTINCT
      jsontext['account_domain']::VARCHAR AS account_domain,
      jsontext['account_id']::NUMBER      AS account_id,
      jsontext['account_name']::VARCHAR   AS account_name,
      jsontext['partition_date']::DATE    AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed