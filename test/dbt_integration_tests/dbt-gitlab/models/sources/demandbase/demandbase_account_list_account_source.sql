WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account_list_account') }}

), renamed AS (

    SELECT
      jsontext['account_id']::NUMBER              AS account_id,
      jsontext['account_list_id']::VARCHAR       AS account_list_id,
      jsontext['partition_date']::DATE           AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed