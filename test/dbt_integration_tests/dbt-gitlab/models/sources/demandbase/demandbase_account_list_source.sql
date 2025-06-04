WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account_list') }}

), renamed AS (

    SELECT
      jsontext['creation_time']::TIMESTAMP          AS created_at,
      jsontext['id']::NUMBER                        AS account_list_id,
      jsontext['name']::VARCHAR                     AS account_list_name,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed