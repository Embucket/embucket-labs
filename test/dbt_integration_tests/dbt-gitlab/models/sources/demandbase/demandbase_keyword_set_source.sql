WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'keyword_set') }}

), renamed AS (

    SELECT
      jsontext['competitive']::BOOLEAN              AS is_competitive,
      jsontext['creation_time']::TIMESTAMP          AS created_at,
      jsontext['id']::NUMBER                        AS keyword_set_id,
      jsontext['name']::VARCHAR                     AS name,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed