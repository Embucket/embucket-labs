WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'keyword_set_keyword') }}

), renamed AS (

    SELECT
      jsontext['keyword']::VARCHAR                  AS keyword,
      jsontext['keyword_set_id']::NUMBER            AS keyword_set_id,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed