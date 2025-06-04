WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account_keyword_historical_rollup') }}

), renamed AS (

    SELECT
      jsontext['account_id']::NUMBER                AS account_id,
      jsontext['backward_ordinal']::NUMBER          AS backward_ordinal,
      jsontext['duration_count']::NUMBER            AS duration_count,
      jsontext['duration_type']::VARCHAR            AS duration_type,
      jsontext['keyword']::VARCHAR                  AS keyword,
      jsontext['people_researching_count']::NUMBER  AS people_researching_count,
      jsontext['start_date']::DATE                  AS start_date,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed