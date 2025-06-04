WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account_keyword_intent') }}

), renamed AS (

    SELECT
      jsontext['account_id']::NUMBER                AS account_id,
      jsontext['intent_strength']::VARCHAR          AS intent_strength,
      jsontext['is_trending']::BOOLEAN              AS is_trending,
      jsontext['keyword']::VARCHAR                  AS keyword,
      jsontext['keyword_set_id']::NUMBER            AS keyword_set_id,
      jsontext['people_researching_count']::NUMBER  AS people_researching_count,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed