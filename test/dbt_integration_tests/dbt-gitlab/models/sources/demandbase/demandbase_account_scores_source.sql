WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account_scores') }}

), renamed AS (

    SELECT
      jsontext['account_domain']::VARCHAR           AS account_domain,
      jsontext['account_id']::NUMBER                AS account_id,
      jsontext['score']::VARCHAR                    AS account_score,
      jsontext['score_type']::VARCHAR               AS score_type,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed