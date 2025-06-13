WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'red_master_stats') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT d.value as data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT
      data_by_row['date']::date       AS commit_date,
      data_by_row['id']::varchar      AS commit_id
    FROM intermediate

)

SELECT *
FROM renamed
