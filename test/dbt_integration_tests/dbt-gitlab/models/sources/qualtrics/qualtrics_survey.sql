
WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'survey') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT d.value as data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), parsed AS (

    SELECT 
      data_by_row['id']::VARCHAR    AS survey_id,
      data_by_row['name']::VARCHAR  AS survey_name
    FROM intermediate
    WHERE data_by_row IS NOT NULL

)
SELECT *
FROM parsed
