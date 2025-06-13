WITH source AS (
  
    SELECT * 
    FROM {{ source('hyperproof', 'roles') }}
    
), metric_per_row AS (

    SELECT
      data_by_row.value['id']::VARCHAR                              AS role_id,
      data_by_row.value['name']::VARCHAR                            AS role_name,
      data_by_row.value['scope']::VARCHAR                           AS scope,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(jsontext), OUTER => True) data_by_row
)

SELECT *
FROM metric_per_row