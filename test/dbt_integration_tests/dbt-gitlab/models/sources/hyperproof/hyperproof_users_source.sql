WITH source AS (
  
    SELECT * 
    FROM {{ source('hyperproof', 'users') }}
    
), metric_per_row AS (

    SELECT
        data_by_row.value['id']::VARCHAR            AS id,
        data_by_row.value['email']::VARCHAR         AS email,
        data_by_row.value['givenName']::VARCHAR     AS given_name,
        data_by_row.value['surname']::VARCHAR       AS surname,
        data_by_row.value['language']::VARCHAR      AS user_language,
        data_by_row.value['lastLogin']::TIMESTAMP   AS last_login,
        data_by_row.value['locale']::VARCHAR        AS locale,
        data_by_row.value['status']::VARCHAR        AS status,
        data_by_row.value['timeZone']::VARCHAR      AS time_zone,
        data_by_row.value['title']::VARCHAR         AS title,
        data_by_row.value['type']::VARCHAR          AS user_type,
        data_by_row.value['userId']::VARCHAR        AS user_id,
        uploaded_at

    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(jsontext), OUTER => True) data_by_row

)
SELECT *
FROM metric_per_row