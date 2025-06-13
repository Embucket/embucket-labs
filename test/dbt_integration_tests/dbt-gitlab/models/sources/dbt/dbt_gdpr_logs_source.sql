{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'gdpr_logs') }}

    ), flattened AS (

    SELECT
      d.value['info']['ts']::TIMESTAMP                                                          AS time_stamp,
      d.value['info']['msg']::VARCHAR                                                           AS info_msg,
      d.value['info']['invocation_id']::VARCHAR                                                 AS invocation_id,
      uploaded_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

)
SELECT * FROM flattened
ORDER BY 1




