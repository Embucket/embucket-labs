WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'advisory_data') }}

), renamed AS (

    SELECT
      "FILE"::VARCHAR         AS filename,
      pubdate::DATE         AS publish_date,
      mergedate::DATE       AS merge_date,
      delta::NUMBER,
      packagetype::VARCHAR  AS package_type
    FROM source

)

SELECT *
FROM renamed
