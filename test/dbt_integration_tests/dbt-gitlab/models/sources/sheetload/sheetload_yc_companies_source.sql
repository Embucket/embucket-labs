WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'yc_companies') }}

), renamed as (

    SELECT 
      MD5("Name"::VARCHAR || "Batch"::VARCHAR) AS company_id,
      "Name"::VARCHAR                          AS company_name,
      "Batch"::VARCHAR                         AS yc_batch,
      "Description"::VARCHAR                   AS company_description
    FROM source

)

SELECT *
FROM renamed
