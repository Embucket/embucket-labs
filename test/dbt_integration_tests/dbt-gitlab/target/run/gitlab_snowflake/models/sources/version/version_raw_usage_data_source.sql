
  
    

create or replace transient table EMBUCKET.version_db.version_raw_usage_data_source
    

    
    as (

WITH source AS (

    SELECT *
    FROM EMBUCKET.version_db.raw_usage_data
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::VARCHAR             AS raw_usage_data_id,
      PARSE_JSON(payload)     AS raw_usage_data_payload,
      created_at::TIMESTAMP   AS created_at,
      recorded_at::TIMESTAMP  AS recorded_at,
      _uploaded_at::TIMESTAMP AS uploaded_at
    FROM source

)

SELECT *
FROM renamed
    )
;


  