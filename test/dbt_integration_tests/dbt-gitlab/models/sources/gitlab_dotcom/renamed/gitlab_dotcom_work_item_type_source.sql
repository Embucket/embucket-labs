WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_work_item_types_dedupe_source') }}
    
), renamed AS (
  
    SELECT
      id::NUMBER                                AS work_item_type_id,
      name::VARCHAR                             AS work_item_type_name,
      created_at::TIMESTAMP                     AS created_at,
      updated_at::TIMESTAMP                     AS updated_at,
      TO_TIMESTAMP_NTZ(_uploaded_at::NUMBER)    AS uploaded_at
      
    FROM source
    
)

SELECT * 
FROM renamed
