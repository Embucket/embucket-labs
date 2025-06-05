
  
    

create or replace transient table EMBUCKET.version_db.version_usage_ping_metadata_source
    

    
    as (


WITH source AS (

  SELECT *
  FROM EMBUCKET.version_db.usage_ping_metadata
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1

),

renamed AS (

  SELECT
    id::NUMBER AS id,
    created_at::TIMESTAMP AS created_at,
    metrics::VARCHAR as metrics,
    uuid::VARCHAR AS uuid
  FROM source

)

SELECT *
FROM renamed
ORDER BY created_at
    )
;


  