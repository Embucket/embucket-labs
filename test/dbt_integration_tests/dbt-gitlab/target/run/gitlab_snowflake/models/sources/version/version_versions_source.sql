
  
    

create or replace transient table EMBUCKET.version_db.version_versions_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.version_db.versions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY version ORDER BY updated_at DESC) = 1


), renamed AS (

    SELECT
      id::NUMBER            AS id,
      version::VARCHAR      AS version,
      vulnerable::VARCHAR   AS is_vulnerable,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at
    FROM source  

)

SELECT *
FROM renamed
    )
;


  