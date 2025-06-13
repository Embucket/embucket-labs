 WITH source AS(
 SELECT *
  FROM {{ source('coupa', 'supplier') }}
  WHERE NOT _fivetran_deleted --means it was deleted in source system and fivetran can't see it anymore


),

renamed AS (

  SELECT
    id::INT                 AS supplier_id,
    name::VARCHAR           AS supplier_name,
    STATUS::VARCHAR         AS supplier_status,  
    
  FROM source

)

SELECT *
FROM renamed