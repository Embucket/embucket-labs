WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'entity') }}

), renamed AS (

    SELECT
      --Primary Key
      entity_id::FLOAT             AS entity_id,

      --Info
      name::VARCHAR                AS entity_name,
      full_name::VARCHAR           AS entity_full_name,
      _fivetran_deleted::BOOLEAN   As is_fivetran_deleted

    FROM source

)

SELECT *
FROM renamed
