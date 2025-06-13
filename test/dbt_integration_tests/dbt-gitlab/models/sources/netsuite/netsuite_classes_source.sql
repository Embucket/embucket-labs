WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'classes') }}

), renamed AS (

    SELECT
      --Primary Key
      class_id::FLOAT              AS class_id,

      --Info
      name::VARCHAR                AS class_name,
      full_name::VARCHAR           AS class_full_name,
      true::BOOLEAN          AS is_inactive,
      true::BOOLEAN   AS is_fivetran_deleted

    FROM source

)

SELECT *
FROM renamed
