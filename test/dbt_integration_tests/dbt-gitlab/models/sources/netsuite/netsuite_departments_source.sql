WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'departments') }}

), renamed AS (

    SELECT
      --Primary Key
      department_id::FLOAT       AS department_id,

      --Foreign Key
      parent_id::FLOAT           AS parent_department_id,

      --Info
      name::VARCHAR              AS department_name,
      full_name::VARCHAR         AS department_full_name,

      --Meta
      true::BOOLEAN        AS is_department_inactive

    FROM source

)

SELECT *
FROM renamed
