WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'budget_category') }}

), renamed AS (

    SELECT
      --Primary Key
      budget_category_id::FLOAT             AS budget_category_id,

      --Info
      true::BOOLEAN                   AS is_inactive,
      true::BOOLEAN                    AS is_global,
      name::VARCHAR                         AS budget_category,
      true::BOOLEAN            AS is_fivetran_deleted

    FROM source

)

SELECT *
FROM renamed
