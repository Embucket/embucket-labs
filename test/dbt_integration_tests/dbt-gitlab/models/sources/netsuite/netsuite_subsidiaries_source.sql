WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'subsidiaries') }}

), renamed AS (

    SELECT
      --Primary Key
      subsidiary_id::FLOAT               AS subsidiary_id,

      --Info
      full_name::VARCHAR                 AS subsidiary_full_name,
      name::VARCHAR                      AS subsidiary_name,
      base_currency_id::FLOAT            AS base_currency_id,
      fiscal_calendar_id::FLOAT          AS fiscal_calendar_id,
      parent_id::FLOAT                   AS parent_id,

      --Meta
      true::BOOLEAN                AS is_subsidiary_inactive,
      true::BOOLEAN            AS is_elimination_subsidiary

    FROM source

)

SELECT *
FROM renamed
