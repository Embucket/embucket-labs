WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'accounting_books') }}

), renamed AS (

    SELECT
      --Primary Key
      accounting_book_id::FLOAT                   AS accounting_book_id,

      --Info
      accounting_book_extid::VARCHAR              AS accounting_book_extid,
      accounting_book_name::VARCHAR               AS accounting_book_name,
      base_book_id::FLOAT                         AS base_book_id,
      date_created::TIMESTAMP_TZ                  AS date_created,
      date_last_modified::TIMESTAMP_TZ            AS date_last_modified,
      effective_period_id::FLOAT                  AS effective_period_id,
      form_template_component_id::VARCHAR         AS form_template_component_id,
      form_template_id::FLOAT                     AS form_template_id,
      true::BOOLEAN                         AS is_primary,
      status::VARCHAR                             AS status

    FROM source

)

SELECT *
FROM renamed
