WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'product') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                       AS product_id,

      --Info
      name::VARCHAR                     AS product_name,
      sku::VARCHAR                      AS sku,
      description::VARCHAR              AS product_description,
      category::VARCHAR                 AS category,
      updatedbyid::VARCHAR              AS updated_by_id,
      updateddate::TIMESTAMP_TZ         AS updated_date,
      deleted                           AS is_deleted,
      effectivestartdate                AS effective_start_date,
      effectiveenddate                  AS effective_end_date

    FROM source

)

SELECT *
FROM renamed
