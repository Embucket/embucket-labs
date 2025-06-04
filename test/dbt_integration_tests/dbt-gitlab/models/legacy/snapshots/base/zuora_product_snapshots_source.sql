WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'zuora_product_snapshots') }}

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

      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to

    FROM source

)

SELECT *
FROM renamed
