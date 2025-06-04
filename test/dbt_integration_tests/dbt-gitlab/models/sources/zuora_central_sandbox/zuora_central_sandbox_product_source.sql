WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'product') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                       AS product_id,

      --Info
      name::VARCHAR                     AS product_name,
      sku::VARCHAR                      AS sku,
      description::VARCHAR              AS product_description,
      category::VARCHAR                 AS category,
      product_tier_c::VARCHAR           AS product_tier,
      product_delivery_c::VARCHAR       AS product_delivery_type,
      product_deployment_c::VARCHAR     AS product_deployment_type,
      updated_by_id::VARCHAR            AS updated_by_id,
      updated_date::TIMESTAMP_TZ        AS updated_date,
      _FIVETRAN_DELETED                 AS is_deleted,
      effective_start_date              AS effective_start_date,
      effective_end_date                AS effective_end_date


    FROM source

)

SELECT *
FROM renamed
