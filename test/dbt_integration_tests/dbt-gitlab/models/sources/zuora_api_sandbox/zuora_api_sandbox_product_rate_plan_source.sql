WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'product_rate_plan') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                       AS product_rate_plan_id,

      --Info
      productid::VARCHAR                AS product_id,
      description::VARCHAR              AS product_rate_plan_description,
      effectiveenddate::TIMESTAMP_TZ    AS effective_end_date,
      effectivestartdate::TIMESTAMP_TZ  AS effective_start_date,
      name::VARCHAR                     AS product_rate_plan_name,
      createdbyid::VARCHAR              AS created_by_id,
      createddate::TIMESTAMP_TZ         AS created_date,
      updatedbyid::VARCHAR              AS updated_by_id,
      updateddate::TIMESTAMP_TZ         AS updated_date,
      deleted                           AS is_deleted

    FROM source

)

SELECT *
FROM renamed