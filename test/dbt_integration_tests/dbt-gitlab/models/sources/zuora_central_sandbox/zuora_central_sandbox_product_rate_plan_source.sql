WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'product_rate_plan') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                               AS product_rate_plan_id,

      --Info
      product_id::VARCHAR                       AS product_id,
      description::VARCHAR                      AS product_rate_plan_description,
      effective_end_date::TIMESTAMP_TZ          AS effective_end_date,
      effective_start_date::TIMESTAMP_TZ        AS effective_start_date,
      name::VARCHAR                             AS product_rate_plan_name,
      created_by_id::VARCHAR                    AS created_by_id,
      created_date::TIMESTAMP_TZ                AS created_date,
      updated_by_id::VARCHAR                    AS updated_by_id,
      updated_date::TIMESTAMP_TZ                AS updated_date,
      _FIVETRAN_DELETED                         AS is_deleted

    FROM source

)

SELECT *
FROM renamed