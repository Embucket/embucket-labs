WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'rate_plan') }}

), renamed AS(

    SELECT
      id                            AS rate_plan_id,
      name                          AS rate_plan_name,
      --keys
      subscription_id               AS subscription_id,
      product_id                    AS product_id,
      product_rate_plan_id          AS product_rate_plan_id,
      -- info
      amendment_id                  AS amendment_id,
      amendment_type                AS amendment_type,

      --metadata
      updated_by_id                 AS updated_by_id,
      updated_date                  AS updated_date,
      created_by_id                 AS created_by_id,
      created_date                  AS created_date,
      _FIVETRAN_DELETED             AS is_deleted

    FROM source

)

SELECT *
FROM renamed
