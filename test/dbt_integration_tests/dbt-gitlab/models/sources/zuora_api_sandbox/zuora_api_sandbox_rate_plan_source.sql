WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'rate_plan') }}

), renamed AS(

    SELECT
      id                  AS rate_plan_id,
      name                AS rate_plan_name,
      --keys
      subscriptionid      AS subscription_id,
      productid           AS product_id,
      productrateplanid   AS product_rate_plan_id,
      -- info
      amendmentid         AS amendment_id,
      amendmenttype       AS amendment_type,

      --metadata
      updatedbyid         AS updated_by_id,
      updateddate         AS updated_date,
      createdbyid         AS created_by_id,
      createddate         AS created_date,
      deleted             AS is_deleted,
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category') }}

    FROM source

)

SELECT *
FROM renamed
