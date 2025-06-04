WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_subscription_add_on_purchases_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                        AS id,
      subscription_add_on_id::NUMBER    AS subscription_add_on_id,
      namespace_id::NUMBER              AS namespace_id,
      quantity::NUMBER                  AS quantity,
      expires_on::TIMESTAMP             AS expires_on,
      purchase_xid::VARCHAR             AS purchase_xid,
      created_at::TIMESTAMP             AS created_at,
      updated_at::TIMESTAMP             AS updated_at

    FROM source

)

SELECT  *
FROM renamed
