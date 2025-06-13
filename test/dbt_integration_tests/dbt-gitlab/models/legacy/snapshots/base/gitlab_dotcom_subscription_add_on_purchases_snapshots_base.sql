{{ config({
    "alias": "gitlab_dotcom_subscription_add_on_purchases_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_subscription_add_on_purchases_snapshots') }}
    
), renamed AS (

  SELECT
    id::NUMBER                                                      AS id,
    created_at::TIMESTAMP                                           AS created_at,
    updated_at::TIMESTAMP                                           AS updated_at,
    subscription_add_on_id::NUMBER                                  AS subscription_add_on_id,
    namespace_id::NUMBER                                            AS namespace_id,
    quantity::NUMBER                                                AS quantity,
    expires_on::TIMESTAMP                                           AS expires_on,
    purchase_xid::VARCHAR                                           AS purchase_xid,
    dbt_valid_from::TIMESTAMP                                       AS valid_from,
    dbt_valid_to::TIMESTAMP                                         AS valid_to
  FROM source
    
), final AS (

  SELECT 
    renamed.id,
    renamed.created_at,
    renamed.updated_at,
    renamed.subscription_add_on_id,
    renamed.namespace_id,
    renamed.quantity,
    renamed.expires_on,
    -- This is an temprary addition until the base table is fixed.
    -- More details about the existing issues with the base table (https://gitlab.com/gitlab-data/product-analytics/-/issues/2244#note_2183442993)
    CASE 
      -- Handle specific cases where upgrades occurred before snapshot creation
      WHEN renamed.purchase_xid IN ('A-S00017713','A-S00031556','A-S00043856','A-S00078868','A-S00096791','A-S00107013') THEN renamed.updated_at
      -- Detect upgrades/downgrades by comparing current and previous subscription_add_on_id
      WHEN renamed.subscription_add_on_id != LAG(renamed.subscription_add_on_id) OVER (PARTITION BY renamed.namespace_id ORDER BY renamed.valid_from) THEN valid_from
      -- Default case: use created_at for all other scenarios
      ELSE renamed.created_at
    END                                                             AS corrected_purchase_date,
    -- If the subscription upgrades then the existing purchase will be valid till the next purchase, 
    CASE
      -- sometimes expires_on is future dated in those cases valid_to will be used
      WHEN renamed.valid_to IS NOT NULL THEN renamed.valid_to::DATE
      -- if valid_to is null and expires_on is not future dated then use expires_on
      WHEN renamed.expires_on <= CURRENT_DATE() THEN renamed.expires_on::DATE
      -- for all other scenarios use current date
      ELSE CURRENT_DATE()
    END                                                             AS corrected_expires_on_date,
    renamed.purchase_xid,
    renamed.valid_from,
    renamed.valid_to
  FROM renamed

)

SELECT *
FROM final

