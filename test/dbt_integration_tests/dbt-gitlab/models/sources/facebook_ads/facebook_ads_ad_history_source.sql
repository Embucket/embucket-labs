WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','ad_history') }}
),

renamed AS (

  SELECT
    id::NUMBER                  AS ad_id,
    updated_time::TIMESTAMP     AS updated_time,
    account_id::NUMBER          AS account_id,
    campaign_id::NUMBER         AS campaign_id,
    creative_id::NUMBER         AS creative_id,
    name::VARCHAR               AS ad_name,
    status::VARCHAR             AS ad_status,
    ad_set_id::NUMBER           AS ad_set_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
