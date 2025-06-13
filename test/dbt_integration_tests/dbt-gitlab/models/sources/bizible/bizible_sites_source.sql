WITH source AS (

    SELECT
      id                        AS site_id,
      display_id                AS display_id,
      ad_account_unique_id      AS ad_account_unique_id,
      ad_account_name           AS ad_account_name,
      advertiser_unique_id      AS advertiser_unique_id,
      advertiser_name           AS advertiser_name,
      ad_group_unique_id        AS ad_group_unique_id,
      ad_group_name             AS ad_group_name,
      ad_campaign_unique_id     AS ad_campaign_unique_id,
      ad_campaign_name          AS ad_campaign_name,
      is_active                 AS is_active,
      is_deleted                AS is_deleted,
      modified_date             AS modified_date,
      first_imported            AS first_imported,
      name                      AS name,
      needs_update              AS needs_update,
      grouping_key              AS grouping_key,
      entity_type               AS entity_type,
      provider_type             AS provider_type,
      row_key                   AS row_key,
      _created_date             AS _created_date,
      _modified_date            AS _modified_date,
      _deleted_date             AS _deleted_date
    FROM {{ source('bizible', 'biz_sites') }}
 
)

SELECT *
FROM source