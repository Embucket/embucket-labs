WITH source AS (

    SELECT

      id                                            AS cost_id,
      modified_date                                 AS modified_date,
      cost_date                                     AS cost_date,
      source                                        AS source,
      cost_in_micro                                 AS cost_in_micro,
      clicks                                        AS clicks,
      impressions                                   AS impressions,
      estimated_total_possible_impressions          AS estimated_total_possible_impressions,
      ad_provider                                   AS ad_provider,
      channel_unique_id                             AS channel_unique_id,
      channel_name                                  AS channel_name,
      channel_is_aggregatable_cost                  AS channel_is_aggregatable_cost,
      advertiser_unique_id                          AS advertiser_unique_id,
      advertiser_name                               AS advertiser_name,
      advertiser_is_aggregatable_cost               AS advertiser_is_aggregatable_cost,
      account_unique_id                             AS account_unique_id,
      account_name                                  AS account_name,
      account_is_aggregatable_cost                  AS account_is_aggregatable_cost,
      campaign_unique_id                            AS campaign_unique_id,
      campaign_name                                 AS campaign_name,
      campaign_is_aggregatable_cost                 AS campaign_is_aggregatable_cost,
      ad_group_unique_id                            AS ad_group_unique_id,
      ad_group_name                                 AS ad_group_name,
      ad_group_is_aggregatable_cost                 AS ad_group_is_aggregatable_cost,
      ad_unique_id                                  AS ad_unique_id,
      ad_name                                       AS ad_name,
      ad_is_aggregatable_cost                       AS ad_is_aggregatable_cost,
      creative_unique_id                            AS creative_unique_id,
      creative_name                                 AS creative_name,
      creative_is_aggregatable_cost                 AS creative_is_aggregatable_cost,
      keyword_unique_id                             AS keyword_unique_id,
      keyword_name                                  AS keyword_name,
      keyword_is_aggregatable_cost                  AS keyword_is_aggregatable_cost,
      placement_unique_id                           AS placement_unique_id,
      placement_name                                AS placement_name,
      placement_is_aggregatable_cost                AS placement_is_aggregatable_cost,
      site_unique_id                                AS site_unique_id,
      site_name                                     AS site_name,
      site_is_aggregatable_cost                     AS site_is_aggregatable_cost,
      is_deleted                                    AS is_deleted,
      iso_currency_code                             AS iso_currency_code,
      source_id                                     AS source_id,
      row_key                                       AS row_key,
      account_row_key                               AS account_row_key,
      advertiser_row_key                            AS advertiser_row_key,
      site_row_key                                  AS site_row_key,
      placement_row_key                             AS placement_row_key,
      campaign_row_key                              AS campaign_row_key,
      ad_row_key                                    AS ad_row_key,
      ad_group_row_key                              AS ad_group_row_key,
      creative_row_key                              AS creative_row_key,
      keyword_row_key                               AS keyword_row_key,
      currency_id                                   AS currency_id,
      _created_date                                 AS _created_date,
      _modified_date                                AS _modified_date,
      _deleted_date                                 AS _deleted_date

    FROM {{ source('bizible', 'biz_costs') }}
 
)

SELECT *
FROM source

