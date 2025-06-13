/*
    Each Platform has different ways they organize campaigns.
    The table below is taken from Linkedin Docs that has an accurate comparison

    +-------+-------------+----------+----------------+
    | Level |   Google    | Facebook |    LinkedIn    |
    +-------+-------------+----------+----------------+
    |     1 | Campaign    | Campaign | Campaign Group |
    |     2 | Ad Group    | Ad Set   | Campaign       |
    |     3 | Ad Keyword* | Ad       | Creative       |
    +-------+-------------+----------+----------------+
    |     4 |             | Creative |                |

    Facebook has another level to get the destination url called creative.
    *Fivetran reanmes this layer to `Ad`

    This union query prefers the Facebook naming since its the most generic and
    most marketers understand a `campaign` AS the top level.

*/

WITH union_sources AS (
  SELECT
    google_ads.ad_stats_date                         AS report_date,
    'google_ads'                                     AS platform,

    google_ads.campaign_id,
    google_ads.campaign_name,
    google_ads.campaign_status,
    google_ads.campaign_end_date,
    google_ads.campaign_start_date,

    google_ads.ad_status,
    NULL                                             AS creative_type,
    LTRIM(RTRIM(google_ads.ad_final_urls, ']'), '[') AS landing_page_url,


    SUM(impressions)                                 AS impressions,
    SUM(clicks)                                      AS clicks,
    NULL                                             AS linkedin_leads,
    SUM(cost_micros / 1000000)                       AS spend

  FROM
    {{ ref('google_ads') }}
  {{ dbt_utils.group_by(n=10) }}

  UNION ALL

  SELECT
    linkedin_ads.campaign_day    AS report_date,
    'linkedin_ads'               AS platform,

    linkedin_ads.campaign_id,
    linkedin_ads.campaign_name,
    linkedin_ads.campaign_status,
    linkedin_ads.campaign_end_date,
    linkedin_ads.campaign_start_date,

    linkedin_ads.creative_status AS ad_status,
    linkedin_ads.creative_type,
    linkedin_ads.click_uri       AS landing_page_url,

    SUM(impressions)             AS impressions,
    SUM(clicks)                  AS clicks,
    SUM(linkedin_leads)          AS linkedin_leads,
    SUM(cost_in_usd)             AS spend

  FROM
    {{ ref('linkedin_ads') }}
  {{ dbt_utils.group_by(n=10) }}

  UNION ALL

  SELECT
    facebook_ads.campaign_day AS report_date,
    'facebook_ads'            AS platform,

    facebook_ads.campaign_id,
    facebook_ads.campaign_name,
    NULL                      AS campaign_status,
    NULL                      AS campaign_end_date,
    NULL                      AS campaign_start_date,

    facebook_ads.ad_status,
    facebook_ads.creative_type,
    facebook_ads.page_link    AS landing_page_url,

    SUM(impressions)          AS impressions,
    SUM(inline_link_clicks)   AS clicks,
    NULL                      AS linkedin_leads,
    SUM(spend)                AS spend

  FROM
    {{ ref('facebook_ads') }}
  {{ dbt_utils.group_by(n=10) }}

),

parse_utms AS (

  SELECT
    union_sources.*,
    -- keywords,
    PARSE_URL(landing_page_url) :parameters:utm_medium::string   AS utm_medium,
    PARSE_URL(landing_page_url) :parameters:utm_source::string   AS utm_source,
    PARSE_URL(landing_page_url) :parameters:utm_budget::string   AS utm_budget,
    PARSE_URL(landing_page_url) :parameters:utm_campaign::string AS utm_campaign,

    SPLIT_PART(utm_campaign, '_', 1)                            AS utm_campaigncode,
    SPLIT_PART(utm_campaign, '_', 2)                            AS utm_geo,
    SPLIT_PART(utm_campaign, '_', 3)                            AS utm_targeting,
    SPLIT_PART(utm_campaign, '_', 4)                            AS utm_ad_unit,
    SPLIT_PART(utm_campaign, '_', 5)                            AS utm_br_bn,
    SPLIT_PART(utm_campaign, '_', 6)                            AS utm_matchtype,

    PARSE_URL(landing_page_url) :parameters:utm_content::string  AS utm_content,
    SPLIT_PART(utm_content, '_', 1)                             AS utm_contentcode,
    SPLIT_PART(utm_content, '_', 2)                             AS utm_team,
    SPLIT_PART(utm_content, '_', 3)                             AS utm_segment,
    SPLIT_PART(utm_content, '_', 4)                             AS utm_language
  FROM
    union_sources

)

SELECT *
FROM
  parse_utms
