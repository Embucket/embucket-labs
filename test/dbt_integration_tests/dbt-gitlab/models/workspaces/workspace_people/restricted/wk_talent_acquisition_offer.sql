WITH greenhouse_offers_source AS (
  SELECT *
  FROM {{ ref('greenhouse_offers_source') }}
),

greenhouse_offer_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_offer_custom_fields_source') }}
),

workday_hcm_organization_source AS (
  SELECT *
  FROM {{ ref('workday_hcm_organization_source') }}
),

offers_source AS (
  SELECT
    offer_id,
    application_id,
    offer_status,
    created_by,
    start_date,
    created_at,
    sent_at,
    resolved_at,
    updated_at
  FROM greenhouse_offers_source
),

offers_custom AS (
  SELECT 
    offer_id,   
    MAX(
      CASE offer_custom_field
      WHEN 'Entity'
      THEN offer_custom_field_display_value
      END)                                        AS entity,
    MAX(
      CASE 
      WHEN offer_custom_field = 'Candidate Country'
        AND offer_custom_field_display_value = 'Korea, Republic of'
        THEN 'South Korea'
      WHEN offer_custom_field = 'Candidate Country'
        THEN offer_custom_field_display_value
      END)                                        AS candidate_country,      
  FROM greenhouse_offer_custom_fields_source
  WHERE offer_custom_field in ('Entity', 'Candidate Country')
  GROUP BY 1

),

country_source AS (
  SELECT 
    id                        AS country_id,
    name                      AS country_name,
    superior_organization_id  AS region_id 
  FROM workday_hcm_organization_source
  WHERE type = 'Location_Hierarchy'
    AND sub_type = 'Country'
),

region_source AS (
  SELECT 
    id                        AS region_id,
    name                      AS region_name
  FROM workday_hcm_organization_source
  WHERE type = 'Location_Hierarchy'
    AND sub_type = 'Region'
),

region_mapping AS (
  SELECT 
    country_source.country_id,
    country_source.country_name,
    country_source.region_id,
    region_source.region_name
  FROM country_source
  LEFT JOIN region_source on country_source.region_id = region_source.region_id
),

final AS (
  SELECT 
    offers_source.offer_id,
    offers_source.application_id,
    offers_source.offer_status,
    offers_source.created_by,
    offers_source.start_date,
    offers_source.created_at,
    offers_source.sent_at,
    offers_source.resolved_at,
    offers_source.updated_at,
    offers_custom.candidate_country,
    region_mapping.region_name AS candidate_region
  FROM offers_source
  LEFT JOIN offers_custom on offers_source.offer_id = offers_custom.offer_id
  LEFT JOIN region_mapping on offers_custom.candidate_country = region_mapping.country_name
  WHERE offers_source.sent_at IS NOT NULL
  )

SELECT *
FROM final
