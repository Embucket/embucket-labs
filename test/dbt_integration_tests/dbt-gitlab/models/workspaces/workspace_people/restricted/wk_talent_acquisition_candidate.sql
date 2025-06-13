WITH greenhouse_candidates_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidates_source') }}
),

greenhouse_candidate_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidate_custom_fields_source') }}
),

candidates_raw AS (
  SELECT *
  FROM {{ source('greenhouse', 'candidates') }}
),

candidate AS (
  SELECT
    c1.candidate_id,
    c1.candidate_company,
    c1.candidate_title,
    c1.candidate_created_at,
    c1.candidate_updated_at,
    c1.is_candidate_migrated,
    c1.is_candidate_private,
    c2.first_name,
    c2.last_name
  FROM greenhouse_candidates_source AS c1
  LEFT JOIN candidates_raw AS c2 ON c1.candidate_id = c2.id
),

candidate_custom AS (
  SELECT
    candidate_id,
    MAX(CASE candidate_custom_field
      WHEN 'Country'
        THEN candidate_custom_field_display_value
    END) AS country,
    MAX(CASE candidate_custom_field
      WHEN 'Locality'
        THEN candidate_custom_field_display_value
    END) AS locality,
    MAX(CASE candidate_custom_field
      WHEN 'Relationship'
        THEN candidate_custom_field_display_value
    END) AS relationship,
    MAX(CASE candidate_custom_field
      WHEN 'Family Relationship'
        THEN candidate_custom_field_display_value
    END) AS family_relationship,
    MAX(CASE candidate_custom_field
      WHEN 'Referral'
        THEN candidate_custom_field_display_value
    END) AS referral,
    MAX(CASE candidate_custom_field
      WHEN 'Preferred First Name/Nickname'
        THEN candidate_custom_field_display_value
    END) AS preferred_first,
    MAX(CASE candidate_custom_field
      WHEN 'Referral Notes'
        THEN candidate_custom_field_display_value
    END) AS referral_notes
  FROM greenhouse_candidate_custom_fields_source
  GROUP BY 1
)

SELECT
  candidate.*,
  cust.country,
  cust.locality,
  cust.relationship,
  cust.family_relationship,
  cust.referral,
  cust.preferred_first,
  cust.referral_notes
FROM candidate
LEFT JOIN candidate_custom AS cust ON candidate.candidate_id = cust.candidate_id
