WITH greenhouse_candidates_tags_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidates_tags_source') }}
),

greenhouse_tags_source AS (
  SELECT *
  FROM {{ ref('greenhouse_tags_source') }}
),

cand_tags AS (
  SELECT *
  FROM greenhouse_candidates_tags_source
),

tags AS (
  SELECT *
  FROM greenhouse_tags_source
)

SELECT
  cand_tags.candidate_id,
  cand_tags.candidate_tag_id,
  cand_tags.tag_id,
  tags.tag_name,
  cand_tags.candidate_tag_created_at,
  cand_tags.candidate_tag_updated_at
FROM cand_tags
LEFT JOIN tags ON cand_tags.tag_id = tags.tag_id
