WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_award_emoji_dedupe_source') }}

)

SELECT
  id::NUMBER              AS award_emoji_id,
  name::VARCHAR           AS award_emoji_name,
  user_id::NUMBER         AS user_id,
  awardable_id::NUMBER    AS awardable_id,
  awardable_type::VARCHAR AS awardable_type,
  created_at::TIMESTAMP   AS created_at,
  updated_at::TIMESTAMP   AS updated_at
FROM source
