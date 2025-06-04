WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_award_emoji_source') }}

)

SELECT *
FROM source
