
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_web_hooks_source') }}

)

SELECT *
FROM source
