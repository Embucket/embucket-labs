WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_gpg_signatures_source') }}

)

SELECT *
FROM source
