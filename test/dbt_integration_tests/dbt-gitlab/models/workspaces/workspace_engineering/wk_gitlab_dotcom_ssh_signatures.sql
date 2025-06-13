WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ssh_signatures_source') }}

)

SELECT *
FROM source
