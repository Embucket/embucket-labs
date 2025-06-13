WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_security_scans_source') }}

)

SELECT *
FROM source
