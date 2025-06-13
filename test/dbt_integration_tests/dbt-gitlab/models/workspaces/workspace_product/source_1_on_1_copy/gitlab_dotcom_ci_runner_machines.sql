WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_runner_machines_source') }}

)

SELECT *
FROM source
