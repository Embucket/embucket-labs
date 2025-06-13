
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_access_levels') }}

),

renamed AS (

  SELECT
    access::NUMBER AS access_level_id,
    label::VARCHAR AS access_level_label,
    name::VARCHAR AS access_level_name
  FROM source 
)

SELECT *
FROM renamed