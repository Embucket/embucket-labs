WITH source AS (
  SELECT *
  FROM
    {{ source('rally_initial_export', 'rally_initial_export_optouts') }}
)

SELECT *
FROM source
