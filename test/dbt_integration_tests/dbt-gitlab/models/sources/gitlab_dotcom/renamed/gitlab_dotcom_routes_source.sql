WITH all_routes AS (

  SELECT 
    id::NUMBER                    AS route_id,
    source_id::NUMBER             AS source_id,
    source_type::VARCHAR          AS source_type,
    path::VARCHAR                 AS path
  FROM {{ ref('gitlab_dotcom_routes_dedupe_source') }}

),

internal_routes AS (

  SELECT 
    id::NUMBER                    AS internal_route_id,
    source_id::NUMBER             AS internal_source_id,
    source_type::VARCHAR          AS internal_source_type,
    path::VARCHAR                 AS internal_path
  FROM {{ ref('gitlab_dotcom_routes_internal_only_dedupe_source') }}

), 

combined AS (

SELECT
  all_routes.route_id                   AS route_id,
  all_routes.source_id                  AS source_id,
  internal_routes.internal_source_type  AS source_type,
  internal_routes.internal_path         AS path
FROM all_routes
LEFT JOIN internal_routes
  ON all_routes.route_id = internal_routes.internal_route_id

)

SELECT * 
FROM combined