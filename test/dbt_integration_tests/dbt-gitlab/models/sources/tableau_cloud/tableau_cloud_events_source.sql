WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','events') }}
),

renamed AS (
  SELECT
    "Site Luid"::VARCHAR                      AS site_luid,
    "Historical Project Name"::VARCHAR        AS historical_project_name,
    "Site Name"::VARCHAR                      AS site_name,
    "Actor User Id"::NUMBER                   AS actor_user_id,
    "Target User Id"::NUMBER                  AS target_user_id,
    "Actor User Name"::VARCHAR                AS actor_user_name,
    "Event Name"::VARCHAR                     AS event_name,
    "Event Type"::VARCHAR                     AS event_type,
    "Event Id"::NUMBER                        AS event_id,
    "Event Date"::TIMESTAMP                   AS event_at,
    "Actor License Role"::VARCHAR             AS actor_license_role,
    "Actor Site Role"::VARCHAR                AS actor_site_role,
    "Item Type"::VARCHAR                      AS item_type,
    "Item Id"::NUMBER                         AS item_id,
    "Item LUID"::VARCHAR                      AS item_luid,
    "Item Name"::VARCHAR                      AS item_name,
    "Workbook Name"::VARCHAR                  AS workbook_name,
    "Historical Item Name"::VARCHAR           AS historical_item_name,
    "Project Name"::VARCHAR                   AS project_name,
    "Item Owner Id"::NUMBER                   AS item_owner_id,
    "Item Owner Email"::VARCHAR               AS item_owner_email,
    "Item Repository Url"::VARCHAR            AS item_repository_url,
    "Historical Item Repository Url"::VARCHAR AS historical_item_repository_url,
    "Admin Insights Published At"::VARCHAR    AS admin_insights_published_at,
    uploaded_at::TIMESTAMP                    AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY site_luid, event_id ORDER BY admin_insights_published_at DESC) = 1
