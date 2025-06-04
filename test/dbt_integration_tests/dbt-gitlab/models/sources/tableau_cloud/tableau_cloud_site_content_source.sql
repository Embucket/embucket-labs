WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','site_content') }}
),

renamed AS (
  SELECT
    "Created At"::TIMESTAMP                         AS created_at,
    "Data Source Content Type"::VARCHAR             AS data_source_content_type,
    "Data Source Database Type"::VARCHAR            AS data_source_database_type,
    "Data Source Is Certified"::VARCHAR             AS data_source_is_certified,
    "Description"::VARCHAR                          AS item_description,
    "Extracts Incremented At"::TIMESTAMP            AS extracts_incremented_at,
    "Extracts Refreshed At"::TIMESTAMP              AS extracts_refreshed_at,
    "First Published At"::TIMESTAMP                 AS first_published_at,
    "Has Incrementable Extract"::BOOLEAN            AS has_incrementable_extract,
    "Has Refresh Scheduled"::BOOLEAN                AS has_refresh_scheduled,
    "Has Refreshable Extract"::BOOLEAN              AS has_refreshable_extract,
    "Is Data Extract"::BOOLEAN                      AS is_data_extract,
    "Item Hyperlink"::VARCHAR                       AS item_hyperlink,
    "Item ID"::NUMBER                               AS item_id,
    "Item LUID"::VARCHAR                            AS item_luid,
    "Item Name"::VARCHAR                            AS item_name,
    "Item Parent Project ID"::NUMBER                AS item_parent_project_id,
    "Item Parent Project Level"::NUMBER             AS item_parent_project_level,
    "Item Parent Project Name"::VARCHAR             AS item_parent_project_name,
    "Item Parent Project Owner Email"::VARCHAR      AS item_parent_project_owner_email,
    "Item Revision"::NUMBER                         AS item_revision,
    "Item Type"::VARCHAR                            AS item_type,
    "Last Accessed At"::TIMESTAMP                   AS last_accessed_at,
    "Last Published At"::TIMESTAMP                  AS last_published_at,
    "Owner Email"::VARCHAR                          AS owner_email,
    "Project Level"::NUMBER                         AS project_level,
    "Controlled Permissions Enabled"::BOOLEAN       AS is_controlled_permissions_enabled,
    "Nested Projects Permissions Enabled"::BOOLEAN  AS is_nested_projects_permissions_enabled,
    "Controlling Permissions Project LUID"::VARCHAR AS controlling_permissions_project_luid,
    "Controlling Permissions Project Name"::VARCHAR AS controlling_permissions_project_name,
    "Site Hyperlink"::VARCHAR                       AS site_hyperlink,
    "Site LUID"::VARCHAR                            AS site_luid,
    "Site Name"::VARCHAR                            AS site_name,
    "Size (bytes)"::NUMBER                          AS size_bytes,
    "Size (MB)"::FLOAT                              AS size_mb,
    "Total Size (bytes)"::NUMBER                    AS total_size_bytes,
    "Total Size (MB)"::FLOAT                        AS total_size_mb,
    "Storage Quota (bytes)"::NUMBER                 AS storage_quota_bytes,
    "Top Parent Project Name"::VARCHAR              AS top_parent_project_name,
    "Updated At"::VARCHAR                           AS updated_at,
    "View Title"::VARCHAR                           AS view_title,
    "View Type"::VARCHAR                            AS view_type,
    "View Workbook ID"::VARCHAR                     AS view_workbook_id,
    "View Workbook Name"::VARCHAR                   AS view_workbook_name,
    "Workbook Shows Sheets As Tabs"::BOOLEAN        AS does_workbook_shows_sheets_as_tabs,
    "Admin Insights Published At"::VARCHAR          AS admin_insights_published_at,
    uploaded_at::VARCHAR                            AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
