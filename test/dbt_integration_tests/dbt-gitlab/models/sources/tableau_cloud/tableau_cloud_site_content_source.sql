WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','tableau_cloud_site_content') }}
),

renamed AS (
  SELECT
    created_at::TIMESTAMP                         AS created_at,
    data_source_content_type::VARCHAR             AS data_source_content_type,
    data_source_database_type::VARCHAR            AS data_source_database_type,
    data_source_is_certified::VARCHAR             AS data_source_is_certified,
    item_description::VARCHAR                          AS item_description,
    extracts_incremented_at::TIMESTAMP            AS extracts_incremented_at,
    extracts_refreshed_at::TIMESTAMP              AS extracts_refreshed_at,
    first_published_at::TIMESTAMP                 AS first_published_at,
    has_incrementable_extract::BOOLEAN            AS has_incrementable_extract,
    has_refresh_scheduled::BOOLEAN                AS has_refresh_scheduled,
    has_refreshable_extract::BOOLEAN              AS has_refreshable_extract,
    is_data_extract::BOOLEAN                      AS is_data_extract,
    item_hyperlink::VARCHAR                       AS item_hyperlink,
    item_id::NUMBER                               AS item_id,
    item_luid::VARCHAR                            AS item_luid,
    item_name::VARCHAR                            AS item_name,
    item_parent_project_id::NUMBER                AS item_parent_project_id,
    item_parent_project_level::NUMBER             AS item_parent_project_level,
    item_parent_project_name::VARCHAR             AS item_parent_project_name,
    item_parent_project_owner_email::VARCHAR      AS item_parent_project_owner_email,
    item_revision::NUMBER                         AS item_revision,
    item_type::VARCHAR                            AS item_type,
    last_accessed_at::TIMESTAMP                   AS last_accessed_at,
    last_published_at::TIMESTAMP                  AS last_published_at,
    owner_email::VARCHAR                          AS owner_email,
    project_level::NUMBER                         AS project_level,
    is_controlled_permissions_enabled::BOOLEAN       AS is_controlled_permissions_enabled,
    is_nested_projects_permissions_enabled::BOOLEAN  AS is_nested_projects_permissions_enabled,
    controlling_permissions_project_luid::VARCHAR AS controlling_permissions_project_luid,
    controlling_permissions_project_name::VARCHAR AS controlling_permissions_project_name,
    site_hyperlink::VARCHAR                       AS site_hyperlink,
    site_luid::VARCHAR                            AS site_luid,
    site_name::VARCHAR                            AS site_name,
    size_bytes::NUMBER                          AS size_bytes,
    size_mb::FLOAT                              AS size_mb,
    total_size_bytes::NUMBER                    AS total_size_bytes,
    total_size_mb::FLOAT                        AS total_size_mb,
    storage_quota_bytes::NUMBER                 AS storage_quota_bytes,
    top_parent_project_name::VARCHAR              AS top_parent_project_name,
    updated_at::VARCHAR                           AS updated_at,
    view_title::VARCHAR                           AS view_title,
    view_type::VARCHAR                            AS view_type,
    view_workbook_id::VARCHAR                     AS view_workbook_id,
    view_workbook_name::VARCHAR                   AS view_workbook_name,
    does_workbook_shows_sheets_as_tabs::BOOLEAN        AS does_workbook_shows_sheets_as_tabs,
    admin_insights_published_at::VARCHAR          AS admin_insights_published_at,
    uploaded_at::VARCHAR                            AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
