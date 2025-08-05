WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','dup_tableau_cloud_users') }}
),

renamed AS (
  SELECT
    site_luid::VARCHAR                              AS site_luid,
    site_name::VARCHAR                              AS site_name,
    allowed_creators::NUMBER                        AS allowed_creators,
    allowed_explorers::NUMBER                       AS allowed_explorers,
    total_allowed_licenses::NUMBER                  AS total_allowed_licenses,
    allowed_viewers::NUMBER                         AS allowed_viewers,
    user_id::NUMBER                                 AS user_id,
    user_luid::VARCHAR                              AS user_luid,
    user_email::VARCHAR                             AS user_email,
    user_name::VARCHAR                              AS user_name,
    user_friendly_name::VARCHAR                     AS user_friendly_name,
    user_creation_at::TIMESTAMP                   AS user_creation_at,
    user_account_age::NUMBER                        AS user_account_age,
    last_login_at::TIMESTAMP                      AS last_login_at,
    days_since_last_login::NUMBER                   AS days_since_last_login,
    user_license_type::VARCHAR                      AS user_license_type,
    user_site_role::VARCHAR                         AS user_site_role,
    user_projects::NUMBER                                AS user_projects,
    user_data_sources::NUMBER                           AS user_data_sources,
    certified_data_sources::NUMBER                  AS certified_data_sources,
    size_of_data_sources_mb::FLOAT                AS size_of_data_sources_mb,
    user_workbooks::NUMBER                               AS user_workbooks,
    size_of_workbooks_mb::FLOAT                   AS size_of_workbooks_mb,
    user_views::NUMBER                                   AS user_views,
    access_events_data_sources::NUMBER            AS access_events_data_sources,
    access_events_views::NUMBER                   AS access_events_views,
    publish_events_data_sources::NUMBER          AS publish_events_data_sources,
    publish_events_workbooks::NUMBER              AS publish_events_workbooks,
    data_source_last_access_at::TIMESTAMP       AS data_source_last_access_at,
    data_source_last_publish_at::TIMESTAMP      AS data_source_last_publish_at,
    view_last_access_at::TIMESTAMP              AS view_last_access_at,
    workbook_last_publish_at::TIMESTAMP         AS workbook_last_publish_at,
    web_authoring_last_access_at::TIMESTAMP     AS web_authoring_last_access_at,
    total_traffic_data_sources::NUMBER            AS total_traffic_data_sources,
    unique_visitors_data_sources::NUMBER          AS unique_visitors_data_sources,
    total_traffic_views::NUMBER                   AS total_traffic_views,
    unique_visitors_views::NUMBER                 AS unique_visitors_views,
    admin_insights_published_at::VARCHAR            AS admin_insights_published_at,
    tableau_desktop_last_access_at::TIMESTAMP   AS tableau_desktop_last_access_at,
    tableau_desktop_last_product_version::VARCHAR AS tableau_desktop_last_product_version,
    tableau_prep_last_access_at::TIMESTAMP      AS tableau_prep_last_access_at,
    tableau_prep_last_product_version::VARCHAR    AS tableau_prep_last_product_version,
    site_version::VARCHAR                           AS site_version,
    total_occupied_licenses::NUMBER                 AS total_occupied_licenses,
    total_remaining_licenses::NUMBER                AS total_remaining_licenses,
    occupied_explorer_licenses::NUMBER              AS occupied_explorer_licenses,
    occupied_viewer_licenses::NUMBER                AS occupied_viewer_licenses,
    occupied_creator_licenses::NUMBER               AS occupied_creator_licenses,
    uploaded_at::TIMESTAMP                            AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
