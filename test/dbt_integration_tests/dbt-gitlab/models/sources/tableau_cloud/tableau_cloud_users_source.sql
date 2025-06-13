WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','users') }}
),

renamed AS (
  SELECT
    "Site Luid"::VARCHAR                              AS site_luid,
    "Site Name"::VARCHAR                              AS site_name,
    "Allowed Creators"::NUMBER                        AS allowed_creators,
    "Allowed Explorers"::NUMBER                       AS allowed_explorers,
    "Total Allowed Licenses"::NUMBER                  AS total_allowed_licenses,
    "Allowed Viewers"::NUMBER                         AS allowed_viewers,
    "User ID"::NUMBER                                 AS user_id,
    "User LUID"::VARCHAR                              AS user_luid,
    "User Email"::VARCHAR                             AS user_email,
    "User Name"::VARCHAR                              AS user_name,
    "User Friendly Name"::VARCHAR                     AS user_friendly_name,
    "User Creation Date"::TIMESTAMP                   AS user_creation_at,
    "User Account Age"::NUMBER                        AS user_account_age,
    "Last Login Date"::TIMESTAMP                      AS last_login_at,
    "Days Since Last Login"::NUMBER                   AS days_since_last_login,
    "User License Type"::VARCHAR                      AS user_license_type,
    "User Site Role"::VARCHAR                         AS user_site_role,
    "Projects"::NUMBER                                AS user_projects,
    "Data Sources "::NUMBER                           AS user_data_sources,
    "Certified Data Sources"::NUMBER                  AS certified_data_sources,
    "Size of Data Sources (MB)"::FLOAT                AS size_of_data_sources_mb,
    "Workbooks"::NUMBER                               AS user_workbooks,
    "Size of Workbooks (MB)"::FLOAT                   AS size_of_workbooks_mb,
    "Views"::NUMBER                                   AS user_views,
    "Access Events - Data Sources"::NUMBER            AS access_events_data_sources,
    "Access Events - Views"::NUMBER                   AS access_events_views,
    "Publish Events - Data Sources "::NUMBER          AS publish_events_data_sources,
    "Publish Events - Workbooks"::NUMBER              AS publish_events_workbooks,
    "Data Source - Last Access Date"::TIMESTAMP       AS data_source_last_access_at,
    "Data Source - Last Publish Date"::TIMESTAMP      AS data_source_last_publish_at,
    "View - Last Access Date"::TIMESTAMP              AS view_last_access_at,
    "Workbook - Last Publish Date"::TIMESTAMP         AS workbook_last_publish_at,
    "Web Authoring - Last Access Date"::TIMESTAMP     AS web_authoring_last_access_at,
    "Total Traffic - Data Sources"::NUMBER            AS total_traffic_data_sources,
    "Unique Visitors - Data Sources"::NUMBER          AS unique_visitors_data_sources,
    "Total Traffic - Views"::NUMBER                   AS total_traffic_views,
    "Unique Visitors - Views"::NUMBER                 AS unique_visitors_views,
    "Admin Insights Published At"::VARCHAR            AS admin_insights_published_at,
    "Tableau Desktop - Last Access Date"::TIMESTAMP   AS tableau_desktop_last_access_at,
    "Tableau Desktop - Last Product Version"::VARCHAR AS tableau_desktop_last_product_version,
    "Tableau Prep - Last Access Date"::TIMESTAMP      AS tableau_prep_last_access_at,
    "Tableau Prep - Last Product Version"::VARCHAR    AS tableau_prep_last_product_version,
    "Site Version"::VARCHAR                           AS site_version,
    "Total Occupied Licenses"::NUMBER                 AS total_occupied_licenses,
    "Total Remaining Licenses"::NUMBER                AS total_remaining_licenses,
    "Occupied Explorer Licenses"::NUMBER              AS occupied_explorer_licenses,
    "Occupied Viewer Licenses"::NUMBER                AS occupied_viewer_licenses,
    "Occupied Creator Licenses"::NUMBER               AS occupied_creator_licenses,
    uploaded_at::TIMESTAMP                            AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
