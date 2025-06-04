WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','permissions') }}
),

renamed AS (
  SELECT
    "Site LUID"::VARCHAR                            AS site_luid,
    "Grantee LUID"::VARCHAR                         AS grantee_luid,
    "Grantee Type"::VARCHAR                         AS grantee_type,
    "Item LUID"::VARCHAR                            AS item_luid,
    "Item Type"::VARCHAR                            AS item_type,
    "Capability Type"::VARCHAR                      AS capability_type,
    "Site Name"::VARCHAR                            AS site_name,
    "Grantee Name"::VARCHAR                         AS grantee_name,
    "Item Name"::VARCHAR                            AS item_name,
    "Item Hyperlink"::VARCHAR                       AS item_hyperlink,
    "Item Parent Project Name"::VARCHAR             AS item_parent_project_name,
    "Top Parent Project Name"::VARCHAR              AS top_parent_project_name,
    "Controlling Permissions Project Name"::VARCHAR AS controlling_permissions_project_name,
    "Permission Value"::NUMBER                      AS permission_value,
    "Permission Description"::VARCHAR               AS permission_description,
    NULLIF("User Email"::VARCHAR, 'NA')             AS user_email,
    NULLIF("User LUID"::VARCHAR, 'NA')              AS user_luid,
    NULLIF("User Site Role"::VARCHAR, 'NA')         AS user_site_role,
    "Admin Insights Published At"::TIMESTAMP        AS admin_insights_published_at,
    "Has Permission?"::BOOLEAN                      AS has_permission, -- noqa: L057
    uploaded_at::TIMESTAMP                          AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY site_luid, grantee_luid, item_luid, capability_type, permission_value ORDER BY uploaded_at DESC) = 1
