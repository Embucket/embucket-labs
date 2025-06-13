WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','groups') }}
),

renamed AS (
  SELECT
    "Site LUID"::VARCHAR                     AS site_luid,
    "Group LUID"::VARCHAR                    AS group_luid,
    "User LUID"::VARCHAR                     AS user_luid,
    "Site Name"::VARCHAR                     AS site_name,
    "Group Name"::VARCHAR                    AS group_name,
    "User Email"::VARCHAR                    AS user_email,
    "Group Minimum Site Role"::VARCHAR       AS group_minium_site_role,
    "Group Is License On Sign In"::BOOLEAN   AS is_license_on_sign_in,
    "Admin Insights Published At"::TIMESTAMP AS admin_insights_published_at,
    uploaded_at::TIMESTAMP                   AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
