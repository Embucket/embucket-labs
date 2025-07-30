WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','tableau_cloud_groups') }}
),

renamed AS (
  SELECT
    site_luid::VARCHAR                     AS site_luid,
    group_luid::VARCHAR                    AS group_luid,
    user_luid::VARCHAR                     AS user_luid,
    site_name::VARCHAR                     AS site_name,
    group_name::VARCHAR                    AS group_name,
    user_email::VARCHAR                    AS user_email,
    group_minium_site_role::VARCHAR       AS group_minium_site_role,
    is_license_on_sign_in::BOOLEAN   AS is_license_on_sign_in,
    admin_insights_published_at::TIMESTAMP AS admin_insights_published_at,
    uploaded_at::TIMESTAMP                   AS uploaded_at
  FROM source
)

SELECT *
FROM renamed
