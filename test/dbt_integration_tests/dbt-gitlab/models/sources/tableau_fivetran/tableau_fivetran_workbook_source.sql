WITH source AS (
  SELECT *
  FROM {{ source('tableau_fivetran','workbook') }}
),

renamed AS (
  SELECT
    id::VARCHAR                             AS workbook_id,
    owner_id::VARCHAR                       AS owner_id,
    project_id::VARCHAR                     AS project_id,
    show_tab::BOOLEAN                       AS show_tab,
    created_at::TIMESTAMP                   AS created_at,
    webpage_url::VARCHAR                    AS webpage_url,
    description::VARCHAR                    AS workbook_description,
    location_id::VARCHAR                    AS location_id,
    location_type::VARCHAR                  AS location_type,
    location_name::VARCHAR                  AS project_name,
    updated_at::TIMESTAMP                   AS updated_at,
    size::NUMBER                            AS workbook_size,
    default_view_id::VARCHAR                AS default_view_id,
    name::VARCHAR                           AS workbook_name,
    content_url::VARCHAR                    AS workbook_content_url
  
  FROM source
  WHERE NOT _fivetran_deleted

)

SELECT *
FROM renamed


