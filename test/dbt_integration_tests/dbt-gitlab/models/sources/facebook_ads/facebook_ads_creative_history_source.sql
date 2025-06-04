WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','creative_history') }}
),

renamed AS (

  SELECT
    account_id::NUMBER                    AS account_id,
    id::NUMBER                            AS creative_id,
    body::VARCHAR                         AS body,
    link_destination_display_url::VARCHAR AS link_destination_display_url,
    link_url::VARCHAR                     AS link_url,
    name::VARCHAR                         AS creative_name,
    object_type::VARCHAR                  AS object_type,
    object_url::VARCHAR                   AS object_url,
    status::VARCHAR                       AS creative_status,
    title::VARCHAR                        AS title,
    page_link::VARCHAR                    AS page_link,
    _fivetran_id::VARCHAR                 AS _fivetran_id,
    _fivetran_synced::TIMESTAMP           AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
