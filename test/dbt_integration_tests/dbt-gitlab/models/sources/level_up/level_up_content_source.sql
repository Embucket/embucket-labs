{{ level_up_intermediate('content') }}

parsed AS (
  SELECT
    value['asset']::VARCHAR                   AS asset,
    value['assetAltText']::VARCHAR            AS asset_alt_text,
    value['authorsAndInstructors']::VARIANT   AS authors_and_instructors,
    value['contentTypeLabel']::VARCHAR        AS content_type_label,
    value['courseEndDate']::TIMESTAMP         AS course_end_date,
    value['courseStartDate']::TIMESTAMP       AS course_start_date,
    value['createdAt']::TIMESTAMP             AS created_at,
    value['customFields']::VARIANT            AS custom_fields,
    value['description']::VARCHAR             AS description,
    value['enrollmentCount']::INT             AS enrollment_count,
    value['enrollmentEndDate']::TIMESTAMP     AS enrollment_end_date,
    value['enrollmentStartDate']::TIMESTAMP   AS enrollment_start_date,
    value['freeWithRegistration']::BOOLEAN    AS is_free_with_registration,
    value['hasChildren']::BOOLEAN             AS has_children,
    value['id']::VARCHAR                      AS content_id,
    value['kind']::VARCHAR                    AS kind,
    value['language']::VARCHAR                AS language,
    value['metaDescription']::VARCHAR         AS meta_description,
    value['metaTitle']::VARCHAR               AS meta_title,
    value['priceInCents']::INT                AS price_in_cents,
    value['seatsLimit']::INT                  AS seats_limit,
    value['sku']::VARCHAR                     AS sku,
    value['slug']::VARCHAR                    AS slug,
    value['source']::VARCHAR                  AS source,
    value['status']::VARCHAR                  AS status,
    value['suggestedRetailPriceInCents']::INT AS suggested_retail_price_in_cents,
    value['tags']::VARIANT                    AS tags,
    value['title']::VARCHAR                   AS title,
    value['updatedAt']::TIMESTAMP             AS updated_at,
    value['url']::VARCHAR                     AS url,
    value['waitlistCount']::INT               AS waitlist_count,
    value['waitlistingEnabled']::BOOLEAN      AS is_waitlisting_enabled,
    value['waitlistingTriggered']::BOOLEAN    AS is_waitlisting_triggered,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        content_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
