WITH source AS (
  SELECT *
  FROM {{ source('zip_sandbox','vendors') }}
),

intermediate AS (
  SELECT
    data.value,
    source.uploaded_at
  FROM
    source
  INNER JOIN LATERAL FLATTEN(input => source.jsontext['data']) AS data
),

parsed AS (
  SELECT
    value['address']::VARIANT                AS address,
    value['alternate_name']::VARCHAR         AS alternate_name,
    value['alternate_name_usage']::VARCHAR   AS alternate_name_usage,
    value['categories']::VARIANT             AS categories,
    value['contacts']::VARIANT               AS contacts,
    value['created_at']::TIMESTAMP_NTZ       AS created_at,
    value['currencies']::VARIANT             AS currencies,
    value['currency']::VARCHAR               AS currency,
    value['department']::VARCHAR             AS department,
    value['description']::VARCHAR            AS description,
    value['duns_number']::VARCHAR            AS duns_number,
    value['external_data']::VARIANT          AS external_data,
    value['external_id']::VARCHAR            AS external_id,
    value['id']::VARCHAR                     AS id,
    value['last_reviewed_at']::TIMESTAMP_NTZ AS last_reviewed_at,
    value['legal_name']::VARCHAR             AS legal_name,
    value['name']::VARCHAR                   AS name,
    value['owners']::VARIANT                 AS owners,
    value['po_email']::VARCHAR               AS po_email,
    value['primary_phone']::VARCHAR          AS primary_phone,
    value['primary_phone_country']::VARCHAR  AS primary_phone_country,
    value['remittance_email']::VARCHAR       AS remittance_email,
    value['risk']::VARCHAR                   AS risk,
    value['status']::NUMBER                  AS status,
    value['subsidiaries']::VARIANT           AS subsidiaries,
    value['tax_id_number']::VARCHAR          AS tax_id_number,
    value['tax_id_type']::VARCHAR            AS tax_id_type,
    value['tier']::VARCHAR                   AS tier,
    value['type']::VARCHAR                   AS type,
    value['updated_at']::TIMESTAMP_NTZ       AS updated_at,
    value['vat_number']::VARCHAR             AS vat_number,
    value['website_link']::VARCHAR           AS website_link,
    uploaded_at
  FROM intermediate
)

SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1
