WITH source AS (
  SELECT *
  FROM {{ source('zip_sandbox','requests') }}
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
    value['amount_usd']::VARCHAR         AS amount_usd,
    value['attachments']::VARIANT        AS attachments,
    value['canceled_at']::TIMESTAMP_NTZ  AS canceled_at,
    value['category']::VARIANT           AS category,
    value['completed_at']::TIMESTAMP_NTZ AS completed_at,
    value['created_at']::TIMESTAMP_NTZ   AS created_at,
    value['department']::VARIANT         AS department,
    value['description']::VARCHAR        AS description,
    value['engagement']::VARCHAR         AS engagement,
    value['external_data']::VARIANT      AS external_data,
    value['external_id']::NUMBER         AS external_id,
    value['gl_code']::VARCHAR            AS gl_code,
    value['id']::VARCHAR                 AS id,
    value['initiated_at']::TIMESTAMP_NTZ AS initiated_at,
    value['item_account']::VARCHAR       AS item_account,
    value['location']::VARCHAR           AS location,
    value['name']::VARCHAR               AS name,
    value['owner']::VARCHAR              AS owner,
    value['price_detail']::VARIANT       AS price_detail,
    value['purchase_order']::VARCHAR     AS purchase_order,
    value['request_link']::VARCHAR       AS request_link,
    value['request_number']::NUMBER      AS request_number,
    value['request_type']::VARCHAR       AS request_type,
    value['requester']::VARIANT          AS requester,
    value['status']::NUMBER              AS status,
    value['subcategory']::VARIANT        AS subcategory,
    value['subsidiary']::VARIANT         AS subsidiary,
    value['vendor']::VARIANT             AS vendor,
    value['workflow']::VARIANT           AS workflow,
    uploaded_at
  FROM intermediate
)

SELECT * FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1
