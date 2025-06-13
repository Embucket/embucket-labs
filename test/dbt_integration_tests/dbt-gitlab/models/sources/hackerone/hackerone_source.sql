WITH source AS (
  SELECT *
  FROM {{ source('hackerone','reports') }}
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
    value['id']::INT                   AS id,
    value['state']::VARCHAR            AS state,
    value['created_at']::TIMESTAMP_NTZ AS created_at,
    value['bounties']::VARIANT         AS bounties_array,
    uploaded_at
  FROM intermediate
)

SELECT *
FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1
