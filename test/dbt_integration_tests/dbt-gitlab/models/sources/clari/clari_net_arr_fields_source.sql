{{ config(
    materialized="incremental",
    unique_key="field_id"
    )
}}

WITH
source AS (
  SELECT * FROM
    {{ source('clari', 'net_arr') }}
),

intermediate AS (
  SELECT
    d.value,
    source.uploaded_at
  FROM
    source,
    LATERAL FLATTEN(input => jsontext['data']['fields']) AS d

  {% if is_incremental() %}
    WHERE source.uploaded_at > (SELECT MAX(t.uploaded_at) FROM {{ this }} AS t)
  {% endif %}
),

parsed AS (
  SELECT
    value['fieldId']::VARCHAR   AS field_id,
    value['fieldName']::VARCHAR AS field_name,
    value['fieldType']::VARCHAR AS field_type,
    uploaded_at
  FROM
    intermediate

  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        field_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY field_id
)

SELECT *
FROM
  parsed
