{{ config(
    materialized="incremental",
    unique_key="fiscal_quarter"
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
    LATERAL FLATTEN(input => jsontext['data']['timePeriods']) AS d

  {% if is_incremental() %}
    WHERE source.uploaded_at > (SELECT MAX(t.uploaded_at) FROM {{ this }} AS t)
  {% endif %}
),

parsed AS (
  SELECT
    REPLACE(value['timePeriodId'], '_', '-')::VARCHAR AS fiscal_quarter,
    value['startDate']::DATE                          AS fiscal_quarter_start_date,
    value['endDate']::DATE                            AS fiscal_quarter_end_date,
    value['label']::VARCHAR                           AS quarter,
    value['year']::NUMBER                             AS year,
    value['crmId']::VARCHAR                           AS crm_id,
    value['type']::VARCHAR                            AS time_period_type,
    uploaded_at
  FROM
    intermediate

  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        fiscal_quarter
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY
    fiscal_quarter
)

SELECT *
FROM
  parsed
