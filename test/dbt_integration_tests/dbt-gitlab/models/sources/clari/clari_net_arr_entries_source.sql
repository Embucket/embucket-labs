{{ config(
    materialized="incremental",
    unique_key="entries_id",
    tags=["mnpi"]
    )
}}

WITH
source AS (
  SELECT * FROM
    {{ source('clari', 'net_arr') }}
),

intermediate AS (
  SELECT
    -- in older payloads, `api_forecast_id` was missing, default to `net_arr`
    COALESCE(jsontext['api_forecast_id'], 'net_arr') AS forecast_id,
    d.value,
    source.uploaded_at
  FROM
    source,
    LATERAL FLATTEN(input => jsontext['data']['entries']) AS d
  {% if is_incremental() %}
    WHERE source.uploaded_at > (SELECT MAX(t.uploaded_at) FROM {{ this }} AS t)
  {% endif %}
),

parsed AS (
  SELECT

    forecast_id,
    -- foreign keys
    REPLACE(value['timePeriodId'], '_', '-')::VARCHAR          AS fiscal_quarter,
    -- add fiscal_quarter to unique key: can be dup timeFrameId across quarters
    CONCAT(value['timeFrameId']::VARCHAR, '_', fiscal_quarter) AS time_frame_id,
    value['userId']::VARCHAR                                   AS user_id,
    value['fieldId']::VARCHAR                                  AS field_id,

    -- primary key, must be after aliased cols are derived else sql error
    CONCAT_WS(' | ', time_frame_id, user_id, field_id)         AS entries_id,

    -- logical info
    value['forecastValue']::NUMBER(38, 1)                      AS forecast_value,
    value['currency']::VARIANT                                 AS currency,
    value['isUpdated']::BOOLEAN                                AS is_updated,
    value['updatedBy']::VARCHAR                                AS updated_by,
    TO_TIMESTAMP(value['updatedOn']::INT / 1000)               AS updated_on,

    -- metadata
    uploaded_at
  FROM
    intermediate

    -- filter out overlapping records that exist in both forecast_ids
  WHERE NOT (forecast_id = 'net_arr_ps_summary' AND field_id = 'arr_most_likely')

  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        fiscal_quarter,
        time_frame_id,
        user_id,
        field_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY
    time_frame_id
)


SELECT *
FROM
  parsed
