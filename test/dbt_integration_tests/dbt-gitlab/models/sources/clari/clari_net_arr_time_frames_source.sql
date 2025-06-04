-- non-incremental, so that 'week_number' field can be calculated correctly

WITH
source AS (
  SELECT * FROM
    {{ source('clari', 'net_arr') }}
),

intermediate AS (
  SELECT
    d.value,
    source.uploaded_at,
    source.jsontext['api_fiscal_quarter'] AS fiscal_quarter
  FROM
    source,
    LATERAL FLATTEN(input => jsontext['data']['timeFrames']) AS d
),

parsed AS (
  SELECT
    CONCAT(value['timeFrameId']::VARCHAR, '_', fiscal_quarter) AS time_frame_id,
    fiscal_quarter::VARCHAR                                    AS fiscal_quarter,
    value['startDate']::DATE                                   AS week_start_date,
    value['endDate']::DATE                                     AS week_end_date,
    -- dense_rank() to account for dups prior to qualify
    DENSE_RANK() OVER (
      PARTITION BY
        fiscal_quarter
      ORDER BY
        time_frame_id
    ) - 1                                                      AS week_number, -- start week from 0
    uploaded_at
  FROM
    intermediate

  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        time_frame_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY
    fiscal_quarter,
    time_frame_id
)

SELECT *
FROM
  parsed
