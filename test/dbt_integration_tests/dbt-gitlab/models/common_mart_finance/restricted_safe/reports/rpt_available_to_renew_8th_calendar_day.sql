WITH snapshot_dates AS (
    --Use the 8th calendar day to snapshot ATR
    SELECT DISTINCT
      first_day_of_month,
      snapshot_date_fpa
    FROM {{ ref('dim_date') }}
    WHERE first_day_of_month < '2024-03-01'
    ORDER BY 1 DESC

), mart_available_to_renew_snapshot AS (

    SELECT *
    FROM {{ ref('mart_available_to_renew_snapshot_model') }}

), final AS (

    SELECT *
    FROM mart_available_to_renew_snapshot
    INNER JOIN snapshot_dates
      ON mart_available_to_renew_snapshot.snapshot_date = snapshot_dates.snapshot_date_fpa

)

SELECT *
FROM final
