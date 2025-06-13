WITH snapshot_dates AS (
    --Use the 45th calendar day to snapshot ATR
    SELECT DISTINCT
      first_day_of_month,
      snapshot_date_billings
    FROM {{ ref('dim_date') }}
    ORDER BY 1 DESC

), mart_available_to_renew_snapshot AS (

    SELECT *
    FROM {{ ref('mart_available_to_renew_snapshot_model') }}

), final AS (

    SELECT *
    FROM mart_available_to_renew_snapshot
    INNER JOIN snapshot_dates
      ON mart_available_to_renew_snapshot.snapshot_date = snapshot_dates.snapshot_date_billings

)

SELECT *
FROM final
