{{ config(
    materialized='incremental',
    unique_key='xmau_metric_monthly_snapshot_id',
    tags=["mnpi_exception","edm_snapshot", "product"]
) }}


WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2022-02-23'
    AND date_actual <= CURRENT_DATE {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), rpt_event_xmau_metric_monthly AS (

    SELECT *
    FROM {{ source('snapshots','rpt_event_xmau_metric_monthly_snapshot') }}

), rpt_event_xmau_metric_monthly_spined AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      rpt_event_xmau_metric_monthly.*
    FROM rpt_event_xmau_metric_monthly
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= rpt_event_xmau_metric_monthly.dbt_valid_from
    AND snapshot_dates.date_actual < {{ coalesce_to_infinity('rpt_event_xmau_metric_monthly.dbt_valid_to') }}

), final AS (

     SELECT
       {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'xmau_metric_monthly_id']) }} AS xmau_metric_monthly_snapshot_id,
       *
     FROM rpt_event_xmau_metric_monthly_spined

)

SELECT *
FROM final

