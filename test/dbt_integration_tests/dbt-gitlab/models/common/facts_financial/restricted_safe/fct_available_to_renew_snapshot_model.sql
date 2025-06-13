{{ config({
        "materialized": "incremental",
        "unique_key": "fct_available_to_renew_snapshot_id",
        "tags": ["edm_snapshot", "atr_snapshots"]
    })
}}

WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2020-03-01'
      AND date_actual <= CURRENT_DATE 
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
      AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), fct_available_to_renew AS (

    SELECT *
    FROM {{ ref('fct_available_to_renew_snapshot') }}

), fct_available_to_renew_spined AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      fct_available_to_renew.*
    FROM fct_available_to_renew
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= fct_available_to_renew.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('fct_available_to_renew.dbt_valid_to') }}

), final AS (

     SELECT
       {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'primary_key']) }} AS fct_available_to_renew_snapshot_id,
       *
     FROM fct_available_to_renew_spined

)

SELECT *
FROM final