{{ config({
        "materialized": "incremental",
        "on_schema_change": "sync_all_columns",
        "unique_key": "mart_arr_snapshot_id",
        "tags": ["edm_snapshot", "arr_snapshots"]
    })
}}

/* grain: one record per subscription, product per month */
WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2020-03-01'
    AND date_actual <= CURRENT_DATE {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), mart_arr AS (

    SELECT *
    FROM {{ ref('prep_mart_arr_snapshot_base') }}

), mart_arr_spined AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      mart_arr.*
    FROM mart_arr
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= mart_arr.dbt_valid_from
    AND snapshot_dates.date_actual < {{ coalesce_to_infinity('mart_arr.dbt_valid_to') }}

), final AS (

     SELECT
       {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'primary_key']) }} AS mart_arr_snapshot_id,
       *
     FROM mart_arr_spined

)


SELECT *
FROM final
