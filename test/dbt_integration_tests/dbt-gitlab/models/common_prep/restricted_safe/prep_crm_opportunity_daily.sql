{{
  config(
    tags=['six_hourly'],
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id",
    on_schema_change="sync_all_columns"
  )
}}

WITH source AS (

  SELECT *
  FROM {{ ref('prep_crm_opportunity_live') }}
  {% if is_incremental() %}
    WHERE 
      (
        -- First run of the day: Include all opportunities 
        CURRENT_DATE() > (SELECT MAX(snapshot_date) FROM {{ this }})
      )
      OR 
      (
        -- Subsequent runs in the same day: Only include updated opportunities
        CURRENT_DATE() = (SELECT MAX(snapshot_date) FROM {{ this }})
        AND dbt_valid_from > (SELECT MAX(dbt_valid_from) FROM {{ this }})
      )
  {% endif %}


),

daily_spine AS (

  SELECT *
  FROM {{ ref('dim_date') }}
  /*
    Restricting snapshot model to only have data from this date forward.
    More information https://gitlab.com/gitlab-data/analytics/-/issues/14418#note_1134521216
  */
  WHERE date_actual::DATE >= '2020-02-01'
    AND date_actual <= CURRENT_DATE
    {% if is_incremental() %}
      AND date_actual >= (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['source.dim_crm_opportunity_id','daily_spine.date_id']) }} AS crm_opportunity_snapshot_id,
    daily_spine.date_id                                                                             AS snapshot_id,
    daily_spine.date_actual                                                                         AS snapshot_date,
    daily_spine.first_day_of_month                                                                  AS snapshot_month,
    daily_spine.fiscal_year                                                                         AS snapshot_fiscal_year,
    daily_spine.fiscal_quarter_name_fy                                                              AS snapshot_fiscal_quarter_name,
    daily_spine.first_day_of_fiscal_quarter                                                         AS snapshot_fiscal_quarter_date,
    daily_spine.day_of_fiscal_quarter_normalised                                                    AS snapshot_day_of_fiscal_quarter_normalised,
    daily_spine.day_of_fiscal_year_normalised                                                       AS snapshot_day_of_fiscal_year_normalised,
    daily_spine.last_day_of_fiscal_quarter                                                          AS snapshot_last_day_of_fiscal_quarter,
    source.*
  FROM source
  INNER JOIN daily_spine
    ON source.dbt_valid_from::DATE <= daily_spine.date_actual
      AND (source.dbt_valid_to::DATE > daily_spine.date_actual OR source.dbt_valid_to IS NULL)

)

SELECT *
FROM final
