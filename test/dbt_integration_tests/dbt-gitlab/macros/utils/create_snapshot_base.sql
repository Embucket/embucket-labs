{%- macro create_snapshot_base(source, primary_key, date_start, date_part, snapshot_id_name) -%}

WITH date_spine AS (

    SELECT DISTINCT
      DATE_TRUNC({{ date_part }}, date_day) AS date_actual
    FROM {{ref("date_details")}}
    WHERE date_day >= '{{ date_start }}'::DATE
      AND date_day <= CURRENT_DATE   

), base AS (

    SELECT *
    FROM {{ source }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ primary_key }}, DATE_TRUNC({{ date_part }}, dbt_valid_from)
                               ORDER BY dbt_valid_from DESC) = 1

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key([primary_key, 'date_actual']) }}       AS unique_key,
      dbt_scd_id                                                        AS {{ snapshot_id_name }},
      date_actual,
      dbt_valid_from                                                    AS valid_from,
      dbt_valid_to                                                      AS valid_to,
      IFF(dbt_valid_to IS NULL, TRUE, FALSE)                            AS is_currently_valid,
      base.*
    FROM base
    INNER JOIN date_spine
      ON base.dbt_valid_from::DATE <= date_spine.date_actual
      AND (base.dbt_valid_to::DATE > date_spine.date_actual OR base.dbt_valid_to IS NULL)     
      

)

{%- endmacro -%}
