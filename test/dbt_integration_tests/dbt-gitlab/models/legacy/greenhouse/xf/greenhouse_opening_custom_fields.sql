WITH greenhouse_openings AS (

    SELECT *
    FROM {{ ref ('greenhouse_openings_source') }}

), custom_fields AS (
    
    SELECT 
      opening_id, 
      {{ dbt_utils.pivot(
          'opening_custom_field', 
          dbt_utils.get_column_values(ref('greenhouse_opening_custom_fields_source'), 'opening_custom_field'),
          agg = 'MAX',
          then_value = 'opening_custom_field_display_value',
          else_value = 'NULL',
          quote_identifiers = False
      ) }}
    FROM {{ref('greenhouse_opening_custom_fields_source')}}
    GROUP BY opening_id

), final AS (

    SELECT
      custom_fields.opening_id              AS job_opening_id,
      greenhouse_openings.job_id,
      greenhouse_openings.opening_id,
      null                    AS job_opening_type,
      null as hiring_manager,
      null as finance_id
    FROM custom_fields
    LEFT JOIN greenhouse_openings
      ON custom_fields.opening_id = greenhouse_openings.job_opening_id

)

SELECT *
FROM final
