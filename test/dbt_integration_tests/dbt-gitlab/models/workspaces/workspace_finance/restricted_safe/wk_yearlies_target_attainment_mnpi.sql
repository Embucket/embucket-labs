{{ config(materialized='table') }}

WITH final as (

SELECT
  yearlies_target.yearly_name,
  yearlies_target.yearly_dri,
  yearlies_target.yearly_description,
  yearlies_target.is_mnpi,
  yearlies_target.quarter,
  yearlies_actual.source_table,
  yearlies_target.targets_raw,
  yearlies_actual.actuals_raw,
  DIV0NULL(yearlies_actual.actuals_raw,yearlies_target.targets_raw) AS target_attainment
FROM
  {{ ref('prep_yearlies_target') }} AS yearlies_target
  LEFT JOIN {{ ref('wk_prep_yearlies_actual') }} AS yearlies_actual
  ON yearlies_target.quarter = yearlies_actual.quarter
  AND yearlies_target.yearly_name = yearlies_actual.yearly_name
)

SELECT 
  *,
  IFF(is_mnpi = true AND targets_raw IS NOT NULL, 1, targets_raw) AS targets_abstracted,
  CASE WHEN is_mnpi = true AND targets_raw IS NULL THEN NULL
       WHEN is_mnpi = true AND targets_raw IS NOT NULL THEN target_attainment
       ELSE actuals_raw END AS actuals_abstracted
FROM final
