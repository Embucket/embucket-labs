WITH source AS (

  SELECT
    group_name,
    adopted,
    not_adopted,
    adopted + not_adopted             AS total_findings,
    DIV0NULL(adopted, total_findings) AS adoption_percentage,
    lower_bound,
    upper_bound,

    CASE
      WHEN adoption_percentage < lower_bound THEN 'at_risk'
      WHEN adoption_percentage >= upper_bound THEN 'on_track'
      ELSE 'needs_attention'
    END                               AS adoption_status,

    minimum_findings,
    total_findings < minimum_findings AS is_below_findings_threshold,
    aggregated_at

  FROM {{ ref('adoption_by_group_source') }}

)

SELECT *
FROM source
