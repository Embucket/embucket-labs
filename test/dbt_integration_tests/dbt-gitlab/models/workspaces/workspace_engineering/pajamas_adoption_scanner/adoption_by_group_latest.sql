WITH latest AS (

  SELECT *
  FROM {{ ref('adoption_by_group') }}

  QUALIFY MAX(aggregated_at) OVER () = aggregated_at

),

overall_gb AS (
  SELECT
    SUM(adopted)          AS adopted,
    SUM(not_adopted)      AS not_adopted,
    MAX(lower_bound)      AS lower_bound,
    MAX(upper_bound)      AS upper_bound,
    MAX(minimum_findings) AS minimum_findings,
    MAX(aggregated_at)    AS aggregated_at

  FROM latest
),

overall AS (

  SELECT
    'overall'                         AS group_name,
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
  FROM overall_gb
),

unioned AS (
  SELECT * FROM latest
  UNION ALL
  SELECT * FROM overall
)



SELECT *
FROM unioned
