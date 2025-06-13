{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('rpt_namespace_onboarding','rpt_namespace_onboarding'),
    ('ptpf_scores', 'ptpf_scores')
    ])
}},

namespace_attributes_and_ptp_score_change_base AS (

SELECT 
  rpt.ultimate_parent_namespace_id,
  rpt.visibility_level,
  rpt.namespace_type,
  rpt.namespace_contains_valuable_signup,
  rpt.has_team_activation,
  rpt.creator_is_valuable_signup,
  rpt.namespace_creator_role,
  rpt.namespace_creator_jtbd,
  ptp.score_date::DATE AS ptp_score_date,
  ptp.score_group      AS ptp_score_group,
  LAG(score_group) OVER(PARTITION BY namespace_id ORDER BY score_date ASC) 
                       AS previous_ptp_score,
  LAG(score_date) OVER(PARTITION BY namespace_id ORDER BY score_date ASC)::DATE  
                       AS previous_ptp_score_date,
  previous_ptp_score - ptp_score_group                                             
                       AS ptp_score_group_change,
  CASE WHEN first_paid_subscription_start_date BETWEEN previous_ptp_score_date AND ptp_score_date THEN 'first_paid_subscription_start'
       WHEN ptp_score_group_change < 0 THEN 'ptp_score_decrease'
       WHEN ptp_score_group_change > 0 THEN 'ptp_score_increase'
       WHEN ptp_score_group_change = 0 THEN 'no_change'
       ELSE 'no_prior_score' END AS ptp_change_categories,
  CASE WHEN first_paid_subscription_start_date BETWEEN previous_ptp_score_date AND ptp_score_date THEN TRUE
       WHEN ptp_score_group_change < 0 THEN FALSE
       WHEN ptp_score_group_change > 0 THEN TRUE
       WHEN ptp_score_group_change = 0 THEN FALSE
       ELSE null END AS ptp_improvement --true/false/null output - null if no prior score present
FROM ptpf_scores ptp
JOIN rpt_namespace_onboarding rpt -- This model is filtered to top level namespaces that don't have a namespace creator who is blocked
  ON rpt.ultimate_parent_namespace_id = ptp.namespace_id
WHERE IFF(first_paid_subscription_start_date IS NOT NULL, first_paid_subscription_start_date >= score_date, TRUE) = TRUE 
--excluding namespaces that previously had a paid subscription prior to score date that churned/expired and are again getting scored as a free namespace

), base AS (

-- Aggregating final output - in order to work with this data in Tableau efficiently, output must be less than 10m records

SELECT 
  DATE_TRUNC('month', ptp_score_date)
                       AS ptp_score_date_month,
  visibility_level,
  namespace_contains_valuable_signup,
  has_team_activation,
  creator_is_valuable_signup,
  namespace_creator_role,
  namespace_creator_jtbd,
  ptp_score_group,
  previous_ptp_score,
  ptp_score_group_change,
  ptp_change_categories,
  ptp_improvement,
  COUNT(DISTINCT ultimate_parent_namespace_id) 
                       AS count_top_level_group_namespaces
FROM namespace_attributes_and_ptp_score_change_base
GROUP BY ALL

)


{{ dbt_audit(
    cte_ref="base",
    created_by="@eneuberger",
    updated_by="@eneuberger",
    created_date="2024-03-04",
    updated_date="2024-03-04"
) }}