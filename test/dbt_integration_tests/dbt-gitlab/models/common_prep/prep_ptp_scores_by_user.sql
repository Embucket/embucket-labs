{{ simple_cte([
    ('prep_ptpf_scores_by_user', 'prep_ptpf_scores_by_user'),
    ('prep_ptpt_scores_by_user', 'prep_ptpt_scores_by_user'),
    ('prep_ptpl_scores_by_user', 'prep_ptpl_scores_by_user'),
    ('prep_ptp_scores_by_user_historical', 'prep_ptp_scores_by_user_historical')
    ])
}}

 , base_prep AS (

    SELECT 
      dim_marketing_contact_id,
      ptp_source,
      last_score_date AS score_date,
      score_group,
      valid_to
    FROM prep_ptp_scores_by_user_historical
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_marketing_contact_id ORDER BY valid_to DESC) = 1

), base AS (

    SELECT *
    FROM base_prep
    WHERE (ptp_source = 'Trial' OR score_group >=3) -- All Trial accounts and only Free and Leads >= 3
       AND DATEDIFF('day', valid_to, CURRENT_DATE) <= 60 -- record needs to have been scored in last 60 days
    
), prior_score_dates AS (
    
    SELECT
      dim_marketing_contact_id,
      ptp_source,
      last_score_date AS past_score_date,
      score_group
    FROM prep_ptp_scores_by_user_historical
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_marketing_contact_id ORDER BY valid_to DESC) = 2

), prior_scores AS (

    SELECT
      prior_score_dates.dim_marketing_contact_id,
      prior_score_dates.score_group AS past_score_group,
      prior_score_dates.past_score_date,
      CASE 
        WHEN prior_score_dates.ptp_source = 'Trial'
            THEN prep_ptpt_scores_by_user.insights
        WHEN prior_score_dates.ptp_source = 'Free'
            THEN prep_ptpf_scores_by_user.insights
        WHEN prior_score_dates.ptp_source = 'Lead'
            THEN prep_ptpl_scores_by_user.insights
      END AS past_insights
    FROM prior_score_dates
    LEFT JOIN prep_ptpt_scores_by_user
      ON prior_score_dates.dim_marketing_contact_id = prep_ptpt_scores_by_user.dim_marketing_contact_id
      AND prior_score_dates.past_score_date = prep_ptpt_scores_by_user.score_date
      AND prior_score_dates.ptp_source = 'Trial'
    LEFT JOIN prep_ptpf_scores_by_user
      ON prior_score_dates.dim_marketing_contact_id = prep_ptpf_scores_by_user.dim_marketing_contact_id
      AND prior_score_dates.past_score_date = prep_ptpf_scores_by_user.score_date
      AND prior_score_dates.ptp_source = 'Free'
    LEFT JOIN prep_ptpl_scores_by_user
      ON prior_score_dates.dim_marketing_contact_id = prep_ptpl_scores_by_user.dim_marketing_contact_id
      AND prior_score_dates.past_score_date = prep_ptpl_scores_by_user.score_date
      AND prior_score_dates.ptp_source = 'Lead'
    
)


SELECT
  base.ptp_source,
  base.dim_marketing_contact_id,
  base.score_group,
  CASE 
    WHEN base.ptp_source = 'Trial'
        THEN prep_ptpt_scores_by_user.score
    WHEN base.ptp_source = 'Free'
        THEN prep_ptpf_scores_by_user.score
    WHEN base.ptp_source = 'Lead'
        THEN prep_ptpl_scores_by_user.score
  END AS score,
  base.score_date,
  CASE 
    WHEN base.ptp_source = 'Trial'
        THEN prep_ptpt_scores_by_user.insights
    WHEN base.ptp_source = 'Free'
        THEN prep_ptpf_scores_by_user.insights
    WHEN base.ptp_source = 'Lead'
        THEN prep_ptpl_scores_by_user.insights
  END AS insights,
  prior_scores.past_insights,
  prior_scores.past_score_group,
  prior_scores.past_score_date,
  CASE
    WHEN base.ptp_source = 'Trial'
      THEN 'Namespace'
    WHEN base.ptp_source = 'Free'
      THEN 'Namespace'
    WHEN base.ptp_source = 'Lead'
      THEN 'Lead'
  END AS model_grain,
  CASE
    WHEN base.ptp_source = 'Trial'
      THEN prep_ptpt_scores_by_user.namespace_id
    WHEN base.ptp_source = 'Free'
      THEN prep_ptpf_scores_by_user.namespace_id
    WHEN base.ptp_source = 'Lead'
      THEN prep_ptpl_scores_by_user.lead_id
  END AS model_grain_id,
  CASE
    WHEN base.ptp_source = 'Free'
      THEN prep_ptpf_scores_by_user.days_since_trial_start
    ELSE NULL
  END AS days_since_trial_start
    
FROM base
LEFT JOIN prep_ptpt_scores_by_user
  ON base.dim_marketing_contact_id = prep_ptpt_scores_by_user.dim_marketing_contact_id
  AND base.score_date = prep_ptpt_scores_by_user.score_date
  AND base.ptp_source = 'Trial'
LEFT JOIN prep_ptpf_scores_by_user
  ON base.dim_marketing_contact_id = prep_ptpf_scores_by_user.dim_marketing_contact_id
  AND base.score_date = prep_ptpf_scores_by_user.score_date
  AND base.ptp_source = 'Free'
LEFT JOIN prep_ptpl_scores_by_user
  ON base.dim_marketing_contact_id = prep_ptpl_scores_by_user.dim_marketing_contact_id
  AND base.score_date = prep_ptpl_scores_by_user.score_date
  AND base.ptp_source = 'Lead'
LEFT JOIN prior_scores
  ON base.dim_marketing_contact_id = prior_scores.dim_marketing_contact_id