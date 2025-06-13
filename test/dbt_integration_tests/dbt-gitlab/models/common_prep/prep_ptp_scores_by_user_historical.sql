{% set columns = ["dim_marketing_contact_id", "score_group", "ptp_source", "last_score_date", "valid_from", "valid_to"] %}

{{ simple_cte([
    ('prep_ptpf_scores_by_user', 'prep_ptpf_scores_by_user'),
    ('prep_ptpt_scores_by_user', 'prep_ptpt_scores_by_user'),
    ('prep_ptpl_scores_by_user', 'prep_ptpl_scores_by_user')
    ])
}}


--Pull in the last score that marketing_id received so we can determine if a score is still "active" or not.
, combined_score_dates AS (

    SELECT
      dim_marketing_contact_id,
      score_date

    FROM prep_ptpt_scores_by_user

    UNION ALL

    SELECT
      dim_marketing_contact_id,
      score_date
    FROM prep_ptpf_scores_by_user

    UNION ALL

    SELECT
      dim_marketing_contact_id,
      score_date
    FROM prep_ptpl_scores_by_user
  
), latest_score_date AS (
  
    SELECT
      dim_marketing_contact_id,
      MAX(score_date) AS latest_score_date
    FROM combined_score_dates
    GROUP BY dim_marketing_contact_id

), combined_historic_scores AS (

  SELECT
    COALESCE(prep_ptpt_scores_by_user.dim_marketing_contact_id,
      prep_ptpf_scores_by_user.dim_marketing_contact_id,
      prep_ptpl_scores_by_user.dim_marketing_contact_id)                  AS dim_marketing_contact_id_combined,
    COALESCE(prep_ptpt_scores_by_user.score_date,
      prep_ptpf_scores_by_user.score_date,
      prep_ptpl_scores_by_user.score_date)                                AS score_date_combined,
    /*
    -- Logic is as follows:
      - Trials: Because trials are updated daily, we can use the daily score when determining which model (ptpt, ptpf, ptpl) to use.
                Once the trial is over, their score will revert to NULL for their trial score
      - Free: Updated monthly. Therefore we will want to pull use the most recent Free score on or before the selected date when determining
              which model (ptpt, ptpf, ptpl) to use. This is why we are using the LAG function with IGNORE NULLS
      - Leads: Same logic as free
      - Once the historic ptpt, ptpf, and ptpl scores for each date are determined, then the logic is applied
          to determine which score actually gets populated in Salesforce
    */
    
    COALESCE(prep_ptpt_scores_by_user.score_group,
      LAG(prep_ptpt_scores_by_user.score_group, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined))
                                                  AS latest_ptpt,
    COALESCE(prep_ptpf_scores_by_user.score_group,
      LAG(prep_ptpf_scores_by_user.score_group, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined))
                                                  AS latest_ptpf,
    COALESCE(prep_ptpl_scores_by_user.score_group,
      LAG(prep_ptpl_scores_by_user.score_group, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined))
                                                  AS latest_ptpl,
    prep_ptpf_scores_by_user.score_group          AS ptpf_score_group,
    prep_ptpl_scores_by_user.score_group          AS ptpl_score_group,
    COALESCE(prep_ptpt_scores_by_user.score_date,
      LAG(prep_ptpt_scores_by_user.score_date, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined))
                                                  AS latest_ptpt_score_date,
    COALESCE(prep_ptpf_scores_by_user.score_date,
      LAG(prep_ptpf_scores_by_user.score_date, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined)) 
                                                  AS latest_ptpf_score_date,
    COALESCE(prep_ptpl_scores_by_user.score_date,
      LAG(prep_ptpl_scores_by_user.score_date, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined))
                                                  AS latest_ptpl_score_date,
    
    --Create Logic for which score source to use
    CASE
      WHEN latest_ptpt >= 4 AND DATEDIFF('DAY', latest_ptpt_score_date, score_date_combined) <= 5  -- Only use if score has been updated in last 5 days
        THEN 'Trial'
      WHEN latest_ptpf = 5 AND DATEDIFF('DAY', latest_ptpf_score_date, score_date_combined) <= 45 -- Only use if score has been updated in last 45 days
        THEN 'Free'
      WHEN latest_ptpl = 5 AND DATEDIFF('DAY', latest_ptpl_score_date, score_date_combined) <= 45 -- Only use if score has been updated in last 45 days
        THEN 'Lead'
      WHEN latest_ptpf >= 4 AND DATEDIFF('DAY', latest_ptpf_score_date, score_date_combined) <= 45 -- Only use if score has been updated in last 45 days
        THEN 'Free'
      WHEN latest_ptpl >= 4 AND DATEDIFF('DAY', latest_ptpl_score_date, score_date_combined) <= 45 -- Only use if score has been updated in last 45 days
        THEN 'Lead'
      WHEN prep_ptpt_scores_by_user.dim_marketing_contact_id IS NOT NULL
        THEN 'Trial'
      WHEN prep_ptpf_scores_by_user.dim_marketing_contact_id IS NOT NULL
        THEN 'Free'
      ELSE  'Lead'
    END AS ptp_source,

    --Create logic for pulling in the appropriate score
    CASE
      WHEN ptp_source = 'Trial'
        THEN latest_ptpt
      WHEN ptp_source = 'Free'
        THEN latest_ptpf
      WHEN ptp_source = 'Lead'
        THEN latest_ptpl
    END AS score_group,

    CASE
      WHEN ptp_source = 'Trial'
        THEN latest_ptpt_score_date
      WHEN ptp_source = 'Free'
        THEN latest_ptpf_score_date
      WHEN ptp_source = 'Lead'
        THEN latest_ptpl_score_date
    END AS last_score_date,

    CASE
      WHEN ptp_source = 'Trial'
        THEN 1 -- When trial score is the same as the free or lead score do not collaspe
      WHEN ptp_source = 'Free'
        THEN 2 -- When free or lead score are the same then combine into one
      WHEN ptp_source = 'Lead'
        THEN 2 -- When free or lead score are the same then combine into one
    END AS score_priority
    
  FROM prep_ptpt_scores_by_user
  FULL JOIN prep_ptpf_scores_by_user 
      ON prep_ptpt_scores_by_user.dim_marketing_contact_id = prep_ptpf_scores_by_user.dim_marketing_contact_id
      AND prep_ptpt_scores_by_user.score_date = prep_ptpf_scores_by_user.score_date
  FULL JOIN prep_ptpl_scores_by_user 
      ON prep_ptpt_scores_by_user.dim_marketing_contact_id = prep_ptpl_scores_by_user.dim_marketing_contact_id
      AND prep_ptpt_scores_by_user.score_date = prep_ptpl_scores_by_user.score_date
  ORDER BY dim_marketing_contact_id_combined, score_date_combined

), combined_historic_scores_lag AS (

    SELECT
      *,
        --Determine when a score changes based on previous score
      LAG(score_group, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined) AS prev_group,
       --Determine when a model type changes based on previous type (Free and Leads are treated as same type)
      LAG(score_priority, 1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined) AS prev_score_priority,
      --Determine the next time the model was scored so we know when a score is valid until
      LAG(score_date_combined, -1, NULL) IGNORE NULLS OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined) AS next_score_date,
       --Group scores for a marketing ID together if they dont change. Once the score changes then a new "rank" is created
      CONDITIONAL_TRUE_EVENT(score_group != prev_group OR score_priority != prev_score_priority) OVER (PARTITION BY dim_marketing_contact_id_combined ORDER BY score_date_combined) AS score_group_rank 
    FROM combined_historic_scores

), valid_to_from AS (

    SELECT
      dim_marketing_contact_id_combined    AS dim_marketing_contact_id,
      score_group,
      ptp_source,
      score_date_combined                  AS score_date,
      last_score_date,
      next_score_date,
      latest_score_date,
      prev_group,
      score_group_rank,
      score_priority,
      prev_score_priority,
      --Determine the first date the score occured within the "rank"
      FIRST_VALUE(score_date) OVER (PARTITION BY dim_marketing_contact_id, score_group_rank, score_priority ORDER BY score_date) AS valid_from,
      --Determine the last time the score occured within the "rank"
      LAST_VALUE(next_score_date) OVER (PARTITION BY dim_marketing_contact_id, score_group_rank, score_priority ORDER BY score_date) AS valid_to_prep,
      --If score_date is the same date as the most recent score date. If it is then use, otherwise the valid_to date is the day before the next valid_from date.
      CASE
        WHEN score_date = latest_score_date
          THEN latest_score_date
        ELSE DATEADD('day', -1, valid_to_prep)
      END AS valid_to
    FROM combined_historic_scores_lag 
    LEFT JOIN latest_score_date 
        ON combined_historic_scores_lag.dim_marketing_contact_id_combined = latest_score_date.dim_marketing_contact_id

)

SELECT
  dim_marketing_contact_id,
  score_group,
  ptp_source,
  last_score_date,
  valid_from,
  valid_to
FROM valid_to_from
WHERE VALID_TO IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_marketing_contact_id, valid_from, valid_to ORDER BY last_score_date DESC) = 1
ORDER BY dim_marketing_contact_id, valid_from
