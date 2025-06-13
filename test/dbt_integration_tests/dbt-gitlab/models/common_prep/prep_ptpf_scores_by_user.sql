{{ simple_cte([
    ('ptpf_scores', 'ptpf_scores_source'),
    ('prep_namespace', 'prep_namespace'),
    ('gitlab_dotcom_users_source', 'gitlab_dotcom_users_source')
    ])
}}


, namespace_creator_ptpf_score AS (

    SELECT
      COALESCE(users.notification_email, users.email) AS email_address,
      ptpf_scores.namespace_id,
      ptpf_scores.score,
      ptpf_scores.insights,
      ptpf_scores.days_since_trial_start,
      ptpf_scores.score_group,
      ptpf_scores.score_date::DATE                    AS score_date
    FROM prep_namespace
    INNER JOIN gitlab_dotcom_users_source users
      ON prep_namespace.creator_id = users.user_id
    INNER JOIN ptpf_scores
      ON prep_namespace.dim_namespace_id = ptpf_scores.namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address, score_date ORDER BY score DESC) = 1

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['namespace_creator_ptpf_score.email_address']) }} AS dim_marketing_contact_id,
  namespace_creator_ptpf_score.namespace_id,
  namespace_creator_ptpf_score.score,
  namespace_creator_ptpf_score.insights,
  namespace_creator_ptpf_score.score_group,
  namespace_creator_ptpf_score.score_date,
  namespace_creator_ptpf_score.days_since_trial_start
FROM namespace_creator_ptpf_score