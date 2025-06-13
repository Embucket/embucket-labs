{{ simple_cte([
    ('ptpt_scores', 'ptpt_scores_source'),
    ('prep_namespace', 'prep_namespace'),
    ('gitlab_dotcom_users_source', 'gitlab_dotcom_users_source')
    ])
}}

, namespace_creator_ptpt_score AS (

    SELECT
      COALESCE(users.notification_email, users.email) AS email_address,
      ptpt_scores.namespace_id,
      ptpt_scores.score,
      ptpt_scores.score_group,
      ptpt_scores.insights,
      ptpt_scores.score_date::DATE                    AS score_date
    FROM prep_namespace
    INNER JOIN gitlab_dotcom_users_source users
      ON prep_namespace.creator_id = users.user_id
    INNER JOIN ptpt_scores
      ON prep_namespace.dim_namespace_id = ptpt_scores.namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address, score_date ORDER BY score DESC) = 1

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['namespace_creator_ptpt_score.email_address']) }} AS dim_marketing_contact_id,
  namespace_creator_ptpt_score.namespace_id,
  namespace_creator_ptpt_score.score,
  namespace_creator_ptpt_score.score_group,
  namespace_creator_ptpt_score.insights,
  namespace_creator_ptpt_score.score_date
FROM namespace_creator_ptpt_score
