{{ simple_cte([
    ('ptpl_scores', 'ptpl_scores_source'),
    ('sfdc_lead_source', 'sfdc_lead_source')
    ])
}}


, ptpl_score_email AS (

    SELECT
      leads.lead_email                                     AS email_address,
      ptpl_scores.lead_id,
      ptpl_scores.score,
      ptpl_scores.score_group,
      ptpl_scores.insights,
      ptpl_scores.score_date::DATE                    AS score_date
    FROM sfdc_lead_source leads
    INNER JOIN ptpl_scores
      ON ptpl_scores.lead_id = leads.lead_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address, score_date ORDER BY ptpl_scores.score DESC) = 1

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['ptpl_score_email.email_address']) }} AS dim_marketing_contact_id,
  ptpl_score_email.lead_id,
  ptpl_score_email.score,
  ptpl_score_email.score_group,
  ptpl_score_email.insights,
  ptpl_score_email.score_date
FROM ptpl_score_email
