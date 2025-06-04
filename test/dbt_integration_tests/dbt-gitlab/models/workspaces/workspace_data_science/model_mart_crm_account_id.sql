{{ config(
     materialized = "table"
) }}

{{ simple_cte([
    ('pte_scores', 'pte_scores_source'),
    ('ptc_scores', 'ptc_scores_source')
  ])
}}

, pte_latest_date AS (

    SELECT MAX(score_date) AS score_date
    FROM pte_scores

), ptc_latest_date AS (

    SELECT MAX(score_date) AS score_date
    FROM ptc_scores

), pte AS (

    SELECT
      crm_account_id,
      score_date AS pte_score_date,
      score AS pte_score,
      score_group AS pte_score_group,
      AVG(score) OVER (PARTITION BY score_date) AS avg_pte_score,
      STDDEV(score) OVER (PARTITION BY score_date) AS stdev_pte_score,
      (pte_score - avg_pte_score) / stdev_pte_score AS pte_distance,
      insights,
      uptier_likely
    FROM pte_scores

), ptc AS (

     SELECT
      crm_account_id,
      score_date AS ptc_score_date,
      score AS ptc_score,
      score_group AS ptc_score_group,
      AVG(score) OVER (PARTITION BY score_date ORDER BY score_date ) AS avg_ptc_score,
      STDDEV(score) OVER (PARTITION BY score_date ORDER BY score_date ) AS stdev_ptc_score,
      (ptc_score - avg_ptc_score) / stdev_ptc_score AS ptc_distance,
      insights,
      downtier_likely,
      renewal_date
    FROM ptc_scores

)

SELECT
  COALESCE(a.crm_account_id, b.crm_account_id) AS crm_account_id,
  pte_score_date::DATE AS pte_score_date,
  pte_score,
  CASE
    WHEN pte_score_group = 5 AND ptc_score_group = 1 AND pte_distance < ptc_distance
      THEN 4
    ELSE pte_score_group
  END AS pte_stars,
  a.insights AS pte_insights,
  a.uptier_likely AS pte_uptier_likely,
  ptc_score_date::DATE AS ptc_score_date,
  ptc_score,
--, pte_distance, ptc_distance, avg_pte_score, avg_ptc_score
  CASE
    WHEN pte_score_group = 5 AND ptc_score_group = 1 AND pte_distance >= ptc_distance
      THEN 2
    ELSE ptc_score_group
  END AS ptc_stars,
  b.insights AS ptc_insights,
  b.downtier_likely AS ptc_downtier_likely,
  b.renewal_date::DATE AS renewal_date,
  IFF(pte_score_date = c.score_date, TRUE, FALSE) AS latest_pte_score,
  IFF(ptc_score_date = d.score_date, TRUE, FALSE) AS latest_ptc_score
 FROM pte a
 FULL OUTER JOIN ptc b
   ON  a.crm_account_id = b.crm_account_id
   AND a.pte_score_date = b.ptc_score_date
 LEFT JOIN pte_latest_date c
   ON a.pte_score_date = c.score_date
 LEFT JOIN ptc_latest_date d
   ON b.ptc_score_date = d.score_date
