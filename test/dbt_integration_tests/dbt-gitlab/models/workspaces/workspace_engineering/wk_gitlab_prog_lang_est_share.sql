WITH data AS (

    SELECT * FROM {{ ref('wk_gitlab_prog_lang')}}

), final AS (
  
  SELECT DISTINCT
    programming_language_name,
    (COUNT(project_id) OVER (PARTITION BY programming_language_name) / (SELECT COUNT(DISTINCT project_id) FROM data))          AS project_share,
    (SUM(share / 100 * repository_size) OVER (PARTITION BY programming_language_name) /
      SUM(share / 100 * repository_size) OVER (PARTITION BY 1))  AS estimated_share,
      SUM(share / 100 * repository_size) OVER (PARTITION BY programming_language_name) AS estimated_share_sum
  FROM data
  
)

SELECT 
  programming_language_name,
  project_share,
  estimated_share
FROM final
ORDER BY estimated_share_sum DESC
