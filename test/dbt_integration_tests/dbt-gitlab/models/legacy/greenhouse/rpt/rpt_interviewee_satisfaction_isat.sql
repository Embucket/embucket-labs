{{ config({
    "materialized":"table",
    "schema": "legacy"
    })
}}

WITH greenhouse_candidate_surveys AS (

    SELECT *
    FROM {{ ref('greenhouse_candidate_surveys_source') }}

), interview_results AS (

   SELECT 
     candidate_survey_id, 
     organization_id, 
     department_id, 
     department_name, 
     DATE_TRUNC('month', candidate_survey_submitted_at)::DATE AS submitted_at,
     candidate_survey_question_1,
    CASE
      WHEN candidate_survey_question_1 = 'Strongly Disagree' THEN 1
      WHEN candidate_survey_question_1 = 'Disagree' THEN 2
      WHEN candidate_survey_question_1 = 'Neutral' THEN 3
      WHEN candidate_survey_question_1 = 'Agree' THEN 4
      WHEN candidate_survey_question_1 = 'Strongly Agree' THEN 5
     ELSE NULL END AS isat_score
   FROM greenhouse_candidate_surveys
   WHERE isat_score IS NOT NULL
   {{ dbt_utils.group_by(n=7) }}

 )

SELECT *
FROM interview_results
