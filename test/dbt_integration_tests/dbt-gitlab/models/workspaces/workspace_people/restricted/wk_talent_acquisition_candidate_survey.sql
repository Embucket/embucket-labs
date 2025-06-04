WITH greenhouse_departments_source AS (
  SELECT *
  FROM {{ ref('greenhouse_departments_source') }}
),

greenhouse_candidate_surveys_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidate_surveys_source') }}
),

department AS (
  WITH source AS (
    SELECT *
    FROM greenhouse_departments_source
  ),

  greenhouse_departments (department_name, department_id, hierarchy_id, hierarchy_name) AS (
    SELECT
      department_name,
      department_id,
      TO_ARRAY(department_id)   AS hierarchy_id,
      TO_ARRAY(department_name) AS hierarchy_name
    FROM source
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
      iteration.department_name,
      iteration.department_id,
      ARRAY_APPEND(anchor.hierarchy_id, iteration.department_id)     AS hierarchy_id,
      ARRAY_APPEND(anchor.hierarchy_name, iteration.department_name) AS hierarchy_name
    FROM source AS iteration
    INNER JOIN greenhouse_departments AS anchor ON iteration.parent_id = anchor.department_id
  ),

  departments AS (
    SELECT
      department_name,
      department_id,
      ARRAY_SIZE(hierarchy_id)   AS hierarchy_level,
      hierarchy_id,
      hierarchy_name,
      hierarchy_name[0]::VARCHAR AS level_1,
      hierarchy_name[1]::VARCHAR AS level_2,
      hierarchy_name[2]::VARCHAR AS level_3
    FROM greenhouse_departments
  )

  SELECT
    department_id,
    department_name AS sub_department_name,
    level_1         AS cost_center_name,
    level_2         AS department_name
  FROM departments
)

SELECT
  isat.candidate_survey_id,
  isat.candidate_survey_submitted_at::DATE AS response_date,
  isat.department_id,
  dept.sub_department_name,
  dept.department_name,
  dept.cost_center_name,
  CASE isat.candidate_survey_question_1
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_1,
  CASE isat.candidate_survey_question_2
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_2,
  CASE isat.candidate_survey_question_3
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_3,
  CASE isat.candidate_survey_question_4
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_4,
  CASE isat.candidate_survey_question_5
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_5,
  CASE isat.candidate_survey_question_6
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_6,
  CASE isat.candidate_survey_question_7
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_7,
  CASE isat.candidate_survey_question_8
    WHEN 'Strongly Disagree'
      THEN 1
    WHEN 'Disagree'
      THEN 2
    WHEN 'Neutral'
      THEN 3
    WHEN 'Agree'
      THEN 4
    WHEN 'Strongly Agree'
      THEN 5
  END                                      AS candidate_survey_question_8
FROM greenhouse_candidate_surveys_source AS isat
LEFT JOIN department AS dept ON isat.department_id = dept.department_id
