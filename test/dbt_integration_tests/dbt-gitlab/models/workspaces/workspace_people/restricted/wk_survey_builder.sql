WITH surveys AS (
  SELECT *
  FROM
    {{ ref('greenhouse_survey_builder_surveys_source') }}
),

answers AS (
  SELECT *
  FROM
    {{ ref('greenhouse_survey_builder_answers_source') }}
),

questions AS (
  SELECT *
  FROM
    {{ ref('greenhouse_survey_builder_questions_source') }}
),

organizations AS (
  SELECT *
  FROM
    {{ ref('greenhouse_delivered_candidate_surveys_source') }}
),

departments_source AS (

  SELECT *
  FROM {{ ref('greenhouse_departments_source') }}

),

departments_stage AS (

  SELECT
    department_name,
    department_id,
    TO_ARRAY(department_id)   AS hierarchy_id,
    TO_ARRAY(department_name) AS hierarchy_name
  FROM departments_source
  WHERE parent_id IS NULL

  UNION ALL

  SELECT
    iteration.department_name,
    iteration.department_id,
    ARRAY_APPEND(anchor.hierarchy_id, iteration.department_id)     AS hierarchy_id,
    ARRAY_APPEND(anchor.hierarchy_name, iteration.department_name) AS hierarchy_name
  FROM departments_source AS iteration
  INNER JOIN departments_stage AS anchor
    ON iteration.parent_id = anchor.department_id


),

departments AS (

  SELECT
    department_id,
    department_name,
    ARRAY_SIZE(hierarchy_id)   AS hierarchy_level,
    hierarchy_name[0]::VARCHAR AS cost_center,
    hierarchy_name[1]::VARCHAR AS department,
    hierarchy_name[2]::VARCHAR AS sub_department
  FROM departments_stage


),

final AS (

  SELECT
    answers.survey_id,
    surveys.survey_key,
    surveys.is_survey_active,
    answers.question_id,
    questions.candidate_survey_question AS question,
    questions.is_question_active,
    questions.question_priority         AS question_order,
    answers.delivered_candidate_survey_id,
    answers.answer_id,
    questions.answer_type,
    answers.answer,
    answers.comment,
    answers.submitted_at                AS answer_submitted_at,
    answers.created_at                  AS answer_created_at,
    answers.updated_at                  AS answer_updated_at,
    answers.organization_id,
    organizations.department_id,
    departments.hierarchy_level         AS department_hierarchy_level,
    departments.sub_department,
    departments.department,
    departments.cost_center,
    organizations.office_id,
    organizations.office_name
  FROM answers
  LEFT JOIN questions ON answers.question_id = questions.question_id
  LEFT JOIN surveys ON answers.survey_id = surveys.survey_id
  LEFT JOIN organizations ON answers.delivered_candidate_survey_id = organizations.id
  LEFT JOIN departments ON organizations.department_id = departments.department_id
)

SELECT *
FROM final
