{{ config(
    materialized='incremental',
    unique_key='answer_id'
) }}

{{ level_up_intermediate('assessment_attempts') }}

parsed AS (
  SELECT
    intermediate.value['id']::VARCHAR  AS assessment_attempt_id,
    answers.value['answer']::VARCHAR   AS answer,
    answers.value['correct']::VARCHAR  AS is_correct,
    answers.value['id']::VARCHAR       AS answer_id,
    answers.value['question']::VARCHAR AS question,

    uploaded_at
  FROM intermediate
  INNER JOIN LATERAL FLATTEN(input => intermediate.value['answers']) AS answers

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        answer_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
