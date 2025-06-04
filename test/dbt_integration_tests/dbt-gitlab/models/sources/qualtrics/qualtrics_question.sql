WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'questions') }}

), questions AS (

    SELECT 
      d.value AS data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d
  
), parsed AS (

    SELECT 
      data_by_row['survey_id']::VARCHAR               AS survey_id,
      data_by_row['QuestionID']::VARCHAR              AS question_id,
      data_by_row['QuestionDescription']::VARCHAR     AS question_description,
      data_by_row['Choices']::ARRAY                   AS answer_choices
    FROM questions
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY question_id
      ORDER BY uploaded_at DESC
    ) = 1

)
SELECT * 
FROM parsed
