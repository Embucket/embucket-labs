WITH source AS (
    SELECT * 
    FROM {{ ref('sheetload_team_health_data_source') }}
),
renamed AS (
    SELECT
        timestamp::TIMESTAMP                     AS survey_time_at,
        email_address::VARCHAR                   AS email_address,
        question::VARCHAR                        AS question,
        CASE 
            WHEN question = 'Do you understand what results your team needs to deliver?' THEN 'Question 1'
            WHEN question = 'Does each member on your team understand what results they need to deliver?' THEN 'Question 2'
            WHEN question = 'Do the members of your team have the right skills/abilities to deliver results?' THEN 'Question 3'
            WHEN question = 'Does your team have the right resources to deliver results?' THEN 'Question 4'
            WHEN question = 'Is your team able to pivot and maintain results during time off (PTO, Extended Leave, Position Borrows, unplanned time away, etc.)?' THEN 'Question 5'
            WHEN question = 'Does your team have a shared system or tool used to monitor and communicate status updates?' THEN 'Question 6'
            WHEN question = 'Does your team demonstrate open and collaborative communication?' THEN 'Question 7'
            WHEN question = 'Does your team demonstrate effective cross-functional collaboration?' THEN 'Question 8'
            ELSE NULL 
        END                                     AS question_number,
        rating_description::VARCHAR             AS rating_description,
        rating_value::FLOAT                     AS rating_value,
        to_timestamp(_updated_at::NUMBER)       AS uploaded_at
    FROM source
)
SELECT *
FROM renamed
