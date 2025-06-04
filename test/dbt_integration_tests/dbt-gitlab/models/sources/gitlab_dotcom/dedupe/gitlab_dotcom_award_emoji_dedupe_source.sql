WITH award_emoji AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'award_emoji') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

)

SELECT *
FROM award_emoji
