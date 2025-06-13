WITH qualtrics_distribution AS (

    SELECT *
    FROM {{ ref('qualtrics_distribution') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY distribution_id ORDER BY uploaded_at DESC) = 1

)

SELECT *
FROM qualtrics_distribution
