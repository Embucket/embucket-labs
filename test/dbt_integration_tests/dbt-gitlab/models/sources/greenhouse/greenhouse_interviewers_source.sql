WITH source as (

    SELECT *
    FROM {{ source('greenhouse', 'interviewers') }}
    -- intentionally excluded record, details in https://gitlab.com/gitlab-data/analytics/-/issues/10211
    -- hide PII columns, just exclude one record intentionally
    WHERE NOT (user IS NULL
    AND scorecard_id IS NULL
    AND interview_id = 101187575002)

), renamed as (

  SELECT distinct user AS interviewer_name
  FROM source

)

SELECT *
FROM renamed
