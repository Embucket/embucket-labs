WITH contributors AS (

    SELECT *
    FROM {{ source('handbook', 'values_before_2020_06')}}

    UNION ALL

    SELECT *
    FROM {{ source('handbook', 'values_after_2020_06')}}

), rename AS (

    SELECT
      name::VARCHAR     AS author_name,
      sha::VARCHAR      AS git_sha,
      email::VARCHAR    AS author_email,
      date::TIMESTAMP   AS git_commit_at,
      message::VARCHAR  AS git_message
    FROM contributors

)

SELECT *
FROM rename
