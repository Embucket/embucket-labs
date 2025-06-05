WITH contributors AS (

    SELECT *
    FROM EMBUCKET.handbook.values_page_git_log_before_2020_06

    UNION ALL

    SELECT *
    FROM EMBUCKET.handbook.values_page_git_log

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