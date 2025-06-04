{{ level_up_intermediate('course_views') }}

parsed AS (
  SELECT
    value['companyId']::VARCHAR        AS company_id,
    value['courseId']::VARCHAR         AS course_id,
    value['courseSku']::VARCHAR        AS course_sku,
    value['courseTitle']::VARCHAR      AS course_title,
    value['timestamp']::TIMESTAMP      AS event_timestamp,
    value['lessonId']::VARCHAR         AS lesson_id,
    value['lessonSlug']::VARCHAR       AS lesson_slug,
    value['lessonTitle']::VARCHAR      AS lesson_title,
    value['sectionId']::VARCHAR        AS section_id,
    value['sectionSlug']::VARCHAR      AS section_slug,
    value['sectionTitle']::VARCHAR     AS section_title,
    value['topicId']::VARCHAR          AS topic_id,
    value['topicTitle']::VARCHAR       AS topic_title,

    value['userDetail']['id']::VARCHAR AS user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        event_timestamp,
        topic_title
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
