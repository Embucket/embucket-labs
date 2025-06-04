{{ level_up_intermediate('meetings') }}

parsed AS (
  SELECT
    value['attendeeInfo']::VARCHAR    AS attendee_info,
    value['course']['id']::VARCHAR    AS course_id,
    value['course']['title']::VARCHAR AS course_title,
    value['endDate']::TIMESTAMP       AS meeting_end_date,
    value['id']::VARCHAR              AS meeting_id,
    value['instructors']::VARIANT     AS instructors,
    value['location']::VARCHAR        AS location,
    value['startDate']::TIMESTAMP     AS meeting_start_date,
    value['title']::VARCHAR           AS meeting_title,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        meeting_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
