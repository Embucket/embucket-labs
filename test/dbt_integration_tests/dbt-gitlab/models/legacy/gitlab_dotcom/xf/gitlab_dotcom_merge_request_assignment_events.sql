

WITH users AS (

    SELECT
      user_id,
      user_name
    FROM {{ ref('gitlab_dotcom_users') }} users
    WHERE {{ filter_out_blocked_users('users', 'user_id') }}

), notes AS (

    SELECT
      *,
      IFF(note LIKE 'Reassigned%', note, NULL)                                    AS reassigned,
      IFF(note LIKE 'assigned%', SPLIT_PART(note, 'unassigned ', 1), NULL)        AS assigned,
      IFF(note LIKE '%unassigned%', SPLIT_PART(note, 'unassigned ', 2), NULL)     AS unassigned
    FROM {{ ref('gitlab_dotcom_internal_notes_xf') }}
    WHERE noteable_type = 'MergeRequest'
      AND (note LIKE 'assigned to%' OR note LIKE 'unassigned%' OR note LIKE 'Reassigned%')

), notes_cleaned AS (

    SELECT
      note_id,
      noteable_id,
      note_author_id,
      created_at,
      note,
      event,
      "{{this.database}}".{{target.schema}}.regexp_to_array(event_string, '(?<=\@)(.*?)(?=(\\s|$|\,))') AS event_cleaned
    FROM notes
    UNPIVOT(event_string FOR event IN (assigned, unassigned, reassigned))
  
), notes_flat AS (

    SELECT
      notes_cleaned.*,
      f.index AS rank_in_event,
      f.value AS user_name
    FROM notes_cleaned,
    LATERAL FLATTEN(input => event_cleaned) f  

), joined AS (

    SELECT
      noteable_id  AS merge_request_id,
      note_id,
      note_author_id,
      created_at   AS note_created_at,
      LOWER(event) AS event,
      user_id      AS event_user_id,
      rank_in_event
    FROM notes_flat 
    INNER JOIN users
      ON notes_flat.user_name = users.user_name
    WHERE {{ filter_out_blocked_users('notes_flat', 'note_author_id') }} 

)

SELECT *
FROM joined
ORDER BY 1,2,7
