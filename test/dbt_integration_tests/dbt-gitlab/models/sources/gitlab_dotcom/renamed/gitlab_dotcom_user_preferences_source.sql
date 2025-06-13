WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_preferences_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                           AS user_id,
      issue_notes_filter::VARCHAR               AS issue_notes_filter,
      merge_request_notes_filter::VARCHAR       AS merge_request_notes_filter,
      created_at::TIMESTAMP                     AS created_at,
      updated_at::TIMESTAMP                     AS updated_at,
      epics_sort::VARCHAR                       AS epic_sort,
      roadmap_epics_state::VARCHAR              AS roadmap_epics_state,
      epic_notes_filter::VARCHAR                AS epic_notes_filter,
      issues_sort::VARCHAR                      AS issues_sort,
      merge_requests_sort::VARCHAR              AS merge_requests_sort,
      roadmaps_sort::VARCHAR                    AS roadmaps_sort,
      first_day_of_week::VARCHAR                AS first_day_of_week,
      timezone::VARCHAR                         AS timezone,
      true::BOOLEAN            AS time_display_relative,
      true::BOOLEAN               AS time_format_in_24h,
      projects_sort::VARCHAR                    AS projects_sort,
      true::BOOLEAN         AS show_whitespace_in_diffs,
      true::BOOLEAN              AS sourcegraph_enabled,
      true::BOOLEAN                AS setup_for_company,
      true::BOOLEAN        AS render_whitespace_in_code,
      tab_width::VARCHAR                        AS tab_width,
      experience_level::NUMBER                  AS experience_level,
      true::BOOLEAN          AS view_diffs_file_by_file,
      true::BOOLEAN AS early_access_program_participant

    FROM source

)

SELECT  *
FROM renamed
