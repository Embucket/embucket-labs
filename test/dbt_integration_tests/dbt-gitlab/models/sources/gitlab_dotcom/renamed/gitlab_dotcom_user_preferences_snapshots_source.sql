WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_user_preferences_snapshots') }}

), renamed AS (

    SELECT
      user_id::NUMBER AS user_id,
      issue_notes_filter::VARCHAR AS issue_notes_filter,
      merge_request_notes_filter::VARCHAR AS merge_request_notes_filter,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      epics_sort::VARCHAR   AS epic_sort,
      roadmap_epics_state::VARCHAR AS roadmap_epics_state,
      epic_notes_filter::VARCHAR AS epic_notes_filter,
      issues_sort::VARCHAR AS issues_sort,
      merge_requests_sort::VARCHAR AS merge_requests_sort,
      roadmaps_sort::VARCHAR AS roadmaps_sort,
      first_day_of_week::VARCHAR AS first_day_of_week,
      timezone::VARCHAR AS timezone,
      time_display_relative::BOOLEAN AS time_display_relative,
      time_format_in_24h::BOOLEAN AS time_format_in_24h,
      projects_sort::VARCHAR AS projects_sort,
      show_whitespace_in_diffs::BOOLEAN AS show_whitespace_in_diffs,
      sourcegraph_enabled::BOOLEAN AS sourcegraph_enabled,
      setup_for_company::BOOLEAN AS setup_for_company,
      render_whitespace_in_code::BOOLEAN AS render_whitespace_in_code,
      tab_width::VARCHAR AS tab_width,
      experience_level::NUMBER AS experience_level,
      view_diffs_file_by_file::BOOLEAN AS view_diffs_file_by_file,
      dbt_valid_from::TIMESTAMP AS dbt_valid_from,
      dbt_valid_to::TIMESTAMP AS dbt_valid_to
    FROM source
    
)

SELECT  *
FROM renamed