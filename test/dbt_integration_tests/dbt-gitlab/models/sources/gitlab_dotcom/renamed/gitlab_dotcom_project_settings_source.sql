WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_settings_dedupe_source') }}
  
), renamed AS (

  SELECT
    project_id::NUMBER                                    AS project_id,
    created_at::TIMESTAMP                                 AS created_at, 
    updated_at::TIMESTAMP                                 AS updated_at,
    push_rule_id::NUMBER                                  AS push_rule_id,
    show_default_award_emojis::BOOLEAN                    AS show_default_award_emojis, 
    allow_merge_on_skipped_pipeline::BOOLEAN              AS allow_merge_on_skipped_pipeline,
    squash_option::NUMBER                                 AS squash_option,
    has_confluence::BOOLEAN                               AS has_confluence,
    has_vulnerabilities::BOOLEAN                          AS has_vulnerabilities,
    prevent_merge_without_jira_issue::BOOLEAN             AS prevent_merge_without_jira_issue,
    cve_id_request_enabled::BOOLEAN                       AS cve_id_request_enabled,
    mr_default_target_self::BOOLEAN                       AS mr_default_target_self,
    previous_default_branch::VARCHAR                      AS previous_default_branch,
    warn_about_potentially_unwanted_characters::BOOLEAN   AS warn_about_potentially_unwanted_characters,
    merge_commit_template::VARCHAR                        AS merge_commit_template,
    has_shimo::BOOLEAN                                    AS has_shimo,
    squash_commit_template::VARCHAR                       AS squash_commit_template,
    legacy_open_source_license_available::BOOLEAN         AS legacy_open_source_license_available,
    target_platforms::VARCHAR                             AS target_platforms,
    enforce_auth_checks_on_uploads::BOOLEAN               AS enforce_auth_checks_on_uploads
  FROM source

)

SELECT *
FROM renamed
