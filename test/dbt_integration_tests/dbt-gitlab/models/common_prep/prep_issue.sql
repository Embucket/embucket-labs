-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

{% set fields_to_mask = ['issue_title', 'issue_description'] %}

{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('prep_project', 'prep_project'),
    ('prep_issue_severity', 'prep_issue_severity'),
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels'),
    ('prep_epic', 'prep_epic'),
    ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
    ('prep_user', 'prep_user'),
    ('prep_milestone', 'prep_milestone'),
    ('gitlab_dotcom_epic_issues_source', 'gitlab_dotcom_epic_issues_source'),
    ('prep_epic', 'prep_epic'),
    ('gitlab_dotcom_routes_source', 'gitlab_dotcom_routes_source'),
    ('gitlab_dotcom_award_emoji_source', 'gitlab_dotcom_award_emoji_source'),
    ('gitlab_dotcom_work_item_type_source', 'gitlab_dotcom_work_item_type_source'),
    ('gitlab_dotcom_issues_source', 'gitlab_dotcom_issues_source')
]) }}

, issue_metrics AS (

    /* In a very small number of cases there are duplicate records for some issues with a
       created_at and updated_at time that varies by seconds from the otherwise identical record.
       We have confirmed with Engineering Analytics this is likely an ETL issue and we can dedupe this data.
    */

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_metrics_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY updated_at DESC) = 1

), namespace_prep AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE

), first_events_weight AS (

    SELECT
      issue_id,
      MIN(created_at) AS first_weight_set_at
    FROM {{ ref('gitlab_dotcom_resource_weight_events_source') }}
    GROUP BY 1

), upvote_count AS (

    SELECT
      awardable_id                                        AS issue_id,
      SUM(IFF(award_emoji_name LIKE 'thumbsup%', 1, 0))   AS thumbsups_count,
      SUM(IFF(award_emoji_name LIKE 'thumbsdown%', 1, 0)) AS thumbsdowns_count,
      thumbsups_count - thumbsdowns_count                 AS upvote_count
    FROM gitlab_dotcom_award_emoji_source
    WHERE awardable_type = 'Issue'
    GROUP BY 1

), agg_labels AS (

    SELECT
      gitlab_dotcom_issues_source.issue_id                                                          AS issue_id,
      ARRAY_AGG(LOWER(prep_labels.label_title)) WITHIN GROUP (ORDER BY prep_labels.label_title ASC) AS labels
    FROM gitlab_dotcom_issues_source
    LEFT JOIN prep_label_links
        ON gitlab_dotcom_issues_source.issue_id = prep_label_links.issue_id
    LEFT JOIN prep_labels
        ON prep_label_links.dim_label_id = prep_labels.dim_label_id
    GROUP BY gitlab_dotcom_issues_source.issue_id


), renamed AS (

    SELECT

      -- SURROGATE KEY
      {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_issues_source.issue_id']) }} AS dim_issue_sk,

      -- NATURAL KEY
      gitlab_dotcom_issues_source.issue_id                                             AS issue_id,

      -- LEGACY NATURAL KEY
      gitlab_dotcom_issues_source.issue_id                                             AS dim_issue_id,

      -- FOREIGN KEYS
      prep_project.dim_project_sk,
      namespace_prep.dim_namespace_sk,
      namespace_prep.ultimate_parent_namespace_id,
      prep_epic.dim_epic_sk,
      prep_date.date_id                                                                AS created_date_id,
      prep_gitlab_dotcom_plan.dim_plan_sk                                              AS dim_plan_sk_at_creation,
      prep_milestone.dim_milestone_sk,
      gitlab_dotcom_issues_source.sprint_id,
      author.dim_user_sk                                                               AS dim_user_author_sk,
      updated_by.dim_user_sk                                                           AS dim_user_updated_by_sk,
      last_edited_by.dim_user_sk                                                       AS dim_user_last_edited_by_sk,
      closed_by.dim_user_sk                                                            AS dim_user_closed_by_sk,
      -- maintained to keep prep_event working until all of gitlab.com lineage has surrogate keys available for all event sources
      prep_gitlab_dotcom_plan.dim_plan_id                                              AS dim_plan_id_at_creation,
      prep_project.project_id,
      gitlab_dotcom_issues_source.author_id,

      gitlab_dotcom_issues_source.moved_to_id,
      gitlab_dotcom_issues_source.duplicated_to_id,
      gitlab_dotcom_issues_source.promoted_to_epic_id,

      gitlab_dotcom_issues_source.created_at,
      gitlab_dotcom_issues_source.updated_at,
      gitlab_dotcom_issues_source.issue_last_edited_at,
      gitlab_dotcom_issues_source.issue_closed_at,
      gitlab_dotcom_issues_source.is_confidential,
      {% for field in fields_to_mask %}
        CASE
          WHEN gitlab_dotcom_issues_source.is_confidential = TRUE
            THEN 'confidential - masked'
          WHEN prep_project.visibility_level != 'public'
          AND namespace_prep.namespace_is_internal = FALSE
            THEN 'private/internal - masked'
          ELSE {{field}}
        END                                                                            AS {{field}},
      {% endfor %}
      gitlab_dotcom_issues_source.issue_iid                                            AS issue_internal_id,
      gitlab_dotcom_issues_source.weight,
      gitlab_dotcom_issues_source.due_date,
      gitlab_dotcom_issues_source.lock_version,
      gitlab_dotcom_issues_source.time_estimate,
      gitlab_dotcom_issues_source.has_discussion_locked,
      gitlab_dotcom_issues_source.relative_position,
      gitlab_dotcom_issues_source.service_desk_reply_to,
      gitlab_dotcom_issues_source.state_id                                             AS issue_state_id,
      {{ map_state_id('gitlab_dotcom_issues_source.state_id') }}                       AS issue_state,
      gitlab_dotcom_work_item_type_source.work_item_type_name                          AS issue_type,
      CASE
        WHEN prep_issue_severity.severity = 4
          THEN 'S1'
        WHEN ARRAY_CONTAINS('severity::1'::variant, agg_labels.labels)
            OR ARRAY_CONTAINS('s1'::variant, agg_labels.labels)
            THEN 'S1'
        WHEN prep_issue_severity.severity = 3
          THEN 'S2'
        WHEN ARRAY_CONTAINS('severity::2'::variant, agg_labels.labels)
            OR ARRAY_CONTAINS('s2'::variant, agg_labels.labels)
            THEN 'S2'
        WHEN prep_issue_severity.severity = 2
          THEN 'S3'
        WHEN ARRAY_CONTAINS('severity::3'::variant, agg_labels.labels)
            OR ARRAY_CONTAINS('s3'::variant, agg_labels.labels)
            THEN 'S3'
        WHEN prep_issue_severity.severity = 1
          THEN 'S4'
        WHEN ARRAY_CONTAINS('severity::4'::variant, agg_labels.labels)
            OR ARRAY_CONTAINS('s4'::variant, agg_labels.labels)
            THEN 'S4'
        ELSE NULL
      END                                                                              AS severity,
      IFF(prep_project.visibility_level = 'private',
        'private - masked',
        'https://gitlab.com/' || gitlab_dotcom_routes_source.path || '/issues/' || gitlab_dotcom_issues_source.issue_iid)
                                                                                       AS issue_url,
      IFF(prep_project.visibility_level = 'private',
        'private - masked',
        prep_milestone.milestone_title)                                                AS milestone_title,
      prep_milestone.milestone_due_date,
      agg_labels.labels,
      ARRAY_TO_STRING(agg_labels.labels,',')                                           AS masked_label_title,
      IFNULL(upvote_count.upvote_count, 0)                                             AS upvote_count,
      issue_metrics.first_mentioned_in_commit_at,
      issue_metrics.first_associated_with_milestone_at,
      issue_metrics.first_added_to_board_at,
      namespace_prep.namespace_is_internal                                             AS is_internal_issue,
      first_events_weight.first_weight_set_at,
      CASE
      WHEN ARRAY_CONTAINS('priority::1'::variant, agg_labels.labels)
        OR ARRAY_CONTAINS('P1'::variant, agg_labels.labels)
        THEN 'priority 1'
      WHEN ARRAY_CONTAINS('priority::2'::variant, agg_labels.labels)
        OR ARRAY_CONTAINS('P2'::variant, agg_labels.labels)
        THEN 'priority 2'
      WHEN ARRAY_CONTAINS('priority::3'::variant, agg_labels.labels)
        OR ARRAY_CONTAINS('P3'::variant, agg_labels.labels)
        THEN 'priority 3'
      WHEN ARRAY_CONTAINS('priority::4'::variant, agg_labels.labels)
        OR ARRAY_CONTAINS('P4'::variant, agg_labels.labels)
        THEN 'priority 4'
      ELSE 'undefined'
    END                                                                                AS priority,

    CASE
      WHEN namespace_prep.namespace_id = 9970
        AND ARRAY_CONTAINS('security'::variant, agg_labels.labels)
        THEN TRUE
      ELSE FALSE
    END                                                                                AS is_security_issue,

    IFF(gitlab_dotcom_issues_source.project_id IN ({{is_project_included_in_engineering_metrics()}}),
      TRUE, FALSE)                                                                     AS is_included_in_engineering_metrics,
    IFF(gitlab_dotcom_issues_source.project_id IN ({{is_project_part_of_product()}}),
      TRUE, FALSE)                                                                     AS is_part_of_product,
    CASE
      WHEN namespace_prep.namespace_id = 9970
        AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.labels)
        THEN TRUE
      ELSE FALSE
      END                                                                              AS is_community_contributor_related,
    gitlab_dotcom_issues_source.is_deleted                                             AS is_deleted,
    gitlab_dotcom_issues_source.is_deleted_updated_at                                  AS is_deleted_updated_at
    FROM gitlab_dotcom_issues_source
    LEFT JOIN agg_labels
        ON gitlab_dotcom_issues_source.issue_id = agg_labels.issue_id
    LEFT JOIN prep_project
      ON gitlab_dotcom_issues_source.project_id = prep_project.project_id
    LEFT JOIN prep_namespace_plan_hist
      ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_issues_source.created_at >= prep_namespace_plan_hist.valid_from
      AND gitlab_dotcom_issues_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_date
      ON TO_DATE(gitlab_dotcom_issues_source.created_at) = prep_date.date_day
    LEFT JOIN prep_issue_severity
      ON gitlab_dotcom_issues_source.issue_id = prep_issue_severity.issue_id
    LEFT JOIN gitlab_dotcom_epic_issues_source
      ON gitlab_dotcom_issues_source.issue_id = gitlab_dotcom_epic_issues_source.issue_id
    LEFT JOIN gitlab_dotcom_routes_source
      ON gitlab_dotcom_routes_source.source_id = gitlab_dotcom_issues_source.project_id
      AND gitlab_dotcom_routes_source.source_type = 'Project'
    LEFT JOIN prep_milestone
      ON prep_milestone.milestone_id = gitlab_dotcom_issues_source.milestone_id
    LEFT JOIN upvote_count
      ON upvote_count.issue_id = gitlab_dotcom_issues_source.issue_id
    LEFT JOIN issue_metrics
      ON gitlab_dotcom_issues_source.issue_id = issue_metrics.issue_id
    LEFT JOIN namespace_prep
      ON prep_project.dim_namespace_sk = namespace_prep.dim_namespace_sk
    LEFT JOIN first_events_weight
      ON gitlab_dotcom_issues_source.issue_id = first_events_weight.issue_id
    LEFT JOIN prep_epic
      ON gitlab_dotcom_epic_issues_source.epic_id = prep_epic.epic_id
    LEFT JOIN prep_gitlab_dotcom_plan
      ON IFNULL(prep_namespace_plan_hist.dim_plan_id, 34) = prep_gitlab_dotcom_plan.dim_plan_id
    LEFT JOIN prep_user author
      ON gitlab_dotcom_issues_source.author_id = author.user_id
    LEFT JOIN prep_user updated_by
      ON gitlab_dotcom_issues_source.updated_by_id = updated_by.user_id
    LEFT JOIN prep_user last_edited_by
      ON gitlab_dotcom_issues_source.last_edited_by_id = last_edited_by.user_id
    LEFT JOIN prep_user closed_by
      ON gitlab_dotcom_issues_source.closed_by_id = closed_by.user_id
    LEFT JOIN gitlab_dotcom_work_item_type_source
      ON gitlab_dotcom_issues_source.work_item_type_id = gitlab_dotcom_work_item_type_source.work_item_type_id
    WHERE gitlab_dotcom_issues_source.project_id IS NOT NULL
)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@utkarsh060",
    created_date="2021-06-17",
    updated_date="2024-07-09"
) }}
