-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

WITH internal_projects AS (

  SELECT *
  FROM {{ ref('map_project_internal') }}

),

merge_requests AS (

  SELECT
    {{ dbt_utils.star(from=ref('gitlab_dotcom_merge_requests_source'), relation_alias="merge_requests" ) }},
    internal_projects.parent_namespace_id          AS namespace_id,
    internal_projects.ultimate_parent_namespace_id AS ultimate_parent_id,
    merge_requests.created_at                      AS merge_request_created_at,
    merge_requests.updated_at                      AS merge_request_updated_at
  FROM {{ ref('gitlab_dotcom_merge_requests_source') }} AS merge_requests
  INNER JOIN internal_projects
    ON merge_requests.target_project_id = internal_projects.project_id

),

merge_request_metrics AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_metrics_source') }}
  QUALIFY MAX(merge_request_metric_id) OVER (PARTITION BY merge_request_id) = merge_request_metric_id

),

label_links AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_label_links_source') }}
  WHERE is_currently_valid = TRUE
    AND target_type = 'MergeRequest'

),

all_labels AS (

  SELECT labels.*
  FROM {{ ref('gitlab_dotcom_labels_source') }} AS labels

),

agg_labels AS (

  SELECT
    merge_requests.merge_request_id,
    ARRAY_AGG(LOWER(all_labels.label_title)) WITHIN GROUP (ORDER BY all_labels.label_title ASC) AS labels
  FROM merge_requests
  LEFT JOIN label_links
    ON merge_requests.merge_request_id = label_links.target_id
  LEFT JOIN all_labels
    ON label_links.label_id = all_labels.label_id
  GROUP BY merge_requests.merge_request_id

),

milestones AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_milestones_source') }}

),

joined AS (

  SELECT
    merge_requests.*,
    milestones.milestone_title,
    milestones.milestone_description,
    ARRAY_TO_STRING(agg_labels.labels, '|')                                                            AS label_title,
    agg_labels.labels,
    merge_request_metrics.merged_at,
    IFF(merge_requests.target_project_id IN ({{ is_project_included_in_engineering_metrics() }}),
      TRUE, FALSE)                                                                                     AS is_included_in_engineering_metrics,
    IFF(merge_requests.target_project_id IN ({{ is_project_part_of_product() }}),
      TRUE, FALSE)                                                                                     AS is_part_of_product,
    IFF(ARRAY_CONTAINS('community contribution'::VARIANT, agg_labels.labels),
      TRUE, FALSE)                                                                                     AS is_community_contributor_related,
    TIMESTAMPDIFF(HOURS, merge_requests.merge_request_created_at, merge_request_metrics.merged_at)     AS hours_to_merged_status,
    REGEXP_COUNT(merge_requests.merge_request_description, '([-+*]|[\d+\.]) [\[]( |[xX])[\]]', 1, 'm') AS total_checkboxes,
    REGEXP_COUNT(merge_requests.merge_request_description, '([-+*]|[\d+\.]) [\[][xX][\]]', 1, 'm')     AS completed_checkboxes
    -- Original regex, (?:(?:>\s{0,4})*)(?:\s*(?:[-+*]|(?:\d+\.)))+\s+(\[\s\]|\[[xX]\])(\s.+), found in https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/concerns/taskable.rb

  FROM merge_requests
  LEFT JOIN agg_labels
    ON merge_requests.merge_request_id = agg_labels.merge_request_id
  LEFT JOIN merge_request_metrics
    ON merge_requests.merge_request_id = merge_request_metrics.merge_request_id
  LEFT JOIN milestones
    ON merge_requests.milestone_id = milestones.milestone_id
)

SELECT *
FROM joined
