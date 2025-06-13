-- depends_on: {{ ref('projects_part_of_product_ops') }}
-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- These data models are required for this data model based on https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/gitlab_ops/xf/gitlab_ops_merge_requests_xf.sql
-- This data model is missing a lot of other source data models
WITH merge_requests AS (

    SELECT 
      {{ dbt_utils.star(from=ref('gitlab_ops_merge_requests')) }},
      created_at                                                                           AS merge_request_created_at,
      updated_at                                                                           AS merge_request_updated_at
    FROM {{ref('gitlab_ops_merge_requests')}} merge_requests

), label_links AS (

    SELECT *
    FROM {{ref('gitlab_ops_label_links')}}
    WHERE is_currently_valid = True
      AND target_type = 'MergeRequest'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_ops_labels_xf')}}

), agg_labels AS (

    SELECT
      merge_requests.merge_request_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC)  AS labels
    FROM merge_requests
    LEFT JOIN label_links
      ON merge_requests.merge_request_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY merge_requests.merge_request_id

),  latest_merge_request_metric AS (

    SELECT MAX(merge_request_metric_id) AS target_id
    FROM {{ref('gitlab_dotcom_merge_request_metrics_source')}}
    GROUP BY merge_request_id

),  merge_request_metrics AS (

    SELECT *
    FROM {{ref('gitlab_ops_merge_request_metrics')}}
    INNER JOIN latest_merge_request_metric
    ON merge_request_metric_id = target_id

), projects AS (

    SELECT *
    FROM {{ref('gitlab_ops_projects_xf')}}

), joined AS (

    SELECT
      merge_requests.*,
      merge_request_metrics.merged_at,
      projects.namespace_id,
      ARRAY_TO_STRING(agg_labels.labels,'|')                                        AS masked_label_title,
      agg_labels.labels, 
     IFF(merge_requests.target_project_id IN ({{is_project_included_in_engineering_metrics()}}),
        TRUE, FALSE)                                                                AS is_included_in_engineering_metrics,
      IFF(merge_requests.target_project_id IN ({{is_project_part_of_product_ops()}}),
        TRUE, FALSE)                                                                AS is_part_of_product_ops
    FROM merge_requests
    LEFT JOIN merge_request_metrics
        ON merge_requests.merge_request_id = merge_request_metrics.merge_request_id
    LEFT JOIN agg_labels
      ON merge_requests.merge_request_id = agg_labels.merge_request_id
    LEFT JOIN projects
        ON merge_requests.target_project_id = projects.project_id

)

SELECT *
FROM joined
