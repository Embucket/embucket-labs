WITH RECURSIVE issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}

), issues_moved_duplicated AS (

    SELECT
      *,
      IFNULL(moved_to_id, duplicated_to_id) AS moved_duplicated_to_id
    FROM issues

), recursive_cte(issue_id, moved_duplicated_to_id, issue_lineage) AS (

    SELECT
      issue_id,
      moved_duplicated_to_id,
      TO_ARRAY(issue_id) AS issue_lineage
    FROM issues_moved_duplicated
    WHERE moved_duplicated_to_id IS NULL

    UNION ALL

    SELECT
      iter.issue_id,
      iter.moved_duplicated_to_id,
      ARRAY_INSERT(anchor.issue_lineage, 0, iter.issue_id) AS issue_lineage
    FROM recursive_cte AS anchor
    INNER JOIN issues_moved_duplicated AS iter
      ON iter.moved_duplicated_to_id = anchor.issue_id

), mapping_results AS (

    SELECT
      issue_id                                                             AS issue_id,
      issue_lineage                                                        AS issue_lineage,
      issue_lineage[ARRAY_SIZE(issue_lineage) - 1]::NUMBER                 AS last_moved_duplicated_issue_id,
      IFF(last_moved_duplicated_issue_id != issue_id, TRUE, FALSE)         AS is_issue_moved_duplicated,
      --return final common dimension mapping,
      last_moved_duplicated_issue_id                                       AS dim_issue_id
    FROM recursive_cte

), final AS (

    SELECT
      mapping_results.issue_id,
      mapping_results.issue_lineage,
      mapping_results.last_moved_duplicated_issue_id,
      mapping_results.is_issue_moved_duplicated,
      prep_issue.dim_issue_sk
    FROM mapping_results
    INNER JOIN {{ ref('prep_issue') }}
      ON mapping_results.last_moved_duplicated_issue_id = prep_issue.issue_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-10-12",
    updated_date="2023-10-11",
) }}