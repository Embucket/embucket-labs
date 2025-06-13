
WITH source AS (
  SELECT
    *
  FROM {{ ref('gitlab_dotcom_namespaces_snapshots_base') }}
),
/*
This CTE finds groups of snapshoted chages that changed the parent id. This is a typical 'gaps and islands' problem.
*/
  parent_groups AS (
    SELECT
      namespace_id,
      parent_id,
      IFNULL(parent_id, -1)                                                             AS no_null_parent_id,
      LAG(no_null_parent_id, 1, -1)
          OVER (PARTITION BY namespace_id ORDER BY valid_from)                          AS lag_parent_id,
      CONDITIONAL_TRUE_EVENT(no_null_parent_id != lag_parent_id)
                             OVER ( PARTITION BY namespace_id ORDER BY valid_from)      AS parent_id_group,
      valid_from,
      IFNULL(valid_to, CURRENT_DATE())                                                  AS valid_to
    FROM source
),
  parent_change AS (
    SELECT
      namespace_id,
      parent_id,
      parent_id_group,
      MIN(valid_from)  AS valid_from,
      MAX(valid_to)    AS valid_to
    FROM parent_groups
    GROUP BY 1,2,3
  ),
  recursive_namespace_lineage(namespace_id, parent_id, valid_to, valid_from, valid_to_list, valid_from_list,
                               upstream_lineage) AS (
    SELECT
      root.namespace_id,
      root.parent_id,
      root.valid_to,
      root.valid_from,
      TO_ARRAY(root.valid_to)     AS valid_to_list,
      TO_ARRAY(root.valid_from)   AS valid_from_list,
      TO_ARRAY(root.namespace_id) AS upstream_lineage
    FROM parent_change AS root
    WHERE parent_id IS NULL

    UNION ALL

    SELECT
      iter.namespace_id,
      iter.parent_id,
      iter.valid_to,
      iter.valid_from,
      ARRAY_APPEND(anchor.valid_to_list, iter.valid_to)        AS valid_to_list,
      ARRAY_APPEND(anchor.valid_from_list, iter.valid_from)    AS valid_from_list,
      ARRAY_APPEND(anchor.upstream_lineage, iter.namespace_id) AS upstream_lineage
    FROM recursive_namespace_lineage AS anchor
    INNER JOIN parent_change AS iter
      ON iter.parent_id = anchor.namespace_id
      AND NOT ARRAY_CONTAINS(iter.namespace_id::VARIANT, anchor.upstream_lineage)
      AND (CASE
             WHEN iter.valid_from BETWEEN anchor.valid_from AND anchor.valid_to THEN TRUE
             WHEN iter.valid_to BETWEEN anchor.valid_from AND anchor.valid_to THEN TRUE
             WHEN anchor.valid_from BETWEEN iter.valid_from AND iter.valid_to THEN TRUE
             ELSE FALSE
           END) = TRUE
  ),

  namespace_lineage_scd AS (
    SELECT
      recursive_namespace_lineage.namespace_id,
      recursive_namespace_lineage.parent_id,
      recursive_namespace_lineage.upstream_lineage,
      recursive_namespace_lineage.upstream_lineage[0]::NUMBER     AS ultimate_parent_id,
      ARRAY_SIZE(recursive_namespace_lineage.upstream_lineage)    AS lineage_depth,
      recursive_namespace_lineage.valid_from_list,
      recursive_namespace_lineage.valid_to_list,
      MAX(from_list.value::TIMESTAMP)                             AS lineage_valid_from,
      MIN(to_list.value::TIMESTAMP)                               AS lineage_valid_to
    FROM recursive_namespace_lineage
    INNER JOIN LATERAL FLATTEN(INPUT =>valid_from_list) from_list
    INNER JOIN LATERAL FLATTEN(INPUT =>valid_to_list) to_list
    GROUP BY 1,2,3,4,5,6,7
    HAVING lineage_valid_to > lineage_valid_from
  ),
  event_index AS (
    SELECT
      {{ dbt_utils.generate_surrogate_key(['namespace_id', 'lineage_valid_from'] ) }}    AS namespace_lineage_id,
      namespace_id,
      parent_id,
      upstream_lineage,
      ultimate_parent_id,
      lineage_depth,
      lineage_valid_from,
      lineage_valid_to,
      ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY lineage_valid_from) AS sequence_number,
      IFF(lineage_valid_to = CURRENT_DATE(), TRUE, FALSE)                       AS is_current
    FROM namespace_lineage_scd
  )


{{ dbt_audit(
    cte_ref="event_index",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2021-11-16",
    updated_date="2021-11-16"
) }}


