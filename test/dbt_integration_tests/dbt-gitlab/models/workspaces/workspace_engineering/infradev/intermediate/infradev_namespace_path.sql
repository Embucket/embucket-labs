{{ config(
    materialized='ephemeral'
) }}

WITH namespaces AS (
    SELECT *
    FROM {{ ref('dim_namespace') }} 

), namespace_path AS (

    SELECT
      namespaces.dim_namespace_id,
      CASE
        WHEN namespace_4.dim_namespace_id IS NOT NULL THEN namespace_4.namespace_path || '/' ||
                                                           namespace_3.namespace_path || '/' ||
                                                           namespace_2.namespace_path || '/' ||
                                                           namespace_1.namespace_path || '/' ||
                                                           namespaces.namespace_path
        WHEN namespace_3.dim_namespace_id IS NOT NULL THEN namespace_3.namespace_path || '/' ||
                                                           namespace_2.namespace_path || '/' ||
                                                           namespace_1.namespace_path || '/' ||
                                                           namespaces.namespace_path
        WHEN namespace_2.dim_namespace_id IS NOT NULL THEN namespace_2.namespace_path || '/' ||
                                                           namespace_1.namespace_path || '/' ||
                                                           namespaces.namespace_path
        WHEN namespace_1.dim_namespace_id IS NOT NULL
          THEN namespace_1.namespace_path || '/' || namespaces.namespace_path
        ELSE namespaces.namespace_path
      END AS full_namespace_path
    FROM namespaces
    LEFT OUTER JOIN namespaces namespace_1
      ON namespace_1.dim_namespace_id = namespaces.parent_id AND namespaces.namespace_is_ultimate_parent = FALSE
    LEFT OUTER JOIN namespaces namespace_2
      ON namespace_2.dim_namespace_id = namespace_1.parent_id AND namespace_1.namespace_is_ultimate_parent = FALSE
    LEFT OUTER JOIN namespaces namespace_3
      ON namespace_3.dim_namespace_id = namespace_2.parent_id AND namespace_2.namespace_is_ultimate_parent = FALSE
    LEFT OUTER JOIN namespaces namespace_4
      ON namespace_4.dim_namespace_id = namespace_3.parent_id AND namespace_3.namespace_is_ultimate_parent = FALSE

)

SELECT *
FROM namespace_path