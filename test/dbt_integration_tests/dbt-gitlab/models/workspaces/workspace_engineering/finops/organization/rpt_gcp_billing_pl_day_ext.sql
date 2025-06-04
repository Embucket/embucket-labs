WITH pl_day AS (

  SELECT *
  FROM {{ ref('rpt_gcp_billing_pl_day') }}

),

/*
For every new category breakout, a new cte is created to join with the previous one
*/

join_product_component AS (

  SELECT
    *,
    CASE
      WHEN from_mapping LIKE '%ci_runner_pl_daily%'
        OR from_mapping LIKE 'build_artifacts%'
        OR infra_label IN ('continuous_integration', 'runner_saas', 'build_artifacts') THEN 'Production: Continuous Integration'
      WHEN from_mapping LIKE '%haproxy-%' OR infra_label = 'pages' THEN 'Production: HAProxy'
      WHEN from_mapping LIKE 'repo_storage%' OR infra_label IN ('git_lfs', 'gitaly') THEN 'Production: Gitaly'
      WHEN from_mapping LIKE 'container_registry%' OR infra_label IN ('container_registry', 'registry') THEN 'Production: Container Registry'
      WHEN 
        from_mapping IN ('folder_pl', 'projects_pl') 
          OR infra_label = 'security' 
          OR gcp_project_id LIKE ANY ('gitlab-ci-private-%', 'gitlab-r-saas-l-m-amd64-org-%', 'gitlab-r-saas-l-p-amd64-%') 
          OR infra_label = 'infrastructure' THEN 'R&D - Staging, Ops, Quality, Security, Demos, Sandboxes'
      WHEN gcp_project_id = 'unreview-poc-390200e5' THEN 'AI'
      WHEN infra_label = 'dependency_proxy' THEN 'Production: Dependency Proxy'
      WHEN infra_label = 'shared' AND from_mapping = 'infralabel_pl' THEN 'WIP: Unallocated Production costs'
      WHEN infra_label IS NULL AND from_mapping IS NULL THEN
        CASE WHEN gcp_project_id != 'gitlab-production' THEN 'WIP: Unallocated costs'
          WHEN gcp_project_id = 'gitlab-production' THEN 'WIP: Unallocated Production costs' END
      ELSE CONCAT(infra_label, '-', from_mapping)
    END AS product_component
  FROM pl_day
  

),

join_finance_component AS (

  SELECT
    *,
    CASE
      -- STORAGE
      WHEN LOWER(gcp_service_description) = 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd capacity%' THEN 'Storage'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd snapshot%' THEN 'Storage'
      WHEN (LOWER(gcp_service_description) = 'bigquery' AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Storage'
      WHEN (LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Storage'

      -- COMPUTE
      WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'--keep
      WHEN LOWER(gcp_service_description) = 'vertex ai' THEN 'AI/ML'--new category
      WHEN LOWER(gcp_service_description) = 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%gpu%' or LOWER(gcp_sku_description) like '%prediction%') THEN 'AI/ML'--new category
      WHEN LOWER(gcp_service_description) = 'kubernetes engine' THEN 'Compute'
      WHEN LOWER(gcp_service_description) = 'bigquery' AND LOWER(gcp_sku_description) NOT LIKE 'storage' THEN 'Compute'
      WHEN LOWER(gcp_service_description) = 'cloud sql' AND (LOWER(gcp_sku_description) LIKE '%cpu%' OR LOWER(gcp_sku_description) LIKE '%ram%') THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%capacity%' THEN 'Compute'

      -- NETWORKING
      WHEN LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) = 'cloud pub/sub' THEN 'Networking'--keep
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
      WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Networking'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%ip%' or LOWER(gcp_sku_description) LIKE '%network%' or LOWER(gcp_sku_description) LIKE '%upload%'  or LOWER(gcp_sku_description) LIKE '%download%') THEN 'Networking'--keep
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND (LOWER(gcp_sku_description) LIKE '%network%' OR LOWER(gcp_sku_description) LIKE '%download%' or LOWER(gcp_sku_description) LIKE '%cdn%') THEN 'Networking'

      -- SUPPORT
      WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Support'
      WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Support'
      WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Support'

      -- OTHERS
      when lower(gcp_service_description) = 'cloud storage' then 'Storage'
      when LOWER(gcp_service_description) = 'compute engine' then 'Compute'
      ELSE gcp_service_description
    END AS finance_sku_type,

    CASE
      -- STORAGE
      WHEN LOWER(gcp_service_description) = 'cloud storage' and ((LOWER(gcp_sku_description) LIKE '%standard storage%') or (LOWER(gcp_sku_description) LIKE '%coldline storage%') or (LOWER(gcp_sku_description) LIKE '%archive storage%') or (LOWER(gcp_sku_description) LIKE '%nearline storage%')) THEN 'Object (storage)'
      WHEN LOWER(gcp_service_description) = 'cloud storage' and LOWER(gcp_sku_description) LIKE '%operations%' THEN 'Object (operations)'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%pd capacity%' OR LOWER(gcp_sku_description) LIKE '%pd snapshot%') THEN 'Repository (storage) - to be refined'
      WHEN (LOWER(gcp_service_description) = 'bigquery' AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Data Warehouse (storage)'
      WHEN (LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%storage%') THEN 'Databases (storage)'

      -- COMPUTE
      WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage' --keep
      WHEN LOWER(gcp_service_description) = 'kubernetes engine' THEN 'Container orchestration (compute)'--keep
      WHEN LOWER(gcp_service_description) = 'vertex ai' THEN 'AI/ML (compute)'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND LOWER(gcp_sku_description) LIKE '%gpu%' THEN 'AI/ML (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'bigquery' AND LOWER(gcp_sku_description) NOT LIKE 'storage' THEN 'Data Warehouse (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud sql' AND (LOWER(gcp_sku_description) LIKE '%cpu%' OR LOWER(gcp_sku_description) LIKE '%ram%') THEN 'Data Warehouse (compute)'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%capacity%' THEN 'Memorystores (compute)'

      -- NETWORKING
      WHEN LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Databases (networking)'
      WHEN LOWER(gcp_service_description) = 'cloud pub/sub' THEN 'Messaging (networking)'--keep
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%networking%' THEN 'Memorystores (networking)'
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND LOWER(gcp_sku_description) LIKE '%egress%' THEN case when LOWER(gcp_sku_description) LIKE '%multi-region within%' then 'Object (networking)' else 'Object (networking) - to be refined' end --keep
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND LOWER(gcp_sku_description) LIKE '%cdn%' THEN case when LOWER(gcp_sku_description) NOT LIKE '%from%' then 'Object CDN (networking)' else 'Object CDN (networking) - to be refined' end--keep
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND (LOWER(gcp_sku_description) LIKE '%network%' OR LOWER(gcp_sku_description) LIKE '%download%') THEN 'Networking on Buckets'--rename
      WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Load Balancing (networking)'
      WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking (mixed) - to be refined'

      -- SUPPORT
      WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Google Support (support)'
      WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Security (support)'
      WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Marketplace (support)'

      -- OTHERS
      when lower(gcp_service_description) = 'cloud storage' then 'Storage'
      when LOWER(gcp_service_description) = 'compute engine' then 'Compute'
      ELSE gcp_service_description
    END AS finance_sku_subtype
  FROM join_product_component
  
),

join_hierarchy_component AS (
    SELECT jf.*,
    level_0,
    level_1,
    level_2,
    level_3,
    level_4
    FROM join_finance_component jf
    LEFT JOIN {{ ref('gcp_billing_hierarchy')}} hi
    ON coalesce(jf.full_path, '') like coalesce(hi.full_path, '')
      AND coalesce(jf.from_mapping, '') = coalesce(hi.from_mapping, '')
      AND (coalesce(jf.infra_label, '') = coalesce(hi.infra_label, '') or hi.infra_label IS NULL)
      AND (coalesce(jf.gcp_project_id, '') = coalesce(hi.gcp_project_id, '') or hi.gcp_project_id IS NULL)

)

SELECT * FROM join_hierarchy_component
