{{ config(
    materialized='table',
    )
}}




WITH credits AS ( --excluding migration credit between march 7th and March 27th: https://gitlab.com/gitlab-org/quality/engineering-analytics/finops/finops-analysis/-/issues/142

  SELECT
    cr.source_primary_key,
    SUM(coalesce(cr.credit_amount, 0)) AS total_credit
  FROM {{ ref('gcp_billing_export_credits') }} AS cr
  WHERE LOWER(cr.credit_description) != 'migration-credit-1-1709765302259'
  GROUP BY cr.source_primary_key

),

export AS (

  SELECT xf.*, cr.total_credit FROM {{ ref('gcp_billing_export_xf') }} xf
  LEFT JOIN credits cr ON cr.source_primary_key = xf.source_primary_key
  WHERE invoice_month >= '2023-01-01'

),

infra_labels AS (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'gl_product_category'

),

env_labels AS (

  SELECT * FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'env'

),

runner_labels AS (

  SELECT
    source_primary_key,
    CASE
      WHEN resource_label_value LIKE '%gpu%' THEN
        CASE WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-%' THEN '8 - shared saas runners gpu - medium'
          WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-%' THEN '9 - shared saas runners gpu - large'
          ELSE resource_label_value
        END
      WHEN resource_label_value LIKE '%runners-manager-shared-blue-%' THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%runners-manager-shared-green-%' THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%gitlab-shared-runners-manager-%' THEN '1 - shared gitlab org runners' --ok
      WHEN resource_label_value LIKE '%shared-runners-manager-%' THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-small-amd64-%' THEN '2 - shared saas runners - small'
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-amd64-%' THEN '3 - shared saas runners - medium' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-amd64-%' THEN '4 - shared saas runners - large' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-xlarge-amd64-%' THEN '10 - shared saas runners - xlarge'
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-2xlarge-amd64-%' THEN '11 - shared saas runners - 2xlarge'
      WHEN resource_label_value LIKE '%runners-manager-saas-macos-staging-%' THEN 'runners-manager-saas-macos-staging-'
      WHEN resource_label_value LIKE '%runners-manager-saas-macos%-m1-%' THEN '5 - shared saas macos runners'
      WHEN resource_label_value LIKE '%runners-manager-shared-gitlab-org-%' THEN '1 - shared gitlab org runners' --'1 - shared gitlab org runners' --ok
      WHEN resource_label_value LIKE '%runners-manager-private-%' THEN '6 - private internal runners' --ok, project_pl internal
      WHEN resource_label_value LIKE '%private-runners-manager-%' THEN '6 - private internal runners' --ok, project_pl internal
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%shared-gitlab-org-%') THEN '1 - shared gitlab org runners' --ok
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%amd64%') THEN 'runners-saas'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%s-shared-%') THEN '2 - shared saas runners - small' --ok
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%-shared-%' AND resource_label_value NOT LIKE '%gitlab%') THEN '2 - shared saas runners - small' --ok
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%-private-%') THEN '6 - private internal runners' --ok, project_pl internal

      WHEN resource_label_value LIKE '%gke-runners-gke-default-pool-%' THEN 'gke-runners-gke-default-pool-'
      WHEN resource_label_value LIKE '%test-machine-%' THEN 'test-machine-'
      WHEN resource_label_value LIKE '%tm-runner-%' THEN 'tm-runner-'
      WHEN resource_label_value LIKE '%tm-test-instance%' THEN 'tm-test-instance'
      WHEN resource_label_value LIKE '%gitlab-temporary-gcp-image-%' THEN 'gitlab-temporary-gcp-image-'
      WHEN resource_label_value LIKE '%sd-exporter%' THEN 'sd-exporter'
      WHEN resource_label_value LIKE '%/bastion-%' THEN 'bastion'
      WHEN resource_label_value LIKE '%/gitlab-qa-tunnel%' THEN 'gitlab-qa-tunnel'
      ELSE resource_label_value
    END AS resource_label_value
  FROM {{ ref('gcp_billing_export_resource_labels') }}
  WHERE resource_label_key = 'runner_manager_name'

),

{# unit_mapping AS (

  SELECT * FROM {{ ref('gcp_billing_unit_mapping') }}
  WHERE category = 'usage'

), #}

project_ancestory AS (

  SELECT
    *,
    MAX(IFF(folder_id = 805818759045, 1, 0)) OVER (PARTITION BY source_primary_key) AS has_project_to_exclude --gitlab-production to be excluded
  FROM {{ ref('gcp_billing_export_project_ancestry') }}

),

full_path AS (

  SELECT *
  FROM {{ ref('project_full_path') }}

),


billing_base AS (

  SELECT
    DATE(export.usage_start_time)                              AS day,
    export.project_id                                          AS project_id,
    export.service_description                                 AS service,
    export.sku_description                                     AS sku_description,
    infra_labels.resource_label_value                          AS infra_label,
    env_labels.resource_label_value                            AS env_label,
    runner_labels.resource_label_value                         AS runner_label,
    export.usage_unit                                          AS usage_unit,
    export.pricing_unit                                        AS pricing_unit,
    full_path.full_path                                        AS full_path,
    SUM(export.usage_amount)                                   AS usage_amount,
    SUM(export.usage_amount_in_pricing_units)                  AS usage_amount_in_pricing_units,
    SUM(export.cost_before_credits)                            AS cost_before_credits,
    SUM(export.cost_before_credits + coalesce(export.total_credit, 0))      AS net_cost
  FROM
    export
  LEFT JOIN
    infra_labels
    ON
      export.source_primary_key = infra_labels.source_primary_key
  LEFT JOIN
    env_labels
    ON
      export.source_primary_key = env_labels.source_primary_key
  LEFT JOIN
    runner_labels
    ON
      export.source_primary_key = runner_labels.source_primary_key
  LEFT JOIN
        full_path ON (export.project_id = full_path.gcp_project_id 
        AND DATE_TRUNC('day', export.usage_start_time) >= DATE_TRUNC('day', full_path.first_created_at) 
        AND DATE_TRUNC('day', export.usage_start_time) <= DATEADD('day', -1, DATE_TRUNC('day', full_path.last_updated_at)))
  {{ dbt_utils.group_by(n=10) }}

)

SELECT
  bill.day                           AS day,
  COALESCE(bill.project_id, 'no_id') AS gcp_project_id,
  bill.service                       AS gcp_service_description,
  bill.sku_description               AS gcp_sku_description,
  bill.infra_label                   AS infra_label,
  bill.env_label                     AS env_label,
  bill.runner_label                  AS runner_label,
  bill.usage_unit,
  bill.pricing_unit,
  full_path                AS full_path,
  bill.usage_amount                  AS usage_amount,
  bill.usage_amount_in_pricing_units AS usage_amount_in_pricing_units,
  bill.cost_before_credits           AS cost_before_credits,
  bill.net_cost                      AS net_cost,
  bill.usage_amount                  AS usage_amount_in_standard_unit
FROM billing_base AS bill

