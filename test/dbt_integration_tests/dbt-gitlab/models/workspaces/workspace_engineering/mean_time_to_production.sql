WITH dates AS (

  SELECT *
  FROM {{ ref('dim_date') }}
  WHERE DATE_TRUNC('month', date_actual) <= DATE_TRUNC('month', CURRENT_DATE)
    AND DATE_TRUNC('month', date_actual) >= DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE))

),

merge_requests AS (

  SELECT *
  FROM {{ ref('dim_merge_request') }}
  WHERE merged_at >= '2020-01-01'

),

deployment_merge_requests AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_deployment_merge_requests') }}

),

deployments AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_deployments') }}
  WHERE deployment_iid != 147571 -- Skip deployment from the master branch as it is a NOOP

),

environments AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_environments_xf') }}
  WHERE environment_id = 1178942 AND project_id = 278964
-- LOWER(environments.ENVIRONMENT_NAME) = 'gprd' note: ENVIRONMENT_NAME has been deprecated. Referencing env id instead.
-- 2009901 is gitaly, 278964 is gitlab.com

),

base AS (

  SELECT
    merge_requests.merge_request_internal_id              AS mr_iid,
    merge_requests.merge_request_title                    AS mr_title,
    merge_requests.created_at                             AS mr_created_at,
    merge_requests.merged_at                              AS mr_merged_at,
    deployments.created_at                                AS mr_deployed_at,
    deployments.deployment_id,
    environments.environment_name,
    environments.project_id,
    CASE
      WHEN environments.project_id = 278964 THEN 'GitLab Rails'
      WHEN environments.project_id = 2009901 THEN 'Gitaly'
    END                                                   AS application,
    CASE
      WHEN environments.environment_name = 'gprd' THEN 'Production'
      WHEN environments.environment_name = 'gstg' THEN 'Staging'
      WHEN environments.environment_name = 'gprd-cny' THEN 'Canary'
    END                                                   AS environment,
    DATEDIFF(SECOND, mr_merged_at, mr_deployed_at) / 3600 AS merge_to_deploy_time_in_hours,
    DATEDIFF(SECOND, mr_created_at, mr_merged_at) / 3600  AS create_to_merge_time_in_hours
  FROM merge_requests
  INNER JOIN deployment_merge_requests
    ON merge_requests.merge_request_id = deployment_merge_requests.merge_request_id
  INNER JOIN deployments
    ON deployment_merge_requests.deployment_id = deployments.deployment_id
  INNER JOIN environments
    ON deployments.environment_id = environments.environment_id

),

grouped AS (

  SELECT
    DATE_TRUNC('month', base.mr_deployed_at) AS deploy_date,
    'monthly'                                AS date_aggregation,
    base.application,
    base.environment,
    AVG(base.merge_to_deploy_time_in_hours)  AS mttp_in_hours,
    AVG(base.create_to_merge_time_in_hours)  AS mttm_in_hours,
    COUNT(DISTINCT base.mr_iid)              AS mr_batch_size,
    COUNT(DISTINCT base.deployment_id)       AS deployments
  FROM base
  GROUP BY 1, 3, 4
  UNION ALL
  SELECT
    DATE_TRUNC('day', base.mr_deployed_at)  AS deploy_date,
    'daily'                                 AS date_aggregation,
    base.application,
    base.environment,
    AVG(base.merge_to_deploy_time_in_hours) AS mttp_in_hours,
    AVG(base.create_to_merge_time_in_hours) AS mttm_in_hours,
    COUNT(DISTINCT base.mr_iid)             AS mr_batch_size,
    COUNT(DISTINCT base.deployment_id)      AS deployments
  FROM base
  GROUP BY 1, 3, 4

)

SELECT DISTINCT
  DATE_TRUNC('month', dates.date_actual) AS date_actual,
  COALESCE(date_aggregation, 'monthly')  AS date_aggregation,
  application,
  environment,
  deployments,
  mttp_in_hours,
  mttm_in_hours,
  mr_batch_size
FROM dates
LEFT JOIN grouped
  ON grouped.date_aggregation = 'monthly'
    AND DATE_TRUNC('month', dates.date_actual) = grouped.deploy_date
UNION ALL
SELECT
  DATE_TRUNC('day', dates.date_actual) AS date_actual,
  COALESCE(date_aggregation, 'daily')  AS date_aggregation,
  application,
  environment,
  deployments,
  mttp_in_hours,
  mttm_in_hours,
  mr_batch_size
FROM dates
LEFT JOIN grouped ON grouped.date_aggregation = 'daily'
  AND DATE_TRUNC('day', dates.date_actual) = grouped.deploy_date
