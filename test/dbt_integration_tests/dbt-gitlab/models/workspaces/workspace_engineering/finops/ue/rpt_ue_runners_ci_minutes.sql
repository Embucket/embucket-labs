WITH cost_data AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_pl_day_ext') }}
  WHERE date_day >= '2023-02-01'

),

gitlab_data_ AS (

  SELECT * FROM {{ ref('prep_ci_runner_ratio_daily') }}

),

cloud_data AS (

  SELECT
    date_day,
    gcp_sku_description,
    level_3,
    level_4,
    CASE
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux small ') THEN '2 - shared saas runners - small'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux medium') THEN '3 - shared saas runners - medium'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux large') THEN '4 - shared saas runners - large'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux xlarge') THEN '10 - shared saas runners - xlarge'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux 2xlarge') THEN '11 - shared saas runners - 2xlarge'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux medium gpu') THEN '8 - shared saas runners gpu - medium'
      WHEN (level_3 = 'Internal' AND level_4 = 'linux private internal') THEN '6 - private internal runners'
      WHEN (level_3 = 'Shared org' AND level_4 = 'linux small ') THEN '1 - shared gitlab org runners'
    END                                     AS mapping,
    SUM(usage_amount_in_pricing_units) * 60 AS usage_amount_in_pricing_units,
    SUM(net_cost)                           AS net_cost
  FROM cost_data
  WHERE level_4 IN ('linux small ', 'linux medium', 'linux large', 'linux xlarge', 'linux 2xlarge', 'linux medium gpu', 'linux private internal')
    AND (
      gcp_sku_description LIKE 'N2D AMD Instance Core running in%'
      OR gcp_sku_description LIKE 'N1 Predefined Instance Core running%'
    )
  GROUP BY 1, 2, 3, 4
  ORDER BY 1 DESC

),

gitlab_data AS (

  SELECT
    reporting_day         AS date_day,
    mapping,
    SUM(total_ci_minutes) AS total_ci_minutes
  FROM gitlab_data_
  WHERE reporting_day >= '2023-02-01'
  GROUP BY 1, 2

),

joined AS (

  SELECT
    c.date_day,
    c.level_3,
    c.level_4,
    c.usage_amount_in_pricing_units                      AS cloud_compute_minutes,
    c.net_cost,
    g.total_ci_minutes                                   AS gitlab_ci_minutes,
    c.usage_amount_in_pricing_units - g.total_ci_minutes AS overhead_compute_minutes,
    CASE WHEN c.mapping = '2 - shared saas runners - small' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (1 / 2)) -- cost_factor / core_in_machine
      WHEN c.mapping = '3 - shared saas runners - medium' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (2 / 4)) -- cost factor = 2, 4 cores
      WHEN c.mapping = '4 - shared saas runners - large' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (3 / 8)) -- cost factor = 3, 8 cores
      WHEN c.mapping = '10 - shared saas runners - xlarge' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (6 / 16)) -- cost factor = 6, 16 cores
      WHEN c.mapping = '11 - shared saas runners - 2xlarge' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (12 / 32)) -- cost factor = 12, 32 cores
      WHEN c.mapping = '8 - shared saas runners gpu - medium' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (7 / 4)) -- cost factor = 7, 4 cores
      WHEN c.mapping = '6 - private internal runners' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (1 / 2)) -- cost factor none, 2 cores
      WHEN c.mapping = '1 - shared gitlab org runners' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (1 / 2)) -- cost factor none, 2 cores
    END                                                  AS compute_efficiency,
    c.net_cost / NULLIFZERO((g.total_ci_minutes / 1000))             AS dollar_efficency_cost_for_1000_ci_minutes
  FROM cloud_data AS c
  LEFT JOIN gitlab_data AS g
    ON c.date_day = g.date_day
      AND c.mapping = g.mapping

)

SELECT * FROM joined
ORDER BY 1 DESC
