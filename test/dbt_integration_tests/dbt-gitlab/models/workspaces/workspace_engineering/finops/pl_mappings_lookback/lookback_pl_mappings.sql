WITH flex_cud AS (

  SELECT
    date_day                                        AS date_day,
    'gitlab-production'                             AS gcp_project_id,
    NULL                                            AS gcp_service_description,
    'Commitment - dollar based v1: GCE for 3 years' AS gcp_sku_description,
    'shared'                                        AS infra_label,
    NULL                                            AS env_label,
    NULL                                            AS runner_label,
    NULL                                            AS full_path,
    LOWER(flex_cud_lookback.pl_category)            AS pl_category,
    flex_cud_lookback.pl_percent                    AS pl_percent,
    'flex_cud_lookback'                             AS from_mapping
  FROM {{ ref ('flex_cud_lookback') }}

),

t2d_cud AS (
  WITH sku_list AS (
    SELECT 'Commitment v1: T2D AMD Cpu in Americas for 3 Year' AS sku
    UNION ALL
    SELECT 'Commitment v1: T2D AMD Ram in Americas for 3 Year'
  )

  SELECT
    date_day                            AS date_day,
    t2d_cud_lookback.gcp_project_id     AS gcp_project_id,
    NULL                                AS gcp_service_description,
    sku_list.sku                        AS gcp_sku_description,
    'shared'                            AS infra_label,
    NULL                                AS env_label,
    NULL                                AS runner_label,
    NULL                                AS full_path,
    LOWER(t2d_cud_lookback.pl_category) AS pl_category,
    t2d_cud_lookback.pl_percent         AS pl_percent,
    't2d_cud_lookback'                  AS from_mapping
  FROM {{ ref ('t2d_cud_lookback') }}
  CROSS JOIN sku_list

),


cte_append AS (
  SELECT *
  FROM flex_cud
  UNION ALL
  SELECT * 
  FROM t2d_cud
)

SELECT
  date_day,
  gcp_project_id,
  gcp_service_description,
  gcp_sku_description,
  infra_label,
  env_label,
  runner_label,
  full_path,
  LOWER(pl_category)           AS pl_category,
  pl_percent,
  LISTAGG(DISTINCT from_mapping, ' || ') WITHIN GROUP (
    ORDER BY from_mapping ASC) AS from_mapping
FROM cte_append
{{ dbt_utils.group_by(n=10) }}
