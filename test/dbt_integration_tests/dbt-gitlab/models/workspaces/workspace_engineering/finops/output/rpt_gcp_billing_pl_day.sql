{{ config(
    materialized='table',
    )
}}


WITH pl_combined AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_pl_day_combined') }}

),

lookback_pl_mappings AS (

  SELECT * FROM {{ ref('lookback_pl_mappings') }}

),


overlaps AS (

  SELECT
    pl_combined.date_day                                                                     AS date_day,
    pl_combined.gcp_project_id                                                               AS gcp_project_id,
    pl_combined.gcp_service_description,
    pl_combined.gcp_sku_description,
    pl_combined.infra_label,
    pl_combined.env_label,
    pl_combined.runner_label,
    pl_combined.full_path,
    COALESCE(lookback_pl_mappings.pl_category, pl_combined.pl_category)                      AS pl_category,
    pl_combined.usage_unit,
    pl_combined.pricing_unit,
    pl_combined.usage_amount * COALESCE(lookback_pl_mappings.pl_percent, 1)                  AS usage_amount,
    pl_combined.usage_amount_in_pricing_units * COALESCE(lookback_pl_mappings.pl_percent, 1) AS usage_amount_in_pricing_units,
    pl_combined.cost_before_credits * COALESCE(lookback_pl_mappings.pl_percent, 1)           AS cost_before_credits,
    pl_combined.net_cost * COALESCE(lookback_pl_mappings.pl_percent, 1)                      AS net_cost,
    {# pl_combined.usage_standard_unit, #}
    {# pl_combined.usage_amount_in_standard_unit * COALESCE(lookback_pl_mappings.pl_percent, 1) AS usage_amount_in_standard_unit, #}
    COALESCE(lookback_pl_mappings.from_mapping, pl_combined.from_mapping)                    AS from_mapping,
    combined_pk,
    DENSE_RANK() OVER (PARTITION BY pl_combined.date_day,
      pl_combined.gcp_project_id,
      pl_combined.gcp_service_description,
      pl_combined.gcp_sku_description,
      pl_combined.infra_label,
      pl_combined.env_label,
      pl_combined.runner_label,
      pl_combined.full_path
      ORDER BY
        (CASE WHEN lookback_pl_mappings.full_path IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_service_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_sku_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.infra_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.env_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.runner_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_project_id IS NOT NULL THEN 1 ELSE 0 END) DESC
    )                                                                                        AS priority
  FROM
    pl_combined
  LEFT JOIN lookback_pl_mappings ON lookback_pl_mappings.date_day = pl_combined.date_day
    AND COALESCE(pl_combined.gcp_project_id, 'null') LIKE COALESCE(lookback_pl_mappings.gcp_project_id, COALESCE(pl_combined.gcp_project_id, ''))
    AND COALESCE(lookback_pl_mappings.gcp_service_description, pl_combined.gcp_service_description) = pl_combined.gcp_service_description
    AND COALESCE(lookback_pl_mappings.gcp_sku_description, pl_combined.gcp_sku_description) = pl_combined.gcp_sku_description
    AND COALESCE(lookback_pl_mappings.infra_label, COALESCE(pl_combined.infra_label, '')) = COALESCE(pl_combined.infra_label, '')
    AND COALESCE(lookback_pl_mappings.env_label, COALESCE(pl_combined.env_label, '')) = COALESCE(pl_combined.env_label, '')
    AND COALESCE(lookback_pl_mappings.runner_label, COALESCE(pl_combined.runner_label, '')) = COALESCE(pl_combined.runner_label, '')
    AND COALESCE(lookback_pl_mappings.full_path, COALESCE(pl_combined.full_path, '')) = COALESCE(pl_combined.full_path, '')

),

grouping AS (

  SELECT
    date_day,
    gcp_project_id,
    gcp_service_description,
    gcp_sku_description,
    infra_label,
    env_label,
    runner_label,
    full_path,
    pl_category,
    usage_unit,
    pricing_unit,
    SUM(usage_amount)                  AS usage_amount,
    SUM(usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
    SUM(cost_before_credits)           AS cost_before_credits,
    SUM(net_cost)                      AS net_cost,
    {# usage_standard_unit,
    usage_amount_in_standard_unit, #}
    from_mapping,
    combined_pk
  FROM overlaps
  WHERE priority = 1
  GROUP BY date_day,
    gcp_project_id,
    gcp_service_description,
    gcp_sku_description,
    infra_label,
    env_label,
    runner_label,
    full_path,
    pl_category,
    usage_unit,
    pricing_unit,
    {# usage_standard_unit,
    usage_amount_in_standard_unit, #}
    from_mapping,
    combined_pk
)


SELECT
  *,
  {{ dbt_utils.generate_surrogate_key([ 'date_day', 'gcp_project_id', 'gcp_service_description', 'gcp_sku_description', 'infra_label', 'env_label', 'runner_label', 'full_path', 'pl_category', 'from_mapping']) }} AS pl_pk
FROM grouping

