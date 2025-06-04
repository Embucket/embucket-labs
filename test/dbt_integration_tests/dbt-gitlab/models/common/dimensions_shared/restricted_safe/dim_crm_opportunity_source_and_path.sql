{{ config(
    tags=["six_hourly"]
) }}

WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = TRUE

), distinct_values AS (

    SELECT DISTINCT
      -- Source & Path information
      primary_campaign_source_id,
      generated_source,
      lead_source,
      net_new_source_categories,
      sales_qualified_source,
      sales_qualified_source_grouped,
      sales_path,
      subscription_type,
      source_buckets,
      opportunity_development_representative,
      sdr_or_bdr,
      iqm_submitted_by_role,
      sdr_pipeline_contribution

    FROM prep_crm_opportunity

),

unioned AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key(get_opportunity_source_and_path_fields()) }} AS dim_crm_opportunity_source_and_path_sk,
    *
  FROM distinct_values

  UNION ALL

  SELECT
   MD5('-1')                                          AS dim_crm_opportunity_source_path_sk,
  'Missing primary_campaign_source_id'                AS primary_campaign_source_id,
  'Missing generated_source'                          AS generated_source,
  'Missing lead_source'                               AS lead_source,
  'Missing net_new_source_categories'                 AS net_new_source_categories,
  'Missing sales_qualified_source'                    AS sales_qualified_source,
  'Missing sales_qualified_source_grouped'            AS sales_qualified_source_grouped,
  'Missing sales_path'                                AS sales_path,
  'Missing subscription_type'                         AS subscription_type,
  'Missing source_buckets'                            AS source_buckets,
  'Missing opportunity_development_representative'    AS opportunity_development_representative,
  'Missing sdr_or_bdr'                                AS sdr_or_bdr,
  'Missing iqm_submitted_by_role'                     AS iqm_submitted_by_role,
  0                                                   AS sdr_pipeline_contribution

)
SELECT *
FROM unioned