{{ config(
    tags=["six_hourly"]
) }}

WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = TRUE

), distinct_values AS (

    SELECT DISTINCT

      deal_path,
      opportunity_deal_size,
      deal_category,
      deal_group,
      deal_size,
      calculated_deal_size,
      deal_path_engagement

    FROM prep_crm_opportunity

),
unioned AS (

  SELECT 
      {{ dbt_utils.generate_surrogate_key(get_opportunity_deal_fields()) }} AS dim_crm_opportunity_deal_sk,
      *
  FROM distinct_values

  UNION ALL

  -- Missing member info
  SELECT
  MD5('-1')                                                                 AS dim_crm_opportunity_deal_sk,
  'Missing deal_path'                                                       AS deal_path,
  'Missing opportunity_deal_size'                                           AS opportunity_deal_size,
  'Missing deal_category'                                                   AS deal_category,
  'Missing deal_group'                                                      AS deal_group,
  'Missing deal_size'                                                       AS deal_size,
  'Missing calculated_deal_size'                                            AS calculated_deal_size,
  'Missing deal_path_engagement'                                            AS deal_path_engagement

)
SELECT *
FROM unioned
