WITH product_tier_mapping AS (

    SELECT *
    FROM {{ ref('map_product_tier_api_sandbox') }}

), mapping AS (

    SELECT DISTINCT 
      product_tier_historical,
      product_tier,
      product_delivery_type,
      product_deployment_type,
      product_ranking        
    FROM product_tier_mapping
    
    UNION ALL
    
    SELECT
      'SaaS - Free'                                                 AS product_tier_historical,
      'SaaS - Free'                                                 AS product_tier,
      'SaaS'                                                        AS product_delivery_type,
      'GitLab.com'                                                  AS product_deployment_type,
      0                                                             AS product_ranking
    
    UNION ALL
    
    SELECT
      'Self-Managed - Free'                                         AS product_tier_historical,
      'Self-Managed - Free'                                         AS product_tier,
      'Self-Managed'                                                AS product_delivery_type,
      'Self-Managed'                                                AS product_deployment_type,
      0                                                             AS product_ranking
  
    UNION ALL
    
    SELECT
      'SaaS - Trial: Gold'                                          AS product_tier_historical,
      'SaaS - Trial: Ultimate'                                      AS product_tier,
      'SaaS'                                                        AS product_delivery_type,
      'GitLab.com'                                                  AS product_deployment_type,
      0                                                             AS product_ranking
  
    UNION ALL
    
    SELECT
      'Self-Managed - Trial: Ultimate'                              AS product_tier_historical,
      'Self-Managed - Trial: Ultimate'                              AS product_tier,
      'Self-Managed'                                                AS product_delivery_type,
      'Self-Managed'                                                AS product_deployment_type,
      0                                                             AS product_ranking

), final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['product_tier_historical']) }}      AS dim_product_tier_id,
    product_tier_historical,
    SPLIT_PART(product_tier_historical, ' - ', -1)                  AS product_tier_historical_short,
    product_tier                                                    AS product_tier_name,
    SPLIT_PART(product_tier, ' - ', -1)                             AS product_tier_name_short,
    product_delivery_type,
    product_deployment_type,
    product_ranking
  FROM mapping
  
  UNION ALL
  
  SELECT
    MD5('-1')                                                       AS dim_product_tier_id,
    '(Unknown Historical Tier)'                                     AS product_tier_historical,
    '(Unknown Historical Tier Name)'                                AS product_tier_historical_short,
    '(Unknown Tier)'                                                AS product_tier_name,
    '(Unknown Tier Name)'                                           AS product_tier_name_short,
    '(Unknown Delivery Type)'                                       AS product_delivery_type,
    '(Unknown Deployment Type)'                                     AS product_deployment_type,
    -1                                                              AS product_ranking

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@utkarsh060",
    created_date="2021-08-26",
    updated_date="2024-05-10"
) }}