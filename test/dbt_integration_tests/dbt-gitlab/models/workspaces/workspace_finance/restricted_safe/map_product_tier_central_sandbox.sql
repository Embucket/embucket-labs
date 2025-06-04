{{ config(
    tags=["mnpi"]
) }}

WITH zuora_central_sandbox_product AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_source') }}

), zuora_central_sandbox_product_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_central_sandbox_product_rate_plan_source') }}
    WHERE is_deleted = FALSE

), legacy_plans AS (

    SELECT
      zuora_central_sandbox_product_rate_plan.product_rate_plan_id                  AS product_rate_plan_id,
      zuora_central_sandbox_product_rate_plan.product_rate_plan_name                AS product_rate_plan_name,
      CASE
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%saas - ultimate%'
          THEN 'SaaS - Ultimate'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%saas - premium%'
          THEN 'SaaS - Premium'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%dedicated - ultimate%'
          THEN 'Dedicated - Ultimate'     
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%ultimate%'
          THEN 'Self-Managed - Ultimate'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%premium%'
          THEN 'Self-Managed - Premium'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gold%'
          THEN 'SaaS - Gold'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'silver%'
          THEN 'SaaS - Silver'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%bronze%'
          THEN 'SaaS - Bronze'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%starter%'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gitlab enterprise edition%'
          THEN 'Self-Managed - Starter'
        WHEN zuora_central_sandbox_product_rate_plan.product_rate_plan_name = 'Pivotal Cloud Foundry Tile for GitLab EE'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'plus%'
          THEN 'Plus'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'standard%'
          THEN 'Standard'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'basic%'
          THEN 'Basic'
        WHEN zuora_central_sandbox_product_rate_plan.product_rate_plan_name = 'Trueup'
          THEN 'Trueup'
        WHEN LTRIM(LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name)) LIKE 'githost%'
          THEN 'GitHost'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE ANY ('%quick start with ha%', '%proserv training per-seat add-on%', 'dedicated engineer%')
          THEN 'Support'
        WHEN TRIM(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) IN (
                                                                        'GitLab Service Package'
                                                                      , 'Implementation Services Quick Start'
                                                                      , 'Implementation Support'
                                                                      , 'Support Package'
                                                                      , 'Admin Training'
                                                                      , 'CI/CD Training'
                                                                      , 'GitLab Project Management Training'
                                                                      , 'GitLab with Git Basics Training'
                                                                      , 'Travel Expenses'
                                                                      , 'Training Workshop'
                                                                      , 'GitLab for Project Managers Training - Remote'
                                                                      , 'GitLab with Git Basics Training - Remote'
                                                                      , 'GitLab for System Administrators Training - Remote'
                                                                      , 'GitLab CI/CD Training - Remote'
                                                                      , 'InnerSourcing Training - Remote for your team'
                                                                      , 'GitLab DevOps Fundamentals Training'
                                                                      , 'Self-Managed Rapid Results Consulting'
                                                                      , 'Gitlab.com Rapid Results Consulting'
                                                                      , 'GitLab Security Essentials Training - Remote Delivery'
                                                                      , 'InnerSourcing Training - At your site'
                                                                      , 'Migration+'
                                                                      , 'One Time Discount'
                                                                      , 'LDAP Integration'
                                                                      , 'Dedicated Implementation Services'
                                                                      , 'Quick Start without HA, less than 500 users'
                                                                      , 'Jenkins Integration'
                                                                      , 'Hourly Consulting'
                                                                      , 'JIRA Integration'
                                                                      , 'Custom PS Education Services'
                                                                      , 'GitLab CI/CD Training - Onsite'
                                                                      , 'GitLab DevSecOps Fundamentals Training - Remote'
                                                                      , 'GitLab Agile Portfolio Management Training - Remote'
                                                                      , 'GitLab System Administration Training - Onsite'
                                                                      , 'GitLab Agile Portfolio Management Training - Onsite'
                                                                      , 'GitLab Security Essentials Training - Remote'
                                                                      , 'GitLab with Git Fundamentals Training - Onsite'
                                                                      , 'GitLab with Git Fundamentals Training - Remote'
                                                                      , 'Implementation QuickStart - GitLab.com'
                                                                      , 'GitLab Security Essentials Training - Onsite'
                                                                      , 'Implementation QuickStart - Self Managed (HA)'
                                                                     )
          THEN 'Support'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gitlab geo%'
          THEN 'Other'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'ci runner%'
          THEN 'Other'
        WHEN LOWER(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'discount%'
          THEN 'Other'
        WHEN TRIM(zuora_central_sandbox_product_rate_plan.product_rate_plan_name) IN (
                                                                        '#movingtogitlab'
                                                                      , 'File Locking'
                                                                      , 'Payment Gateway Test'
                                                                      , 'Time Tracking'
                                                                      , '1,000 CI Minutes'
                                                                      , '1,000 Compute Minutes'
                                                                      , 'Gitlab Storage 10GB'
                                                                      , 'EdCast Settlement Revenue'
                                                                     )
          THEN 'Other'
        ELSE 'Not Applicable'
      END                                                                       AS product_tier_historical,
      CASE
        WHEN LOWER(product_tier_historical) LIKE '%self-managed%'
          THEN 'Self-Managed'
        WHEN LOWER(product_tier_historical) LIKE ANY ('%saas%', 'storage', 'standard', 'basic', 'plus', 'githost', 'dedicated - ultimate%')
          THEN 'SaaS'
        WHEN product_tier_historical = 'SaaS - Other'
          THEN 'SaaS'
        WHEN product_tier_historical IN (
                                          'Other'
                                        , 'Support'
                                        , 'Trueup'
                                        )
          THEN 'Others'
        WHEN product_tier_historical = 'Not Applicable'
          THEN 'Not Applicable'
        ELSE NULL
      END                                                           AS product_delivery_type_legacy,
      zuora_central_sandbox_product.product_delivery_type           AS product_delivery_type_active,
      CASE
        WHEN LOWER(product_tier_historical) LIKE '%self-managed%'
          THEN 'Self-Managed'
        WHEN LOWER(product_tier_historical) LIKE 'dedicated - ultimate%'
          THEN 'Dedicated'
        WHEN LOWER(product_tier_historical) LIKE ANY ('%saas%', 'storage', 'standard', 'basic', 'plus', 'githost', 'saas - other')
          THEN 'GitLab.com'
        WHEN product_tier_historical IN (
                                          'Other'
                                        , 'Support'
                                        , 'Trueup'
                                        )
          THEN 'Others'
        WHEN product_tier_historical = 'Not Applicable'
          THEN 'Not Applicable'
        ELSE NULL
      END                                                           AS product_deployment_type_legacy,
      zuora_central_sandbox_product.product_deployment_type         AS product_deployment_type_active,
      CASE
        WHEN product_tier_historical IN (
                                          'SaaS - Gold'
                                        , 'Self-Managed - Ultimate'
                                        , 'SaaS - Ultimate'
                                        , 'Dedicated - Ultimate'
                                        )
          THEN 3
        WHEN product_tier_historical IN (
                                          'SaaS - Silver'
                                        , 'Self-Managed - Premium'
                                        , 'SaaS - Premium'
                                        )
          THEN 2
        WHEN product_tier_historical IN (
                                          'SaaS - Bronze'
                                        , 'Self-Managed - Starter'
                                        )
          THEN 1
        ELSE 0
      END                                                                       AS product_ranking,
      CASE
        WHEN product_tier_historical = 'SaaS - Gold'
          THEN 'SaaS - Ultimate'
        WHEN product_tier_historical = 'SaaS - Silver'
          THEN 'SaaS - Premium'
        ELSE product_tier_historical
      END                                                                       AS product_tier_legacy,
      zuora_central_sandbox_product.product_tier                                AS product_tier_active,
      zuora_central_sandbox_product.category                                    AS product_category,
      zuora_central_sandbox_product_rate_plan.effective_start_date              AS effective_start_date,
      zuora_central_sandbox_product_rate_plan.effective_end_date                AS effective_end_date    
    FROM zuora_central_sandbox_product
    INNER JOIN zuora_central_sandbox_product_rate_plan
      ON zuora_central_sandbox_product.product_id = zuora_central_sandbox_product_rate_plan.product_id

), final AS (

    SELECT DISTINCT
      product_rate_plan_id,
      product_rate_plan_name,
      product_tier_historical,
      CASE 
        WHEN product_tier_historical LIKE ANY (
                                                '%Self-Managed - Starter%',
                                                '%Self-Managed - Premium%',
                                                '%Self-Managed - Ultimate%',
                                                '%SaaS - Ultimate%',
                                                '%SaaS - Premium%',
                                                '%Dedicated - Ultimate%'
                                              )
          THEN product_tier_legacy
        WHEN product_tier_active LIKE ANY (
                                            '%Legacy%',
                                            NULL,
                                            '',
                                            '%None%'
                                          ) 
          THEN product_tier_legacy
        WHEN effective_end_date < CURRENT_TIMESTAMP 
          THEN product_tier_legacy
        ELSE product_tier_active 
      END AS product_tier,

      CASE 
        WHEN product_tier_historical LIKE ANY (
                                                '%Self-Managed - Starter%',
                                                '%Self-Managed - Premium%',
                                                '%Self-Managed - Ultimate%',
                                                '%Other%',
                                                '%Support%'
                                              )
        THEN product_delivery_type_legacy
        WHEN product_tier_active LIKE ANY (
                                            '%Legacy%',
                                            NULL,
                                            ''
                                          ) 
          THEN product_delivery_type_legacy
        WHEN product_tier_historical LIKE '%Not Applicable%' AND  product_delivery_type_active LIKE '%None%' 
          THEN product_delivery_type_legacy
        WHEN effective_end_date < CURRENT_TIMESTAMP 
          THEN product_delivery_type_legacy
        ELSE product_delivery_type_active 
      END AS product_delivery_type,

      CASE 
        WHEN product_tier_historical LIKE ANY (
                                                '%Self-Managed - Starter%',
                                                '%Self-Managed - Premium%',
                                                '%Self-Managed - Ultimate%',
                                                '%Other%',
                                                '%Support%'
                                              )
        THEN product_deployment_type_legacy
        WHEN product_tier_active LIKE ANY (
                                            '%Legacy%',
                                            NULL,
                                            ''
                                          ) 
          THEN product_deployment_type_legacy
        WHEN product_tier_historical LIKE '%Not Applicable%' AND  product_deployment_type_active LIKE '%None%' 
          THEN product_deployment_type_legacy
        WHEN effective_end_date < CURRENT_TIMESTAMP 
          THEN product_deployment_type_legacy
        ELSE product_deployment_type_active 
      END AS product_deployment_type,
      product_category,
      product_ranking,
      effective_start_date,
      effective_end_date
   FROM legacy_plans
)



{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@snalamaru",
    created_date="2022-03-31",
    updated_date="2024-04-10"
) }}