{{ config(
    tags=["mnpi"]
) }}

WITH zuora_api_sandbox_product AS (

    SELECT *
    FROM {{ ref('zuora_api_sandbox_product_source') }}

), zuora_api_sandbox_product_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_api_sandbox_product_rate_plan_source') }}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      zuora_api_sandbox_product_rate_plan.product_rate_plan_id                  AS product_rate_plan_id,
      zuora_api_sandbox_product_rate_plan.product_rate_plan_name                AS product_rate_plan_name,
      CASE
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%saas - ultimate%'
          THEN 'SaaS - Ultimate'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%saas - premium%'
          THEN 'SaaS - Premium'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%self-managed - ultimate%'
          THEN 'Self-Managed - Ultimate'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%dedicated - ultimate%'
          THEN 'Dedicated - Ultimate'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%premium%'
          THEN 'Self-Managed - Premium'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gold%'
          THEN 'SaaS - Gold'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'silver%'
          THEN 'SaaS - Silver'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%bronze%'
          THEN 'SaaS - Bronze'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE '%starter%'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gitlab enterprise edition%'
          THEN 'Self-Managed - Starter'
        WHEN zuora_api_sandbox_product_rate_plan.product_rate_plan_name = 'Pivotal Cloud Foundry Tile for GitLab EE'
          THEN 'Self-Managed - Starter'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'plus%'
          THEN 'Plus'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'standard%'
          THEN 'Standard'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'basic%'
          THEN 'Basic'
        WHEN zuora_api_sandbox_product_rate_plan.product_rate_plan_name = 'Trueup'
          THEN 'Trueup'
        WHEN LTRIM(LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name)) LIKE 'githost%'
          THEN 'GitHost'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE ANY ('%quick start with ha%', '%proserv training per-seat add-on%', 'dedicated engineer%')
          THEN 'Support'
        WHEN TRIM(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) IN (
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
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'gitlab geo%'
          THEN 'Other'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'ci runner%'
          THEN 'Other'
        WHEN LOWER(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) LIKE 'discount%'
          THEN 'Other'
        WHEN TRIM(zuora_api_sandbox_product_rate_plan.product_rate_plan_name) IN (
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
      END                                                                       AS product_delivery_type,
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
      END                                                                     AS product_deployment_type,
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
      END                                                                       AS product_tier
    FROM zuora_api_sandbox_product
    INNER JOIN zuora_api_sandbox_product_rate_plan
      ON zuora_api_sandbox_product.product_id = zuora_api_sandbox_product_rate_plan.product_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@jpeguero",
    created_date="2021-08-26",
    updated_date="2023-05-26"
) }}