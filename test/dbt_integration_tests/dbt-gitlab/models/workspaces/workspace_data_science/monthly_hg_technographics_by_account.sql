{{
  config(
    materialized = 'incremental',
    unique_key = ['crm_account_id', 'snapshot_month'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'fail'
  )
}}

WITH account_months AS (

    SELECT
        DATE_TRUNC('MONTH', b.date_actual) AS snapshot_month
        , a.crm_account_id
        , a.hg_parent_category
        , a.hg_category
        , a.hg_vendor
        , a.hg_product
        , a.hg_intensity
        , a.dbt_valid_from
        , a.dbt_valid_to
    FROM {{ ref('sfdc_hg_insights_technographics_source') }} a
    INNER JOIN {{ ref('dim_date') }} b
        ON a.dbt_valid_from <= b.date_actual
        AND (a.dbt_valid_to > b.date_actual OR a.dbt_valid_to IS NULL) 
    WHERE b.date_actual BETWEEN '2024-12-01' AND CURRENT_DATE    
        AND a.crm_account_id IS NOT NULL
        AND is_deleted = False
        --AND a.crm_account_id = '0018X00002zVecQQAS'
        --AND a.hg_product = 'Jenkins'
        AND snapshot_month >= DATE_TRUNC('MONTH', a.dbt_valid_from) 
        AND (
                snapshot_month <= DATE_TRUNC('MONTH', a.dbt_valid_to) 
                OR 
                a.dbt_valid_to IS NULL
            )  -- Only accept rows that were valid during the snapshot month

        {% if is_incremental() %}
        -- When running incrementally, only process account x snapshot_date with changes since last run
        AND (
            -- New or modified records since last run
            a.dbt_valid_from > (SELECT MAX(dbt_updated_at) FROM {{ this }})
            OR
            (a.dbt_valid_to IS NOT NULL AND a.dbt_valid_to >= (SELECT MAX(dbt_updated_at) FROM {{ this }}))
        )
        {% endif %}

{% if is_incremental() %}
), accounts_to_process AS (
    SELECT DISTINCT 
        crm_account_id
        , snapshot_month
    FROM account_months
{% endif %}

),  dedup_account_months AS (

-- When multiple product_id records exist in the same month for an account, take the latest one

    SELECT a.*
    FROM account_months a
    {% if is_incremental() %}
    INNER JOIN accounts_to_process p 
        ON a.crm_account_id = p.crm_account_id 
        AND a.snapshot_month = p.snapshot_month
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.crm_account_id, a.snapshot_month, a.hg_product
                                ORDER BY 
                                    CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END ASC, -- Prioritize null values first
                                    dbt_valid_to DESC, -- break ties with valid_to then valid_from 
                                    dbt_valid_from DESC
                                ) = 1

                                
), top_vendors AS (

    WITH top_vendors_agg AS (
    
        SELECT 
            crm_account_id
            , snapshot_month
            , hg_parent_category
            , hg_category
            , NTH_VALUE(hg_vendor, 1) OVER (PARTITION BY crm_account_id, snapshot_month ORDER BY hg_intensity DESC) AS first_top_vendor
            , NTH_VALUE(hg_intensity, 1) OVER (PARTITION BY crm_account_id, snapshot_month ORDER BY hg_intensity DESC) AS first_top_vendor_intensity
            , NTH_VALUE(hg_vendor, 2) OVER (PARTITION BY crm_account_id, snapshot_month ORDER BY hg_intensity DESC) AS second_top_vendor
            , NTH_VALUE(hg_intensity, 2) OVER (PARTITION BY crm_account_id, snapshot_month ORDER BY hg_intensity DESC) AS second_top_vendor_intensity
            , NTH_VALUE(hg_vendor, 3) OVER (PARTITION BY crm_account_id, snapshot_month ORDER BY hg_intensity DESC) AS third_top_vendor
            , NTH_VALUE(hg_intensity, 3) OVER (PARTITION BY crm_account_id, snapshot_month ORDER BY hg_intensity DESC) AS third_top_vendor_intensity
            , FIRST_VALUE(hg_vendor) OVER (PARTITION BY crm_account_id, snapshot_month, hg_parent_category ORDER BY hg_intensity DESC) AS top_vendor_by_pcategory
            , FIRST_VALUE(hg_vendor) OVER (PARTITION BY crm_account_id, snapshot_month, hg_category ORDER BY hg_intensity DESC) AS top_vendor_by_category
        FROM dedup_account_months
        QUALIFY ROW_NUMBER() OVER (PARTITION BY crm_account_id, snapshot_month, hg_parent_category, hg_category ORDER BY crm_account_id, snapshot_month) = 1
        ORDER BY crm_account_id, snapshot_month

    )

    SELECT 
        crm_account_id
        , snapshot_month
        -- Top Vendors
        , MAX(first_top_vendor) AS hg_first_top_vendor
        , MAX(first_top_vendor_intensity) AS hg_first_top_vendor_intensity
        , MAX(second_top_vendor) AS hg_second_top_vendor
        , MAX(second_top_vendor_intensity) AS hg_second_top_vendor_intensity
        , MAX(third_top_vendor) AS hg_third_top_vendor
        , MAX(third_top_vendor_intensity) AS hg_third_top_vendor_intensity
    
        -- Top Vendor in each Parent Category
        , MAX(CASE WHEN hg_parent_category = 'Cloud Services' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_cloud_services_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_lcm_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Application Development Software' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_dev_software_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Operations Management' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_op_management_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Application Infrastructure Middleware' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_app_infra_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Enterprise Resource Planning Applications' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_resource_plan_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Database Management System' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_dbm_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Enterprise Applications' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_ent_apps_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Operating Systems' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_os_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Virtualisation' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_virtual_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Collaboration' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_collab_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Information Management' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_im_top_vendor
        , MAX(CASE WHEN hg_parent_category = 'Security' THEN top_vendor_by_pcategory END) AS hg_tech_pcategory_security_top_vendor
    
        -- Top Vendor in each Category
        , MAX(CASE WHEN hg_category = 'Development Languages, Environment and Tools' THEN top_vendor_by_category END) AS hg_tech_category_dev_tools_top_vendor
        , MAX(CASE WHEN hg_category = 'Modeling and Simulation Tools' THEN top_vendor_by_category END) AS hg_tech_category_modeling_tools_top_vendor
        , MAX(CASE WHEN hg_category = 'Software Components' THEN top_vendor_by_category END) AS hg_tech_category_software_top_vendor
        , MAX(CASE WHEN hg_category = 'Application Middleware' THEN top_vendor_by_category END) AS hg_tech_category_app_middleware_top_vendor
        , MAX(CASE WHEN hg_category = 'Business-to-Business Middleware' THEN top_vendor_by_category END) AS hg_tech_category_b2b_middleware_top_vendor
        , MAX(CASE WHEN hg_category = 'Event-Driven Middleware' THEN top_vendor_by_category END) AS hg_tech_category_event_middleware_top_vendor
        , MAX(CASE WHEN hg_category = 'Integration Middleware' THEN top_vendor_by_category END) AS hg_tech_category_integration_middleware_top_vendor
        , MAX(CASE WHEN hg_category = 'Automated Testing and Quality Management' THEN top_vendor_by_category END) AS hg_tech_category_testing_top_vendor
        , MAX(CASE WHEN hg_category = 'Software Change, Configuration, and Process Management' THEN top_vendor_by_category END) AS hg_tech_category_process_management_top_vendor
        , MAX(CASE WHEN hg_category = 'Infrastructure-as-a-Service (IaaS)' THEN top_vendor_by_category END) AS hg_tech_category_iaas_top_vendor
        , MAX(CASE WHEN hg_category = 'Platform-as-a-Service (PaaS)' THEN top_vendor_by_category END) AS hg_tech_category_paas_top_vendor
        , MAX(CASE WHEN hg_category = 'Linux' THEN top_vendor_by_category END) AS hg_tech_category_linux_top_vendor
        , MAX(CASE WHEN hg_category = 'Configuration Management' THEN top_vendor_by_category END) AS hg_tech_category_config_management_top_vendor
        , MAX(CASE WHEN hg_category = 'IT Asset Management' THEN top_vendor_by_category END) AS hg_tech_category_asset_management_top_vendor
        , MAX(CASE WHEN hg_category = 'IT Service Desk' THEN top_vendor_by_category END) AS hg_tech_category_service_desk_top_vendor
        , MAX(CASE WHEN hg_category = 'Network Management' THEN top_vendor_by_category END) AS hg_tech_category_network_management_top_vendor
        , MAX(CASE WHEN hg_category = 'Identity and Access Management' THEN top_vendor_by_category END) AS hg_tech_category_iam_top_vendor
        , MAX(CASE WHEN hg_category = 'Information and Threat Management' THEN top_vendor_by_category END) AS hg_tech_category_threat_managment_top_vendor
        , MAX(CASE WHEN hg_category = 'Vulnerability Management' THEN top_vendor_by_category END) AS hg_tech_category_vulnerability_management_top_vendor
        FROM top_vendors_agg
        GROUP BY crm_account_id, snapshot_month

)

SELECT    a.crm_account_id
        , a.snapshot_month
        -- Totals
        , COUNT(DISTINCT hg_parent_category) AS hg_tech_parent_categories_cnt
        , COUNT(DISTINCT hg_category) AS hg_tech_categories_cnt
        , COUNT(DISTINCT hg_vendor) AS hg_tech_vendors_cnt
        , COUNT(DISTINCT hg_product) AS hg_tech_products_cnt

        , AVG(hg_intensity) AS mean_intensity
        , MEDIAN(hg_intensity) AS median_intensity
    
    
        -- Number of Products in Parent Product Categories
        , COUNT(CASE WHEN hg_parent_category = 'Cloud Services' THEN 1 END) AS hg_tech_pcategory_cloud_services_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Application Life Cycle Management' THEN 1 END) AS hg_tech_pcategory_lcm_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Application Development Software' THEN 1 END) AS hg_tech_pcategory_dev_software_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Operations Management' THEN 1 END) AS hg_tech_pcategory_op_management_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Application Infrastructure Middleware' THEN 1 END) AS hg_tech_pcategory_app_infra_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Enterprise Resource Planning Applications' THEN 1 END) AS hg_tech_pcategory_resource_plan_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Database Management System' THEN 1 END) AS hg_tech_pcategory_dbm_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Enterprise Applications' THEN 1 END) AS hg_tech_pcategory_ent_apps_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Operating Systems' THEN 1 END) AS hg_tech_pcategory_os_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Virtualisation' THEN 1 END) AS hg_tech_pcategory_virtual_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Collaboration' THEN 1 END) AS hg_tech_pcategory_collab_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Information Management' THEN 1 END) AS hg_tech_pcategory_im_cnt
        , COUNT(CASE WHEN hg_parent_category = 'Security' THEN 1 END) AS hg_tech_pcategory_security_cnt
    
        -- Number of Products in Categories
        , COUNT(CASE WHEN hg_category = 'Development Languages, Environment and Tools' THEN 1 END) AS hg_tech_category_dev_tools_cnt
        , COUNT(CASE WHEN hg_category = 'Modeling and Simulation Tools' THEN 1 END) AS hg_tech_category_modeling_tools_cnt
        , COUNT(CASE WHEN hg_category = 'Software Components' THEN 1 END) AS hg_tech_category_software_cnt
        , COUNT(CASE WHEN hg_category = 'Application Middleware' THEN 1 END) AS hg_tech_category_app_middleware_cnt
        , COUNT(CASE WHEN hg_category = 'Business-to-Business Middleware' THEN 1 END) AS hg_tech_category_b2b_middleware_cnt
        , COUNT(CASE WHEN hg_category = 'Event-Driven Middleware' THEN 1 END) AS hg_tech_category_event_middleware_cnt
        , COUNT(CASE WHEN hg_category = 'Integration Middleware' THEN 1 END) AS hg_tech_category_integration_middleware_cnt
        , COUNT(CASE WHEN hg_category = 'Automated Testing and Quality Management' THEN 1 END) AS hg_tech_category_testing_cnt
        , COUNT(CASE WHEN hg_category = 'Software Change, Configuration, and Process Management' THEN 1 END) AS hg_tech_category_process_management_cnt
        , COUNT(CASE WHEN hg_category = 'Infrastructure-as-a-Service (IaaS)' THEN 1 END) AS hg_tech_category_iaas_cnt
        , COUNT(CASE WHEN hg_category = 'Platform-as-a-Service (PaaS)' THEN 1 END) AS hg_tech_category_paas_cnt
        , COUNT(CASE WHEN hg_category = 'Linux' THEN 1 END) AS hg_tech_category_linux_cnt
        , COUNT(CASE WHEN hg_category = 'Configuration Management' THEN 1 END) AS hg_tech_category_config_management_cnt
        , COUNT(CASE WHEN hg_category = 'IT Asset Management' THEN 1 END) AS hg_tech_category_asset_management_cnt
        , COUNT(CASE WHEN hg_category = 'IT Service Desk' THEN 1 END) AS hg_tech_category_service_desk_cnt
        , COUNT(CASE WHEN hg_category = 'Network Management' THEN 1 END) AS hg_tech_category_network_management_cnt
        , COUNT(CASE WHEN hg_category = 'Identity and Access Management' THEN 1 END) AS hg_tech_category_iam_cnt
        , COUNT(CASE WHEN hg_category = 'Information and Threat Management' THEN 1 END) AS hg_tech_category_threat_managment_cnt
        , COUNT(CASE WHEN hg_category = 'Vulnerability Management' THEN 1 END) AS hg_tech_category_vulnerability_management_cnt
    
        -- Vendors
        , COUNT(CASE WHEN hg_vendor = 'JFrog, Ltd.' THEN 1 END) AS hg_vendor_jfrog_cnt
        , COUNT(CASE WHEN hg_vendor = 'Harness, Inc.' THEN 1 END) AS hg_vendor_harness_cnt
        , COUNT(CASE WHEN hg_vendor = 'Google Cloud' THEN 1 END) AS hg_vendor_gcp_cnt
        , COUNT(CASE WHEN hg_vendor = 'Snyk, Ltd.' THEN 1 END) AS hg_vendor_snyk_cnt
        , COUNT(CASE WHEN hg_vendor = 'CloudBees, Inc.' THEN 1 END) AS hg_vendor_cloudbees_cnt
        , COUNT(CASE WHEN hg_vendor = 'Bitrise, Ltd.' THEN 1 END) AS hg_vendor_bitrise_cnt
        , COUNT(CASE WHEN hg_vendor = 'Microsoft Corporation' THEN 1 END) AS hg_vendor_microsoft_cnt
        , COUNT(CASE WHEN hg_vendor = 'Red Hat, Inc.' THEN 1 END) AS hg_vendor_redhat_cnt
        , COUNT(CASE WHEN hg_vendor = 'VMware, Inc.' THEN 1 END) AS hg_vendor_vmware_cnt
        , COUNT(CASE WHEN hg_vendor = 'Mirantis, Inc.' THEN 1 END) AS hg_vendor_mirantis_cnt
        , COUNT(CASE WHEN hg_vendor = 'Atlassian Pty Ltd' THEN 1 END) AS hg_vendor_atlassian_cnt
        , COUNT(CASE WHEN hg_vendor = 'GitHub, Inc.' THEN 1 END) AS hg_vendor_github_cnt
        , COUNT(CASE WHEN hg_vendor = 'Jetbrains s.r.o.' THEN 1 END) AS hg_vendor_jetbrains_cnt
        , COUNT(CASE WHEN hg_vendor = 'The Linux Foundation' THEN 1 END) AS hg_vendor_linux_cnt
        , COUNT(CASE WHEN hg_vendor = 'Circle Internet Services, Inc.' THEN 1 END) AS hg_vendor_circleci_cnt
        , COUNT(CASE WHEN hg_vendor = 'Amazon Web Services, Inc.' THEN 1 END) AS hg_vendor_aws_cnt
        , COUNT(CASE WHEN hg_vendor = 'Sentry' THEN 1 END) AS hg_vendor_sentry_cnt
    
        -- Number of Vendors in Parent Categories
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Cloud Services' THEN hg_vendor END) AS hg_tech_pcategory_cloud_services_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Application Life Cycle Management' THEN hg_vendor END) AS hg_tech_pcategory_lcm_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Application Development Software' THEN hg_vendor END) AS hg_tech_pcategory_dev_software_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Operations Management' THEN hg_vendor END) AS hg_tech_pcategory_op_management_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Application Infrastructure Middleware' THEN hg_vendor END) AS hg_tech_pcategory_app_infra_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Enterprise Resource Planning Applications' THEN hg_vendor END) AS hg_tech_pcategory_resource_plan_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Database Management System' THEN hg_vendor END) AS hg_tech_pcategory_dbm_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Enterprise Applications' THEN hg_vendor END) AS hg_tech_pcategory_ent_apps_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Operating Systems' THEN hg_vendor END) AS hg_tech_pcategory_os_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Virtualisation' THEN hg_vendor END) AS hg_tech_pcategory_virtual_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Collaboration' THEN hg_vendor END) AS hg_tech_pcategory_collab_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Information Management' THEN hg_vendor END) AS hg_tech_pcategory_im_vendor_cnt
        , COUNT(DISTINCT CASE WHEN hg_parent_category = 'Security' THEN 1 END) AS hg_tech_pcategory_security_vendor_cnt
    
        -- Use of DevOps and Point Solution Competitors from Joe Kempton
        -- Only lists the categories they compete it
    
        , MAX(CASE WHEN hg_parent_category = 'Application Development Software' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_dev_software_github_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_lcm_github_flag
        , MAX(CASE WHEN hg_parent_category = 'Collaboration' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_collab_github_flag
        , MAX(CASE WHEN hg_parent_category = 'Security' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_security_github_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' AND hg_vendor = 'Microsoft Corporation' THEN 1 ELSE 0 END) AS hg_tech_uses_lcm_microsoft_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' AND hg_vendor = 'Harness, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_lcm_harness_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Development Software' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_dev_software_atlassian_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_lcm_atlassian_flag
        , MAX(CASE WHEN hg_parent_category = 'Enterprise Resource Planning Applications' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_resource_planning_atlassian_flag
        , MAX(CASE WHEN hg_parent_category = 'Operations Management' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_op_management_atlassian_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_lcm_jfrog_flag
        , MAX(CASE WHEN hg_parent_category = 'Security' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_security_jfrog_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Development Software' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_dev_software_jfrog_flag
        , MAX(CASE WHEN hg_parent_category = 'Application Life Cycle Management' AND hg_vendor = 'Snyk, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_lcm_synk_flag
        , MAX(CASE WHEN hg_parent_category = 'Security' AND hg_vendor = 'Snyk, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_security_synk_flag
      
        , MAX(CASE WHEN hg_category = 'Development Languages, Environment and Tools' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_dev_tools_github_flag
        , MAX(CASE WHEN hg_category = 'Automated Testing and Quality Management' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_testing_github_flag
        , MAX(CASE WHEN hg_category = 'Software Change, Configuration, and Process Management' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_process_management_github_flag
        , MAX(CASE WHEN hg_category = 'Vulnerability Management' AND hg_vendor = 'GitHub, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_vulnerability_management_github_flag
        , MAX(CASE WHEN hg_category = 'Software Change, Configuration, and Process Management' AND hg_vendor = 'Microsoft Corporation' THEN 1 ELSE 0 END) AS hg_tech_uses_process_management_microsoft_flag
        , MAX(CASE WHEN hg_category = 'Automated Testing and Quality Management' AND hg_vendor = 'Harness, Inc.' THEN 1 ELSE 0 END) AS hg_tech_uses_testing_harness_flag
        , MAX(CASE WHEN hg_category = 'Development Languages, Environment and Tools' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_dev_tools_atlassian_flag
        , MAX(CASE WHEN hg_category = 'Software Change, Configuration, and Process Management' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_process_management_atlassian_flag
        , MAX(CASE WHEN hg_category = 'Project and Portfolio Management' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_management_atlassian_flag
        , MAX(CASE WHEN hg_category = 'IT Asset Management' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_asset_management_atlassian_flag
        , MAX(CASE WHEN hg_category = 'IT Service Desk' AND hg_vendor = 'Atlassian Pty Ltd' THEN 1 ELSE 0 END) AS hg_tech_uses_service_desk_atlassian_flag
        , MAX(CASE WHEN hg_category = 'Automated Testing and Quality Management' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_testing_jfrog_flag
        , MAX(CASE WHEN hg_category = 'Software Change, Configuration, and Process Management' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_process_management_jfrog_flag
        , MAX(CASE WHEN hg_category = 'Vulnerability Management' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_vulnerability_management_jfrog_flag
        , MAX(CASE WHEN hg_category = 'Development Languages, Environment and Tools' AND hg_vendor = 'JFrog, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_dev_tools_jfrog_flag
        , MAX(CASE WHEN hg_category = 'Automated Testing and Quality Management' AND hg_vendor = 'Snyk, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_uses_testing_snyk_flag
        , MAX(CASE WHEN hg_category = 'Vulnerability Management' AND hg_vendor = 'Snyk, Ltd.' THEN 1 ELSE 0 END) AS hg_tech_vulnerability_management_synk_flag
    
        -- Top Products
        , MAX(CASE WHEN hg_product = 'Amazon Web Services (AWS)' THEN 1 END) AS hg_product_aws_flag
        , MAX(CASE WHEN hg_product = 'Amazon Web Hosting' THEN 1 END) AS hg_product_aws_hosting_flag
        , MAX(CASE WHEN hg_product = 'Amazon EC2' THEN 1 END) AS hg_product_ec2_flag
        , MAX(CASE WHEN hg_product = 'Jira Software' THEN 1 END) AS hg_product_jira_flag
        , MAX(CASE WHEN hg_product = 'GitHub (Unspecified Product)' THEN 1 END) AS hg_product_github_flag
        , MAX(CASE WHEN hg_product = 'Jenkins' THEN 1 END) AS hg_product_jenkins_flag
        , MAX(CASE WHEN hg_product = 'Red Hat Ansible' THEN 1 END) AS hg_product_ansible_flag
        , MAX(CASE WHEN hg_product = 'Atlassian Confluence' THEN 1 END) AS hg_product_confluence_flag
        , MAX(CASE WHEN hg_product = 'Spring' THEN 1 END) AS hg_product_spring_flag
        , MAX(CASE WHEN hg_product = 'Spring Boot' THEN 1 END) AS hg_product_spring_boot_flag
        , MAX(CASE WHEN hg_product = 'Azure DevOps' THEN 1 END) AS hg_product_azuredevops_flag
        , MAX(CASE WHEN hg_product = 'AWS Lambda' THEN 1 END) AS hg_product_aws_lambda_flag
        , MAX(CASE WHEN hg_product = 'Amazon Elastic Load Balancing' THEN 1 END) AS hg_product_elastic_load_balancing_flag
        , MAX(CASE WHEN hg_product = 'Amazon RDS (Relational Database Service)' THEN 1 END) AS hg_product_rds_flag
        , MAX(CASE WHEN hg_product = 'Atlassian Bitbucket' THEN 1 END) AS hg_product_bitbucket_flag
        , MAX(CASE WHEN hg_product = 'AWS CloudFormation' THEN 1 END) AS hg_product_cloudformation_flag
        , MAX(CASE WHEN hg_product = 'Google Cloud BigQuery' THEN 1 END) AS hg_product_bigquery_flag
        , MAX(CASE WHEN hg_product = 'AWS CodeCommit' THEN 1 END) AS hg_product_codecommit_flag
        , MAX(CASE WHEN hg_product = 'Amazon Elastic Kubernetes Service (Amazon EKS)' THEN 1 END) AS hg_product_eks_flag
        , MAX(CASE WHEN hg_product = 'GitHub Actions' THEN 1 END) AS hg_product_github_actions_flag
        , MAX(CASE WHEN hg_product = 'Amazon Elastic Container Service (ECS)' THEN 1 END) AS hg_product_ecs_flag
        , MAX(CASE WHEN hg_product = 'AWS Identity and Access Management (IAM)' THEN 1 END) AS hg_product_iam_flag
        , MAX(CASE WHEN hg_product = 'Red Hat OpenShift' THEN 1 END) AS hg_product_openshift_flag
        , MAX(CASE WHEN hg_product = 'Azure DevOps Server' THEN 1 END) AS hg_product_azuredevops_server_flag
        , MAX(CASE WHEN hg_product = 'Amazon Virtual Private Cloud (VPC)' THEN 1 END) AS hg_product_vpc_flag
        , MAX(CASE WHEN hg_product = 'JFrog (Unspecified Product)' THEN 1 END) AS hg_product_jfrog_flag
        , MAX(CASE WHEN hg_product = 'Amazon API Gateway' THEN 1 END) AS hg_product_amazon_api_gateway_flag
        , MAX(CASE WHEN hg_product = 'JFrog Artifactory' THEN 1 END) AS hg_product_artifactory_flag
        , MAX(CASE WHEN hg_product = 'Jira Service Management' THEN 1 END) AS hg_product_jira_service_manage_flag
        , MAX(CASE WHEN hg_product = 'Jira Service Desk' THEN 1 END) AS hg_product_jira_service_desk_flag
        , MAX(CASE WHEN hg_product = 'GitHub Enterprise' THEN 1 END) AS hg_product_github_ent_flag
        , MAX(CASE WHEN hg_product = 'Jira Sourcetree' THEN 1 END) AS hg_product_jira_sourcetree_flag
        , MAX(CASE WHEN hg_product = 'Snyk' THEN 1 END) AS hg_product_snyk_flag
        , MAX(CASE WHEN hg_product = 'Jira Work Management' THEN 1 END) AS hg_product_jira_work_manage_flag
        , MAX(CASE WHEN hg_product = 'Atlassian Opsgenie' THEN 1 END) AS hg_product_opsgenie_flag
        , MAX(CASE WHEN hg_product = 'Jira Align' THEN 1 END) AS hg_product_align_flag
        , MAX(CASE WHEN hg_product = 'GitHub Gist' THEN 1 END) AS hg_product_gist_flag
        , MAX(CASE WHEN hg_product = 'GitHub Copilot' THEN 1 END) AS hg_product_copilot_flag
        , MAX(CASE WHEN hg_product = 'Jira Advanced Roadmaps' THEN 1 END) AS hg_product_advanced_roadmaps_flag
        , MAX(CASE WHEN hg_product = 'Harness' THEN 1 END) AS hg_product_harness_flag
        , MAX(CASE WHEN hg_product = 'Conan' THEN 1 END) AS hg_product_conan_flag
        , MAX(CASE WHEN hg_product = 'JFrog Xray' THEN 1 END) AS hg_product_xray_flag
        , MAX(CASE WHEN hg_product = 'GitHub Pages' THEN 1 END) AS hg_product_github_pages_flag
        , MAX(CASE WHEN hg_product = 'GitHub Discussions' THEN 1 END) AS hg_product_github_discussions_flag
        , MAX(CASE WHEN hg_product = 'Drone' THEN 1 END) AS hg_product_drone_flag
        , MAX(CASE WHEN hg_product = 'JFrog Pipelines' THEN 1 END) AS hg_product_jfrog_pipelines_flag
        , MAX(CASE WHEN hg_product = 'CodeQL' THEN 1 END) AS hg_product_codeql_flag
        , MAX(CASE WHEN hg_product = 'Dependabot' THEN 1 END) AS hg_product_dependabot_flag
        , MAX(CASE WHEN hg_product = 'Snyk Code (SAST)' THEN 1 END) AS hg_product_snyk_code_flag
        , MAX(CASE WHEN hg_product = 'GitHub Advanced Security' THEN 1 END) AS hg_product_github_adv_sec_flag
        , MAX(CASE WHEN hg_product = 'Snyk Open Source (SCA)' THEN 1 END) AS hg_product_sca_flag
        , MAX(CASE WHEN hg_product = 'Azure DevOps Tool Integration' THEN 1 END) AS hg_product_azuredevops_tool_int_flag
        , MAX(CASE WHEN hg_product = 'JFrog Bintray' THEN 1 END) AS hg_product_jfrog_bintray
        , MAX(CASE WHEN hg_product = 'JFrog Mission Control' THEN 1 END) AS hg_product_jfrog_mission_control
        , MAX(CASE WHEN hg_product = 'The JFrog Platform' THEN 1 END) AS hg_product_jfrog_platform
        , MAX(CASE WHEN hg_product = 'Amazon Kinesis' THEN 1 END) AS hg_product_amazon_kinesis
        , MAX(CASE WHEN hg_product = 'Amazon SageMaker' THEN 1 END) AS hg_product_amazon_sagemaker
        , MAX(CASE WHEN hg_product = 'Amazon Elastic Block Store (EBS)' THEN 1 END) AS hg_product_amazon_ebs
        , MAX(CASE WHEN hg_product = 'CircleCI' THEN 1 END) AS hg_product_circleci
        , MAX(CASE WHEN hg_product = 'TeamCity' THEN 1 END) AS hg_product_teamcity
        , MAX(CASE WHEN hg_product = 'Sentry' THEN 1 END) AS hg_product_sentry
        , MAX(CASE WHEN hg_product = 'AWS Fargate' THEN 1 END) AS hg_product_aws_fargate
        , MAX(CASE WHEN hg_product = 'AWS Step Functions' THEN 1 END) AS hg_product_aws_step_functions
        , MAX(CASE WHEN hg_product = 'AWS CodePipeline' THEN 1 END) AS hg_product_aws_codepipeline
        , MAX(CASE WHEN hg_product = 'Spring Cloud' THEN 1 END) AS hg_product_spring_cloud
        , MAX(CASE WHEN hg_product = 'AWS Command Line Interface' THEN 1 END) AS hg_product_aws_cli
        , MAX(CASE WHEN hg_product = 'Amazon Elastic Container Registry (ECR)' THEN 1 END) AS hg_product_aws_ecr
        , MAX(CASE WHEN hg_product = 'Google Cloud Storage' THEN 1 END) AS hg_product_gcp_storage
        , MAX(CASE WHEN hg_product = 'AWS Cloud Development Kit' THEN 1 END) AS hg_product_aws_cloud_dev_kit
        , MAX(CASE WHEN hg_product = 'Google Cloud Apigee' THEN 1 END) AS hg_product_gcp_apigee
        , MAX(CASE WHEN hg_product = 'AWS Key Management Service (KMS)' THEN 1 END) AS hg_product_aws_kms
        , MAX(CASE WHEN hg_product = 'AWS CodeDeploy' THEN 1 END) AS hg_product_aws_codedeploy
        , MAX(CASE WHEN hg_product = 'AWS SDKs' THEN 1 END) AS hg_product_aws_sdks
        , MAX(CASE WHEN hg_product = 'AWS CodeBuild' THEN 1 END) AS hg_product_aws_codebuild
        , MAX(CASE WHEN hg_product = 'Amazon Cognito' THEN 1 END) AS hg_product_amazon_cognito
        , MAX(CASE WHEN hg_product = 'Google Compute Engine' THEN 1 END) AS hg_product_gcp_compute_engine
        , MAX(CASE WHEN hg_product = 'Amazon EventBridge' THEN 1 END) AS hg_product_amazon_eventbridge
        , MAX(CASE WHEN hg_product = 'AWS Application Load Balancer' THEN 1 END) AS hg_product_aws_app_load_balancer
        , MAX(CASE WHEN hg_product = 'Google Cloud SQL' THEN 1 END) AS hg_product_gcp_sql
        , MAX(CASE WHEN hg_product = 'Google Cloud Vertex AI' THEN 1 END) AS hg_product_gcp_vertex_ai
        , MAX(CASE WHEN hg_product = 'Google Cloud Run' THEN 1 END) AS hg_product_gcp_run
        , MAX(CASE WHEN hg_product = 'CloudBees' THEN 1 END) AS hg_product_cloudbees
        , MAX(CASE WHEN hg_product = 'AWS GovCloud (US)' THEN 1 END) AS hg_product_aws_govcloud
        , MAX(CASE WHEN hg_product = 'AWS Auto Scaling' THEN 1 END) AS hg_product_aws_auto_scaling
        , MAX(CASE WHEN hg_product = 'AWS AppSync' THEN 1 END) AS hg_product_aws_appsync
        , MAX(CASE WHEN hg_product = 'Google App Engine' THEN 1 END) AS hg_product_gcp_app_engine
        , MAX(CASE WHEN hg_product = 'AWS X-Ray' THEN 1 END) AS hg_product_aws_xray
        , MAX(CASE WHEN hg_product = 'CloudBees Jenkins' THEN 1 END) AS hg_product_cloudbees_jenkins
        , MAX(CASE WHEN hg_product = 'AWS Amplify' THEN 1 END) AS hg_product_aws_amplify
        , MAX(CASE WHEN hg_product = 'Google Cloud Identity and Access Management (IAM)' THEN 1 END) AS hg_product_gcp_iam
        , MAX(CASE WHEN hg_product = 'Amazon Linux AMI' THEN 1 END) AS hg_product_amazon_linux_ami
        , MAX(CASE WHEN hg_product = 'Google Cloud Build' THEN 1 END) AS hg_product_gcp_build
        , MAX(CASE WHEN hg_product = 'Amazon Bedrock' THEN 1 END) AS hg_product_aws_bedrock
        , MAX(CASE WHEN hg_product = 'Bitrise' THEN 1 END) AS hg_product_bitrise
        , MAX(CASE WHEN hg_product = 'Anthos' THEN 1 END) AS hg_product_anthos
        , MAX(CASE WHEN hg_product = 'CloudBees CI' THEN 1 END) AS hg_product_cloudbees_ci
        , MAX(CASE WHEN hg_product = 'Amazon MQ' THEN 1 END) AS hg_product_aws_mq
        , MAX(CASE WHEN hg_product = 'Red Hat OpenShift Service on AWS' THEN 1 END) AS hg_product_redhat_openshift_service
        , MAX(CASE WHEN hg_product = 'Google Cloud Artifact Registry' THEN 1 END) AS hg_product_gcp_artifact_registry
        , MAX(CASE WHEN hg_product = 'AWS CodeStar' THEN 1 END) AS hg_product_aws_codestar
        , MAX(CASE WHEN hg_product = 'BeyondCorp' THEN 1 END) AS hg_product_beyondcorp
        , MAX(CASE WHEN hg_product = 'Google Cloud Armor' THEN 1 END) AS hg_product_gcp_armor
        , MAX(CASE WHEN hg_product = 'Spring XD' THEN 1 END) AS hg_product_spring_xd
        , MAX(CASE WHEN hg_product = 'Red Hat OpenShift 3.x' THEN 1 END) AS hg_product_redhat_openshift3
        , MAX(CASE WHEN hg_product = 'Codefresh' THEN 1 END) AS hg_product_codefresh
        , MAX(CASE WHEN hg_product = 'AWS Copilot' THEN 1 END) AS hg_product_aws_copilot
        , MAX(CASE WHEN hg_product = 'Google Cloud Security Command Center' THEN 1 END) AS hg_product_gcp_security_command_center
        , MAX(CASE WHEN hg_product = 'Azure DevTest Labs' THEN 1 END) AS hg_product_azure_devtest_labs
        , MAX(CASE WHEN hg_product = 'Anthos GKE on-prem' THEN 1 END) AS hg_product_anthos_gke_onprem
        , MAX(CASE WHEN hg_product = 'Google Cloud Source Repositories' THEN 1 END) AS hg_product_gcp_source_repos
        , MAX(CASE WHEN hg_product = 'Anthos Service Mesh' THEN 1 END) AS hg_product_anthos_service_mesh
        , MAX(CASE WHEN hg_product = 'AWS Application Discovery Service' THEN 1 END) AS hg_product_aws_app_discovery
        , MAX(CASE WHEN hg_product = 'Bitnami' THEN 1 END) AS hg_product_bitnami
        , MAX(CASE WHEN hg_product = 'Amazon VPC Traffic Mirroring' THEN 1 END) AS hg_product_aws_vpc_traffic_mirroring
        , MAX(CASE WHEN hg_product = 'Semmle' THEN 1 END) AS hg_product_semmle
        , MAX(CASE WHEN hg_product = 'Google Cloud Security Scanner' THEN 1 END) AS hg_product_gcp_security_scanner
        , MAX(CASE WHEN hg_product = 'Amazon CodeGuru' THEN 1 END) AS hg_product_aws_codeguru
        , MAX(CASE WHEN hg_product = 'Shippable' THEN 1 END) AS hg_product_shippable
        , MAX(CASE WHEN hg_product = 'CloudBees CD' THEN 1 END) AS hg_product_cloudbees_cd
        , MAX(CASE WHEN hg_product = 'AWS App2Container' THEN 1 END) AS hg_product_aws_app2container
        , MAX(CASE WHEN hg_product = 'OverOps' THEN 1 END) AS hg_product_overops
        , MAX(CASE WHEN hg_product = 'Snyk Infrastructure as Code (Snyk IaC)' THEN 1 END) AS hg_product_snyk_iac
        , MAX(CASE WHEN hg_product = 'AWS App Runner' THEN 1 END) AS hg_product_aws_app_runner
        , MAX(CASE WHEN hg_product = 'AWS Tools for PowerShell' THEN 1 END) AS hg_product_aws_powershell
        , MAX(CASE WHEN hg_product = 'Google Cloud Deploy' THEN 1 END) AS hg_product_gcp_deploy
        , MAX(CASE WHEN hg_product = 'AWS Artifact' THEN 1 END) AS hg_product_aws_artifact
        , MAX(CASE WHEN hg_product = 'Jira Product Discovery' THEN 1 END) AS hg_product_jira_product_discovery
        , MAX(CASE WHEN hg_product = 'Google Cloud Application Security' THEN 1 END) AS hg_product_gcp_application_security
        , MAX(CASE WHEN hg_product = 'DeepCode' THEN 1 END) AS hg_product_deepcode
        , MAX(CASE WHEN hg_product = 'AWS Amplify Hosting' THEN 1 END) AS hg_product_aws_amplify_hosting
        , MAX(CASE WHEN hg_product = 'Yandex.Tank' THEN 1 END) AS hg_product_yandex
        , MAX(CASE WHEN hg_product = 'Red Hat OpenShift 4.x' THEN 1 END) AS hg_product_redhat_openshift4
        , MAX(CASE WHEN hg_product = 'AWS IAM Identity Center' THEN 1 END) AS hg_product_aws_iam_id_center
        , MAX(CASE WHEN hg_product = 'Mandiant (Unspecified Product)' THEN 1 END) AS hg_product_mandiant
        , MAX(CASE WHEN hg_product = 'Spring Cloud Sleuth' THEN 1 END) AS hg_product_spring_cloud_sleuth
        , MAX(CASE WHEN hg_product = 'Amazon Linux 2' THEN 1 END) AS hg_product_aws_linux2
        , MAX(CASE WHEN hg_product = 'AWS App Mesh' THEN 1 END) AS hg_product_aws_app_mesh
        , MAX(CASE WHEN hg_product = 'Google Container Registry' THEN 1 END) AS hg_product_gcp_container_registry
        , MAX(CASE WHEN hg_product = 'Codeship' THEN 1 END) AS hg_product_codeship
        , MAX(CASE WHEN hg_product = 'Electric Cloud' THEN 1 END) AS hg_product_electric_cloud
        , MAX(CASE WHEN hg_product = 'Amazon AppFlow' THEN 1 END) AS hg_product_amazon_appflow
        , MAX(CASE WHEN hg_product = 'YouTrack' THEN 1 END) AS hg_product_youtrack
        , MAX(CASE WHEN hg_product = 'Google Cloud Secret Manager' THEN 1 END) AS hg_product_gcp_secret_manager

        -- Top Vendors
        , MAX(b.hg_first_top_vendor) AS hg_first_top_vendor
        , MAX(b.hg_first_top_vendor_intensity) AS hg_first_top_vendor_intensity
        , MAX(b.hg_second_top_vendor) AS hg_second_top_vendor
        , MAX(b.hg_second_top_vendor_intensity) AS hg_second_top_vendor_intensity
        , MAX(b.hg_third_top_vendor) AS hg_third_top_vendor
        , MAX(b.hg_third_top_vendor_intensity) AS hg_third_top_vendor_intensity

        -- Top Vendor in each Parent Category
        , MAX(b.hg_tech_pcategory_cloud_services_top_vendor) AS hg_tech_pcategory_cloud_services_top_vendor
        , MAX(b.hg_tech_pcategory_lcm_top_vendor) AS hg_tech_pcategory_lcm_top_vendor
        , MAX(b.hg_tech_pcategory_dev_software_top_vendor) AS hg_tech_pcategory_dev_software_top_vendor
        , MAX(b.hg_tech_pcategory_op_management_top_vendor) AS hg_tech_pcategory_op_management_top_vendor
        , MAX(b.hg_tech_pcategory_app_infra_top_vendor) AS hg_tech_pcategory_app_infra_top_vendor
        , MAX(b.hg_tech_pcategory_resource_plan_top_vendor) AS hg_tech_pcategory_resource_plan_top_vendor
        , MAX(b.hg_tech_pcategory_dbm_top_vendor) AS hg_tech_pcategory_dbm_top_vendor
        , MAX(b.hg_tech_pcategory_ent_apps_top_vendor) AS hg_tech_pcategory_ent_apps_top_vendor
        , MAX(b.hg_tech_pcategory_os_top_vendor) AS hg_tech_pcategory_os_top_vendor
        , MAX(b.hg_tech_pcategory_virtual_top_vendor) AS hg_tech_pcategory_virtual_top_vendor
        , MAX(b.hg_tech_pcategory_collab_top_vendor) AS hg_tech_pcategory_collab_top_vendor
        , MAX(b.hg_tech_pcategory_im_top_vendor) AS hg_tech_pcategory_im_top_vendor
        , MAX(b.hg_tech_pcategory_security_top_vendor) AS hg_tech_pcategory_security_top_vendor

        -- Top Vendor in each Category
        , MAX(b.hg_tech_category_dev_tools_top_vendor) AS hg_tech_category_dev_tools_top_vendor
        , MAX(b.hg_tech_category_modeling_tools_top_vendor) AS hg_tech_category_modeling_tools_top_vendor
        , MAX(b.hg_tech_category_software_top_vendor) AS hg_tech_category_software_top_vendor
        , MAX(b.hg_tech_category_app_middleware_top_vendor) AS hg_tech_category_app_middleware_top_vendor
        , MAX(b.hg_tech_category_b2b_middleware_top_vendor) AS hg_tech_category_b2b_middleware_top_vendor
        , MAX(b.hg_tech_category_event_middleware_top_vendor) AS hg_tech_category_event_middleware_top_vendor
        , MAX(b.hg_tech_category_integration_middleware_top_vendor) AS hg_tech_category_integration_middleware_top_vendor
        , MAX(b.hg_tech_category_testing_top_vendor) AS hg_tech_category_testing_top_vendor
        , MAX(b.hg_tech_category_process_management_top_vendor) AS hg_tech_category_process_management_top_vendor
        , MAX(b.hg_tech_category_iaas_top_vendor) AS hg_tech_category_iaas_top_vendor
        , MAX(b.hg_tech_category_paas_top_vendor) AS hg_tech_category_paas_top_vendor
        , MAX(b.hg_tech_category_linux_top_vendor) AS hg_tech_category_linux_top_vendor
        , MAX(b.hg_tech_category_config_management_top_vendor) AS hg_tech_category_config_management_top_vendor
        , MAX(b.hg_tech_category_asset_management_top_vendor) AS hg_tech_category_asset_management_top_vendor
        , MAX(b.hg_tech_category_service_desk_top_vendor) AS hg_tech_category_service_desk_top_vendor
        , MAX(b.hg_tech_category_network_management_top_vendor) AS hg_tech_category_network_management_top_vendor
        , MAX(b.hg_tech_category_iam_top_vendor) AS hg_tech_category_iam_top_vendor
        , MAX(b.hg_tech_category_threat_managment_top_vendor) AS hg_tech_category_threat_managment_top_vendor
        , MAX(b.hg_tech_category_vulnerability_management_top_vendor) AS hg_tech_category_vulnerability_management_top_vendor

        ,CURRENT_TIMESTAMP() AS dbt_updated_at

FROM dedup_account_months a
LEFT JOIN top_vendors b
    ON  a.crm_account_id = b.crm_account_id
    AND a.snapshot_month = b.snapshot_month
GROUP BY a.crm_account_id, a.snapshot_month
                           