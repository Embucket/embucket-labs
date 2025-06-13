WITH parse_keystone AS (

    SELECT *
    FROM {{ ref('content_keystone_source') }},
        
), union_types AS (

    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'form_urls' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone, 
        LATERAL FLATTEN(input => parse_keystone.full_value:form_urls) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'landing_page_url' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:landing_page_urls) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'utm_campaign_name' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:utm_campaign_name) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'utm_content_name' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:utm_content_name) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'sfdc_campaigns' AS join_string_type,
        flattened_parsed_keystone.value::VARCHAR AS join_string
    FROM parse_keystone,
        LATERAL FLATTEN(input => parse_keystone.full_value:sfdc_campaigns) flattened_parsed_keystone
    UNION ALL
    SELECT
        content_name,
        gitlab_epic,
        language,
        gtm,
        type,
        url_slug,
        'url_slug' AS join_string_type,
        url_slug AS join_string
    FROM parse_keystone

), prep_crm_unioned_touchpoint AS (

    SELECT
        touchpoint_id AS dim_crm_touchpoint_id,
        bizible_touchpoint_date,
        campaign_id AS dim_campaign_id,
        bizible_ad_campaign_name,
        bizible_form_url,
        bizible_landing_page,
        CASE WHEN contains(bizible_landing_page, 'learn.gitlab.com/') AND split_part(bizible_landing_page, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_landing_page, '/', 3)
        END AS path_factory_slug_landing_page,
        CASE WHEN contains(bizible_form_url, 'learn.gitlab.com/') AND split_part(bizible_form_url, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_form_url, '/', 3)
        END AS path_factory_slug_form_url,
        COALESCE(path_factory_slug_landing_page, path_factory_slug_form_url)          AS pathfactory_slug,
        PARSE_URL(null)['parameters']['utm_campaign']::VARCHAR    AS bizible_landing_page_utm_campaign,
        PARSE_URL(null)['parameters']['utm_campaign']::VARCHAR        AS bizible_form_page_utm_campaign,
        PARSE_URL(null)['parameters']['utm_content']::VARCHAR     AS bizible_landing_page_utm_content,
        PARSE_URL(null)['parameters']['utm_content']::VARCHAR         AS bizible_form_page_utm_content,
        COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
        COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content
    FROM {{ ref('prep_crm_attribution_touchpoint') }}
    UNION ALL
    SELECT
        touchpoint_id AS dim_crm_touchpoint_id,
        bizible_touchpoint_date,
        campaign_id AS dim_campaign_id,
        bizible_ad_campaign_name,
        bizible_form_url,
        bizible_landing_page,
        CASE WHEN contains(bizible_landing_page, 'learn.gitlab.com/') AND split_part(bizible_landing_page, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_landing_page, '/', 3)
        END AS path_factory_slug_landing_page,
        CASE WHEN contains(bizible_form_url, 'learn.gitlab.com/') AND split_part(bizible_form_url, '/', 3) = '' 
            THEN null
            ELSE split_part(bizible_form_url, '/', 3)
        END AS path_factory_slug_form_url,
        COALESCE(path_factory_slug_landing_page, path_factory_slug_form_url)          AS pathfactory_slug,
        PARSE_URL(bizible_landing_page_raw)['parameters']['utm_campaign']::VARCHAR    AS bizible_landing_page_utm_campaign,
        PARSE_URL(bizible_form_url_raw)['parameters']['utm_campaign']::VARCHAR        AS bizible_form_page_utm_campaign,
        PARSE_URL(bizible_landing_page_raw)['parameters']['utm_content']::VARCHAR     AS bizible_landing_page_utm_content,
        PARSE_URL(bizible_form_url_raw)['parameters']['utm_content']::VARCHAR         AS bizible_form_page_utm_content,
        COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
        COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content
    FROM {{ ref('prep_crm_touchpoint') }}

), content_types as (
    SELECT '/ebook_' as search_string UNION ALL
    SELECT '/report_' UNION ALL 
    SELECT '/onepager_' UNION ALL
    SELECT '/comparison_' UNION ALL
    SELECT '/assessment_' UNION ALL
    SELECT '/blog_' UNION ALL
    SELECT '/presentation' UNION ALL
    SELECT '/whitepaper_'

), create_content_code_offline as (
    //offline touchpoints
    SELECT
        dim_crm_touchpoint_id,
        bizible_ad_campaign_name,
        search_string || SUBSTRING(
            bizible_ad_campaign_name,
            POSITION(search_string IN bizible_ad_campaign_name) + LENGTH(search_string)
        ) AS full_content_string,
        SPLIT_PART(full_content_string, '_', 1) AS content_type,
        CASE WHEN ARRAY_SIZE(SPLIT(full_content_string, '_')) = 3 THEN
                SPLIT_PART(full_content_string, '_', 2)
            WHEN ARRAY_SIZE(SPLIT(full_content_string, '_')) = 2 THEN
                NULL
        END AS content_language,
        CASE WHEN ARRAY_SIZE(SPLIT(full_content_string, '_')) = 3 THEN
                SPLIT_PART(full_content_string, '_', 3)
            WHEN ARRAY_SIZE(SPLIT(full_content_string, '_')) = 2 THEN
                SPLIT_PART(full_content_string, '_', 2)
        END AS content_key
    FROM prep_crm_unioned_touchpoint
    JOIN content_types
        ON CONTAINS(bizible_ad_campaign_name, search_string)
    WHERE
    bizible_form_url IS NULL
    // we created the content code mapping system at the start of FY26
    AND bizible_touchpoint_date > '2025-02-01'

), CREATE_content_code_online AS (
    SELECT 
        dim_crm_touchpoint_id,

        search_string || SUBSTRING(
            bizible_landing_page,
            POSITION(search_string IN bizible_landing_page) + LENGTH(search_string)
        ) AS full_content_landing_string,

        search_string || SUBSTRING(
            bizible_form_url,
            POSITION(search_string IN bizible_form_url) + LENGTH(search_string)
        ) AS full_content_form_string,
        
        REPLACE(COALESCE(SPLIT_PART(full_content_landing_string, '_', 1), SPLIT_PART(full_content_form_string, '_', 1)), '/','') AS content_type,

        --content language landing
        CASE WHEN ARRAY_SIZE(SPLIT(full_content_landing_string, '_')) = 3 THEN
                SPLIT_PART(full_content_landing_string, '_', 2)
            WHEN ARRAY_SIZE(SPLIT(full_content_landing_string, '_')) = 2 THEN
                NULL
        END AS content_language_landing,
       --content language form
        CASE WHEN ARRAY_SIZE(SPLIT(full_content_form_string, '_')) = 3 THEN
                SPLIT_PART(full_content_form_string, '_', 3)
            WHEN ARRAY_SIZE(SPLIT(full_content_form_string, '_')) = 2 THEN
                SPLIT_PART(full_content_form_string, '_', 2)
        END AS content_language_form,
        COALESCE(content_language_landing, content_language_form) AS content_language,

        --content key landing
        CASE WHEN ARRAY_SIZE(SPLIT(full_content_landing_string, '_')) = 3 THEN
                SPLIT_PART(full_content_landing_string, '_', 2)
            WHEN ARRAY_SIZE(SPLIT(full_content_landing_string, '_')) = 2 THEN
                NULL
        END AS content_key_landing,
        --content key form
        CASE WHEN ARRAY_SIZE(SPLIT(full_content_form_string, '_')) = 3 THEN
                SPLIT_PART(full_content_form_string, '_', 3)
            WHEN ARRAY_SIZE(SPLIT(full_content_form_string, '_')) = 2 THEN
                SPLIT_PART(full_content_form_string, '_', 2)
        END AS content_key_form,
        COALESCE(content_key_landing, content_key_form) AS content_key

    FROM prep_crm_unioned_touchpoint
    JOIN content_types
        ON CONTAINS(bizible_form_url, search_string) 
            OR CONTAINS(bizible_landing_page, search_string)
    WHERE
    -- we created the content code mapping system at the start of FY26
    bizible_touchpoint_date > '2025-02-01'
    AND bizible_form_url IS NOT NULL
 ), combined_model AS (
 
    SELECT
        prep_crm_unioned_touchpoint.dim_crm_touchpoint_id,
        COALESCE(
            sfdc_campaigns.content_name,
            utm_campaigns.content_name,
            form_urls.content_name,
            landing_pages.content_name,
            pathfactory_slug.content_name,
            create_content_code_online.content_key,
            create_content_code_offline.content_key
        ) AS content_name,
        COALESCE(
            sfdc_campaigns.gitlab_epic,
            utm_campaigns.gitlab_epic,
            form_urls.gitlab_epic,
            landing_pages.gitlab_epic,
            pathfactory_slug.gitlab_epic
        ) AS gitlab_epic,
        COALESCE(
            sfdc_campaigns.language,
            utm_campaigns.language,
            form_urls.language,
            landing_pages.language,
            pathfactory_slug.language,
            create_content_code_online.content_language,
            create_content_code_offline.content_language
        ) AS language,
        COALESCE(
            sfdc_campaigns.gtm,
            utm_campaigns.gtm,
            form_urls.gtm,
            landing_pages.gtm,
            pathfactory_slug.gtm
        ) AS gtm,
        COALESCE(
            sfdc_campaigns.url_slug,
            utm_campaigns.url_slug,
            form_urls.url_slug,
            landing_pages.url_slug,
            pathfactory_slug.url_slug,
            create_content_code_online.content_key,
            create_content_code_offline.content_key
        ) AS url_slug,
        COALESCE(
            sfdc_campaigns.type,
            utm_campaigns.type,
            form_urls.type,
            landing_pages.type,
            pathfactory_slug.type,
            create_content_code_online.content_type,
            create_content_code_offline.content_type
        ) AS type
    FROM prep_crm_unioned_touchpoint
    LEFT JOIN union_types sfdc_campaigns
        ON sfdc_campaigns.join_string_type = 'sfdc_campaigns'
        AND prep_crm_unioned_touchpoint.dim_campaign_id = sfdc_campaigns.join_string
    LEFT JOIN union_types utm_campaigns
        ON utm_campaigns.join_string_type = 'utm_campaign_name'
        AND prep_crm_unioned_touchpoint.utm_campaign = utm_campaigns.join_string
    LEFT JOIN union_types form_urls
        ON form_urls.join_string_type = 'form_urls'
        AND prep_crm_unioned_touchpoint.bizible_form_url = form_urls.join_string
    LEFT JOIN union_types landing_pages
        ON landing_pages.join_string_type = 'landing_page_url'
        AND prep_crm_unioned_touchpoint.bizible_landing_page = landing_pages.join_string
    LEFT JOIN union_types pathfactory_slug
        ON pathfactory_slug.join_string_type = 'url_slug'
        AND prep_crm_unioned_touchpoint.pathfactory_slug = pathfactory_slug.join_string
    LEFT JOIN create_content_code_offline
        ON create_content_code_offline.dim_crm_touchpoint_id = prep_crm_unioned_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN create_content_code_online
        ON create_content_code_online.dim_crm_touchpoint_id = create_content_code_online.dim_crm_touchpoint_id

)

SELECT *
FROM combined_model
WHERE content_name IS NOT NULL
