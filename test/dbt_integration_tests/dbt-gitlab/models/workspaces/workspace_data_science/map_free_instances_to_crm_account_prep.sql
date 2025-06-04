
{{ config({
    "materialized": "incremental",
    "unique_key": "host_name",
    "tags": ["mnpi_exception"]
    })
}}

{%- if is_incremental() %}
    {%- set day_check_query %}
        SELECT 
            CASE 
                WHEN date_part('day', current_date) = {{ var('MODEL_RUN_DAY', 2) }} THEN true  -- run on 2nd of month. Or the date provided to the variable MODEL_RUN_DAY
                WHEN date_part('day', current_date) > {{ var('MODEL_RUN_DAY', 2) }}  -- If it didn't run in the 2nd of the month, it should run the next time the DAG runs
                    AND date_part('month', current_date) <> (
                        SELECT date_part('month', max(processed_date)) 
                        FROM {{ this }}
                    ) THEN true
                ELSE false
            END AS should_run
    {%- endset %}

    {%- set results = run_query(day_check_query) %}
    {%- set should_run = results.columns[0].values()[0] %}
{%- else %}
    {%- set should_run = true %} -- If full_refresh, always run
{%- endif %}

{%- if should_run %} -- Run the query if the shoud_run condition is TRUE

WITH mart_ping_instance AS (
    
    SELECT *
    FROM {{ ref('mart_ping_instance') }}

),

dim_crm_account AS (
    SELECT *
    FROM {{ ref('dim_crm_account') }} 

),

sfdc_lead_source AS (
    SELECT *
    FROM {{ ref('sfdc_lead_source') }}
),

sfdc_contact_source AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}

),

free_instances_int AS (

    SELECT
        host_name,
        CASE
            WHEN RIGHT(host_name, LEN(host_name) - position('.', host_name )) LIKE ANY ('duckdns.org', 'local.com', 'example.com', 'local.com', 'test.com', 'git.com', 'gitlab.cn',
                'no-ip.org', 'dl.net', 'pp.ua', 'devops.com', 'home.com', 'dev.com', 'lab.com', 'demo.com', 'gitlab.io', 'reply.it', 'my.com', 'example.org',
                'cloud.com', 'doneproperly.io', 'home.io',
                'beget', 'mooo' -- hosting platform
                )
                    THEN NULL
            WHEN host_name LIKE '%git.labs.%'
                AND RIGHT(host_name, LEN(host_name) - position('git.labs.', host_name ))
                    LIKE ANY ('duckdns.org', 'local.com', 'example.com', 'local.com', 'test.com', 'git.com', 'gitlab.cn',
                'no-ip.org', 'dl.net', 'pp.ua', 'devops.com', 'home.com', 'dev.com', 'lab.com', 'demo.com', 'gitlab.io', 'reply.it', 'my.com', 'example.org',
                'cloud.com', 'doneproperly.io', 'home.io')
                    THEN NULL
            WHEN host_name LIKE '%git.labs.%'
                THEN RIGHT(host_name, LEN(host_name) - position('git.labs.', host_name ))
            WHEN LEN(RIGHT(host_name, LEN(host_name) - position('.', host_name ))) <= 3 -- Matching extension of domains: .com, .de, ...
                THEN LEFT(host_name, LEN(host_name) - LEN(RIGHT(host_name, LEN(host_name) - position('.', host_name ))) - 1 )
            
            ELSE RIGHT(host_name, LEN(host_name) - position('.', host_name ))
        END AS host_name_chopped,
        CASE WHEN host_name_chopped LIKE '%.edu%' THEN true else false end is_educational,
        CASE WHEN REGEXP_LIKE(host_name_chopped, '^[0-9.]+$') THEN TRUE ELSE FALSE END AS is_ip,
        CASE 
            WHEN is_ip THEN NULL -- if it is an IP, no company name to extract
            
            WHEN ARRAY_SIZE(SPLIT(host_name_chopped, '.')) > 1 AND SPLIT_PART(host_name_chopped, '.', 1) NOT IN ('devops', 'dev', 'hosting', 'info') AND LENGTH(SPLIT_PART(host_name_chopped, '.', 1)) >= LENGTH(SPLIT_PART(host_name_chopped, '.', 2))
                THEN SPLIT_PART(host_name_chopped, '.', 1)  -- Sometimes, the company name is very likely to be the first longest string (separated by dots). Example; gitlab.paperlesswms.com.au

            WHEN ARRAY_SIZE(SPLIT(host_name_chopped, '.')) > 2 AND SPLIT_PART(host_name_chopped, '.', 2) NOT IN ('devops', 'dev', 'hosting', 'info') AND LENGTH(SPLIT_PART(host_name_chopped, '.', 2)) >= LENGTH(SPLIT_PART(host_name_chopped, '.', 3))
                THEN SPLIT_PART(host_name_chopped, '.', 2)  -- Sometimes, it is the second longest string. Example: git.lab.paperlesswms.com.au
                
            WHEN ARRAY_SIZE(SPLIT(host_name, '.')) = 3 AND SPLIT_PART(host_name, '.', 2) = 'co'
                THEN SPLIT_PART(host_name, '.', 1) -- Whenever there is a .co, the company name is usually before it. EXAMPLE firma-nets.co.za
                
            WHEN ARRAY_SIZE(SPLIT(host_name_chopped, '.')) = 2 THEN SPLIT_PART(host_name_chopped, '.', 1)
            
            WHEN ARRAY_SIZE(SPLIT(host_name_chopped, '.')) = 3 THEN
                CASE
                    WHEN SPLIT_PART(host_name_chopped, '.', 2) = 'co' THEN SPLIT_PART(host_name_chopped, '.', 1) -- gitlab.signitysolutions.co.in
                    ELSE SPLIT_PART(host_name_chopped, '.', 2)
                END
            ELSE NULL
        END AS EXTRACTED_NAME_,

        CASE
            WHEN EXTRACTED_NAME_ LIKE ANY ('home', 'local', 'dev', 'office', 'vm', 'localdev', 'example', 'synology', 'science', 'net', 'com', 'corp', 'cloud', 'yandexcloud', 'gitlab', 'internal', 'duckdns', 'iptime', 'fritz', 'ap-northeast-1', 'services', 'intra', 'tpddns', 'service',
            'test', 'private', 'test', 'private', 'devops', 'platform', 'digital', 'plugins', 'develop', 'homelab', 'cluster', 'no-ip', 'docker', 'library', 'development', 'space', 'demo', 'code', 'shared', 'us-east-1', 'self', 'unknown', 'flex', 'reply', 'network',
            '%gitlab%', -- any mention of gitlab gets taken out
            'beget', 'mooo',  -- hosting platforms
            'admin', 'solo', 'cicd', 'video', 'music', 'group',
            'core', 'tech', 'domain', 'linux', 'research', 'labs', 'devtools', 'dev-tools', 'stage', 'servers', 'asia', 'testing',
            'factory', 'cert', 'developer', 'software', 'public', 'selfhost', 'selfhosted', 'support', 'forge', 'work', 'tooling', 'devsecops', 'studio',
            'datacenter', 'portal', 'root', 'state', 'testproject', 'base', 'security', 'alpha', 'beta', 'build', 'casa',
            'localnet', 'myhome', 'open', 'developers', 'kernel', 'sites', 'sudo', 'access', 'host', 'materna', 'media',
            'personal', 'playground', 'data', 'cultura', 'power', 'pilot', 'ubuntu', 'internet', 'spark', 'house') THEN NULL
            WHEN LENGTH(EXTRACTED_NAME_) <= 3 then NULL
            ELSE EXTRACTED_NAME_ END extracted_name,
            MAX(ping_created_date_month) AS max_ping_created_date_month
    FROM mart_ping_instance
    WHERE ping_product_tier = 'Free'
        AND ping_deployment_type = 'Self-Managed'
        AND is_last_ping_of_month = TRUE
        AND ping_created_date_month >= '2022-01-01'
        AND host_name IS NOT NULL
        AND host_name <> 'gitlab.example.com'
        AND umau_value > 0
        AND host_name_chopped != 'gitlab.com'
    GROUP BY 1

), free_instances_agg AS (

    SELECT
        extracted_name,
        COUNT(*) as number_of_pings,
        ROW_NUMBER() OVER(PARTITION BY 1 ORDER BY number_of_pings DESC) AS ping_number
    FROM free_instances_int
    GROUP BY 1
    ORDER BY number_of_pings DESC

)
, free_instances AS (
    
    SELECT
        free_instances_int.* EXCLUDE (host_name_chopped, extracted_name),
        
        CASE -- If the extracted name has a lot of host_names associated with it, it is probably a bad match. Then we ignore those and add a couple of exceptions
            WHEN NOT ((ping_number > 25
        or free_instances_int.extracted_name in ('netease', 'moduleworks', 'colopl', 'rwth-aachen', 'lenovo', 'nasa', 'magnolia-platform',
        'linuxtechi', 'shiyue', 'keenetic', 'hanpda', 'fraunhofer', 'asuscomm', 'fraunhofer', 'informatik',
        'dsone', 'uni-heidelberg', 'thetaray')))
            THEN TRUE
            ELSE FALSE
        END AS is_exclude_host_name, 
        
        CASE WHEN is_exclude_host_name = TRUE THEN NULL ELSE free_instances_int.host_name_chopped END host_name_chopped,
        CASE WHEN is_exclude_host_name = TRUE THEN NULL ELSE free_instances_int.extracted_name END extracted_name,
        
        REGEXP_REPLACE(free_instances_int.host_name_chopped, '[^a-zA-Z0-9\s]', '') AS host_name_chopped_cleaned,
        REGEXP_REPLACE(free_instances_int.extracted_name, '[^a-zA-Z0-9\s]', '') AS extracted_name_cleaned
    FROM free_instances_int
    LEFT JOIN free_instances_agg
        ON free_instances_int.extracted_name = free_instances_agg.extracted_name
        

), accounts_base AS ( -- clean company name and remove company names that are non valid
    
    SELECT
        dim_crm_account.dim_crm_account_id,
        dim_crm_account.dim_parent_crm_account_id,
        dim_crm_account.account_domains,
        REPLACE(REPLACE(REPLACE(TRIM(LOWER(SPLIT(dim_crm_account.account_domains, ',')[0]::VARCHAR)),
            'www.', ''),
            'https:', ''),
            '/', '') AS account_domains_1_int,
        REPLACE(REPLACE(REPLACE(TRIM(LOWER(dim_crm_account.account_domain_1::VARCHAR)),
            'www.', ''),
            'https:', ''),
            '/', '') AS account_domains_1_int2,
        COALESCE(account_domains_1_int2, account_domains_1_int) AS account_domains_1,
        REPLACE(REPLACE(REPLACE(TRIM(LOWER(dim_crm_account.account_domain_2::VARCHAR)),
            'www.', ''),
            'https:', ''),
            '/', '') AS account_domains_2_int,
        CASE
            WHEN EQUAL_NULL(account_domains_2_int, account_domains_1)
                THEN NULL
            ELSE account_domains_2_int
        END account_domains_2,
            
        dim_crm_account.crm_account_name,
        TRIM(REGEXP_REPLACE(LOWER(REGEXP_REPLACE(dim_crm_account.crm_account_name, '\\b(LLC|Inc|Ltd|Corporation|Corp|Co|Limited|GmbH|University|BV|Bv)\\b\\.?', '')),
            '[^a-zA-Z0-9\s]', '')) AS cleaned_company_name,
            
        TRIM(REGEXP_REPLACE(LOWER(REGEXP_REPLACE(SPLIT_PART(dim_crm_account.crm_account_name, '.', 1), '\\b(LLC|Inc|Ltd|Corporation|Corp|Co|Limited|GmbH|University|BV|Bv|of)\\b\\.?', '')),
            '[^a-zA-Z0-9\s]', '')) AS cleaned_company_name_no_dot,
        parent.account_domains AS parent_account_domains,
        parent.crm_account_name AS parent_crm_account_name,
        IFF(dim_crm_account.dim_crm_account_id = dim_crm_account.dim_parent_crm_account_id, True, False) AS is_parent_account,
        dim_crm_account.crm_account_employee_count,
        dim_crm_account.last_activity_date
        
    FROM dim_crm_account
    LEFT JOIN dim_crm_account AS parent
        ON dim_crm_account.dim_parent_crm_account_id = parent.dim_crm_account_id
    WHERE NOT (cleaned_company_name LIKE ANY ('0', 'na', '%missing%', 'none', 'yandexcloud', 'gitlab%', 'synology', 'demo', 'education', 'noip') )
      AND NOT LOWER(dim_crm_account.crm_account_name) LIKE '%dupe%' -- remove duplicated accounts
        
), leads_int AS (

    SELECT
        email_domain,
        lean_data_matched_account AS dim_crm_account_id,
        COUNT(*) AS matches
    FROM sfdc_lead_source
    WHERE email_domain_type = 'Business email domain'
        AND lean_data_matched_account IS NOT NULL
    GROUP BY 1, 2

    UNION

    SELECT
        email_domain,
        account_id,
        COUNT(*) AS matches
    FROM sfdc_contact_source
    WHERE email_domain_type = 'Business email domain'
        AND account_id IS NOT NULL
    GROUP BY 1, 2

), leads_end AS (

    SELECT leads_int.*
    FROM leads_int
    INNER JOIN accounts_base
        ON accounts_base.dim_crm_account_id = leads_int.dim_crm_account_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY leads_int.dim_crm_account_id ORDER BY matches DESC) <= 3
    
), leads AS (

    SELECT leads_end.*,
        accounts_base.dim_parent_crm_account_id,
        accounts_base.crm_account_name,
        accounts_base.cleaned_company_name,
        accounts_base.account_domains,
        accounts_base.parent_account_domains,
        accounts_base.parent_crm_account_name,
        accounts_base.crm_account_employee_count,
        accounts_base.last_activity_date,
        accounts_base.is_parent_account
    FROM leads_end
    LEFT JOIN accounts_base
        ON leads_end.dim_crm_account_id = accounts_base.dim_crm_account_id
        
), accounts AS (

    SELECT *
    FROM accounts_base
    WHERE LENGTH(crm_account_name) > 3
    
), free_instances_with_accounts_first AS (

    SELECT DISTINCT
        a.host_name,
        a.host_name_chopped, 
        a.extracted_name,
        a.extracted_name_cleaned,
        a.is_educational,
        a.is_ip,
        a.max_ping_created_date_month,
        accounts.dim_crm_account_id,
        CASE WHEN accounts.dim_crm_account_id IS NOT NULL THEN 1 END AS match_count,
        accounts.account_domains_1 AS match
    FROM free_instances a
    LEFT JOIN accounts
        ON accounts.account_domains_1 = a.host_name_chopped

), free_instances_with_accounts_second AS (

    SELECT DISTINCT
        a.host_name,
        a.host_name_chopped, 
        a.extracted_name,
        a.extracted_name_cleaned,
        a.is_educational,
        a.is_ip,
        a.max_ping_created_date_month,
        CASE
            WHEN a.dim_crm_account_id IS NULL THEN accounts.dim_crm_account_id
            ELSE a.dim_crm_account_id
        END AS dim_crm_account_id,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match_count
            WHEN accounts.dim_crm_account_id IS NOT NULL THEN 1
        END AS match_count,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match
            ELSE accounts.account_domains_2
        END AS match
    FROM free_instances_with_accounts_first a 
    LEFT JOIN accounts
        ON a.dim_crm_account_id IS NULL -- Only try to join the instances that we could not match in the previous join
        AND accounts.account_domains_2 = a.host_name_chopped

), free_instances_with_accounts_third AS (

    SELECT DISTINCT
        a.host_name,
        a.host_name_chopped, 
        a.extracted_name,
        a.extracted_name_cleaned,
        a.is_educational,
        a.is_ip,
        a.max_ping_created_date_month,
        CASE WHEN a.dim_crm_account_id IS NULL THEN accounts.dim_crm_account_id
            ELSE a.dim_crm_account_id
        END AS dim_crm_account_id,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match_count
            WHEN accounts.dim_crm_account_id IS NOT NULL THEN 2
        END AS match_count,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match
            ELSE accounts.cleaned_company_name
        END AS match
    FROM free_instances_with_accounts_second a 
    LEFT JOIN accounts
        ON a.dim_crm_account_id IS NULL -- Only try to join the instances that we could not match in the previous join
        AND accounts.cleaned_company_name = a.extracted_name
        AND CASE WHEN a.host_name_chopped NOT LIKE '%.edu%' THEN TRUE -- If the ping is educational, only match it if the account is also educational. For example, the ping gitlab.oit.duke.edu, was previously matching the company dukes.com instead of duke.edu
            ELSE IFF(accounts.account_domains LIKE '%.edu%', TRUE, FALSE) END 

), free_instances_with_accounts_fourth AS (

    SELECT DISTINCT
        a.host_name,
        a.host_name_chopped, 
        a.extracted_name,
        a.extracted_name_cleaned,
        a.is_educational,
        a.is_ip,
        a.max_ping_created_date_month,
        CASE WHEN a.dim_crm_account_id IS NULL THEN accounts.dim_crm_account_id
            ELSE a.dim_crm_account_id
        END AS dim_crm_account_id,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match_count
            WHEN accounts.dim_crm_account_id IS NOT NULL THEN 3
        END AS match_count,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match
            ELSE accounts.cleaned_company_name_no_dot
        END AS match
    FROM free_instances_with_accounts_third a 
    LEFT JOIN accounts
        ON a.dim_crm_account_id IS NULL -- Only try to join the instances that we could not match in the previous join
        AND accounts.cleaned_company_name_no_dot = a.extracted_name_cleaned
        AND CASE WHEN a.host_name_chopped NOT LIKE '%.edu%' THEN TRUE -- If the ping is educational, only match it if the account is also educational. For example, the ping gitlab.oit.duke.edu, was previously matching the company dukes.com instead of duke.edu
            ELSE IFF(accounts.account_domains LIKE '%.edu%', TRUE, FALSE) END

), free_instances_with_accounts_fifth AS (

    SELECT DISTINCT
        a.host_name,
        a.host_name_chopped, 
        a.extracted_name,
        a.extracted_name_cleaned,
        a.is_educational,
        a.is_ip,
        a.max_ping_created_date_month,
        CASE
            WHEN a.dim_crm_account_id IS NULL THEN leads.dim_crm_account_id 
            ELSE a.dim_crm_account_id
        END AS dim_crm_account_id,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match_count
            WHEN leads.dim_crm_account_id IS NOT NULL THEN 4
        END AS match_count,
        CASE
            WHEN a.dim_crm_account_id IS NOT NULL THEN match
            ELSE leads.email_domain
        END AS match
    FROM free_instances_with_accounts_fourth a 
    LEFT JOIN leads
        ON a.dim_crm_account_id IS NULL -- Only try to join the instances that we could not match in the previous join
        AND leads.email_domain = a.host_name_chopped

), free_instances_with_accounts_final AS (

    SELECT
        a.host_name,
        a.host_name_chopped, 
        a.extracted_name,
        a.extracted_name_cleaned,
        a.is_educational,
        a.is_ip,
        a.max_ping_created_date_month,
        a.dim_crm_account_id AS matched_dim_crm_account_id,
        a.match_count,
        a.match,
        accounts.crm_account_name AS matched_crm_account_name,
        accounts.dim_parent_crm_account_id,
        accounts.crm_account_employee_count AS matched_crm_account_employee_count,
        accounts.last_activity_date,
        CASE
            WHEN a.dim_crm_account_id = accounts.dim_parent_crm_account_id THEN TRUE
            ELSE FALSE
        END is_parent_crm_account
    FROM free_instances_with_accounts_fifth a
    LEFT JOIN accounts
        ON accounts.dim_crm_account_id = a.dim_crm_account_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY a.host_name ORDER BY is_parent_crm_account DESC, accounts.crm_account_employee_count DESC NULLS LAST,  accounts.last_activity_date DESC NULLS LAST) = 1

)

SELECT DISTINCT
    host_name,
    host_name_chopped, 
    extracted_name,
    extracted_name_cleaned,
    matched_dim_crm_account_id,
    matched_crm_account_name,
    match_count,
    CASE
        WHEN match_count IN (1) THEN 'High'
        WHEN match_count IN (2, 3) THEN 'Medium'
        WHEN match_count IN (4) THEN 'Low'
    END AS match_quality,
    match,
    is_educational,
    is_ip,

    max_ping_created_date_month AS last_date_host_name_seen,
    CURRENT_DATE AS processed_date
    
FROM free_instances_with_accounts_final

{%- else %}
    -- Else return no rows
    SELECT
        NULL AS host_name,
        NULL AS host_name_chopped, 
        NULL AS extracted_name,
        NULL AS extracted_name_cleaned,
        NULL AS matched_dim_crm_account_id,
        NULL AS matched_crm_account_name,
        NULL AS match_count,
        NULL AS match_quality,
        NULL AS match,
        NULL AS is_educational,
        NULL AS is_ip,
        NULL AS last_date_host_name_seen,
        NULL AS processed_date
    WHERE FALSE
{%- endif %}