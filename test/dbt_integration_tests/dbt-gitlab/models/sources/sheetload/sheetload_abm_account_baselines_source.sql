WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'abm_account_baselines') }}

), renamed AS (

    SELECT
      "Date_Added"::DATE                                    AS added_date,
      "Account_ID"::VARCHAR                                 AS account_id,
      "GTM_Strategy"::VARCHAR                               AS gtm_strategy,
      NULLIF("ABM_Tier", '')::VARCHAR                       AS abm_tier,
      NULLIF("Account_Name", '')::VARCHAR                   AS account_name,
      NULLIF("Parent_Account", '')::VARCHAR                 AS parent_account,
      NULLIF("Website", '')::VARCHAR                        AS website,
      NULLIF("Domains", '')::VARCHAR	                    AS domains,
      NULLIF("Type", '')::VARCHAR                           AS type,
      NULLIF("Count_of_Contacts", '')::FLOAT	            AS count_of_contacts,
      NULLIF("Health_Score", '')::VARCHAR                   AS health_score,
      NULLIF("Count_of_Opportunities", '')::FLOAT           AS count_of_opportunities,
      NULLIF("Number_of_Open_Opportunities", '')::FLOAT	    AS number_of_open_opportunities,
      NULLIF("Count_of_Won_Opportunities", '')::FLOAT       AS count_of_won_opportunities,
      NULLIF("Total_Closed_Won_Amount_(All-Time)", '')::FLOAT
                                                            AS total_closed_won_amount,
      NULLIF("Sum:_Open_New_Add-on_IACV_Opportunities", '')::FLOAT
                                                            AS open_new_add_on_iacv,
      NULLIF("Sum_of_Open_Renewal_Opportunities", '')::FLOAT
                                                            AS sum_open_renewal_opportunities,
      NULLIF("Support_Level", '')::VARCHAR                  AS support_level,
      NULLIF("GitLab.com_user", '')::FLOAT                  AS gitlab_com_user,
      NULLIF("GitLab_EE_Customer", '')::FLOAT               AS gitlab_ee_customer,
      NULLIF("EE_Basic_Customer", '')::FLOAT                AS ee_basic_customer,
      NULLIF("EE_Standard_Customer", '')::FLOAT             AS ee_standard_customer,
      NULLIF("EE_Plus_Customer", '')::FLOAT                 AS ee_plus_customer,
      NULLIF("Concurrent_EE_Subscriptions", '')::FLOAT      AS concurrent_ee_subscriptions,
      NULLIF("Count_of_Active_Subscriptions", '')::FLOAT    AS count_active_subscriptions,
      NULLIF("Using_CE", '')::FLOAT                         AS using_ce,
      NULLIF("CE_Instances", '')::FLOAT                     AS ce_instances,
      NULLIF("Active_CE_Users", '')::FLOAT                  AS active_ce_users,
      NULLIF("DemandBase:_Score", '')::VARCHAR              AS demandbase_score,
      NULLIF("DemandBase:_Account_List", '')::VARCHAR       AS demandbase_account_list,
      NULLIF("DemandBase:_Intent", '')::VARCHAR             AS demandbase_intent,
      NULLIF("DemandBase:_Page_Views", '')::FLOAT           AS demandbase_page_views,
      NULLIF("DemandBase:_Sessions", '')::FLOAT             AS demandbase_sessions,
      NULLIF("DemandBase:_Trending_Onsite_Engagement", '')::FLOAT
                                                            AS demandbase_trending_onsite_engagement,
      NULLIF("DemandBase:_Trending_Offsite_Intent", '')::FLOAT
                                                            AS demandbase_trending_offsite_intent,
      NULLIF("Account_Owner", '')::VARCHAR                  AS account_owner,
      NULLIF("Billing_State_Province", '')::VARCHAR         AS billing_state_province

    FROM source

)

SELECT *
FROM renamed
