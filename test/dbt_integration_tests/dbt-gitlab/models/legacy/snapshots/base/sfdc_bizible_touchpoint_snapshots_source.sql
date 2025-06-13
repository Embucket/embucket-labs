WITH source AS (
    SELECT *
    FROM {{ source('snapshots', 'sfdc_bizible_touchpoint_snapshots') }}

), renamed AS (

    SELECT
      id                                      AS touchpoint_id,
      bizible2__bizible_person__c             AS bizible_person_id,

      -- sfdc object lookups
      bizible2__sf_campaign__c                AS campaign_id,
      bizible2__contact__c                    AS bizible_contact,
      bizible2__account__c                    AS bizible_account,      
      
      -- attribution counts
      bizible2__count_first_touch__c          AS bizible_count_first_touch,
      bizible2__count_lead_creation_touch__c  AS bizible_count_lead_creation_touch,
      bizible2__count_u_shaped__c             AS bizible_count_u_shaped,

      -- touchpoint info
      bizible2__touchpoint_date__c            AS bizible_touchpoint_date,
      bizible2__touchpoint_position__c        AS bizible_touchpoint_position,
      bizible2__touchpoint_source__c          AS bizible_touchpoint_source,
      source_type__c                          AS bizible_touchpoint_source_type,
      bizible2__touchpoint_type__c            AS bizible_touchpoint_type,      
      bizible2__ad_campaign_name__c           AS bizible_ad_campaign_name,
      bizible2__ad_content__c                 AS bizible_ad_content,
      bizible2__ad_group_name__c              AS bizible_ad_group_name,
      bizible2__form_url__c                   AS bizible_form_url,
      bizible2__form_url_raw__c               AS bizible_form_url_raw,
      bizible2__landing_page__c               AS bizible_landing_page,
      bizible2__landing_page_raw__c           AS bizible_landing_page_raw,
      bizible2__marketing_channel__c          AS bizible_marketing_channel,
      bizible2__marketing_channel_path__c     AS bizible_marketing_channel_path,
      bizible2__medium__c                     AS bizible_medium, 
      bizible2__referrer_page__c              AS bizible_referrer_page,   
      bizible2__referrer_page_raw__c          AS bizible_referrer_page_raw, 
      bizible2__sf_campaign__c                AS bizible_salesforce_campaign,
      utm_budget__c                           AS utm_budget,
      utm_offersubtype__c                     AS utm_offersubtype,
      utm_offertype__c                        AS utm_offertype,
      utm_targetregion__c                     AS utm_targetregion,
      utm_targetsubregion__c                  AS utm_targetsubregion,
      utm_targetterritory__c                  AS utm_targetterritory,
      utm_usecase__c                          AS utm_usecase,
      CASE 
        WHEN SPLIT_PART(SPLIT_PART(bizible_form_url_raw,'utm_content=',2),'&',1)IS null
          THEN SPLIT_PART(SPLIT_PART(bizible_landing_page_raw,'utm_content=',2),'&',1)
        ELSE SPLIT_PART(SPLIT_PART(bizible_form_url_raw,'utm_content=',2),'&',1) 
      END AS utm_content,

      isdeleted                               AS is_deleted,

      -- snapshot metadata
      dbt_scd_id,
      dbt_updated_at,
      dbt_valid_from,
      dbt_valid_to
    
    FROM source
)

SELECT *
FROM renamed