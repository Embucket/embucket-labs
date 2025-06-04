{% macro bizible_touchpoint_offer_type(bizible_touchpoint_type, bizible_ad_campaign_name, bizible_form_url_clean, bizible_marketing_channel, type, pathfactory_content_type) -%}

CASE   
      WHEN {{ bizible_touchpoint_type }} = 'Web Chat' 
        OR LOWER({{ bizible_ad_campaign_name }}) LIKE '%webchat%' 
        OR {{ bizible_ad_campaign_name }} = 'FY24_Qualified.com web conversation'
        THEN 'Web Chat'
      WHEN {{ bizible_touchpoint_type }} IN ('Web Form', 'marketotouchpoin')
        AND {{ bizible_form_url_clean }} IN ('gitlab.com/-/trial_registrations/new',
                                'gitlab.com/-/trial_registrations',
                                'gitlab.com/-/trials/new')
        THEN 'GitLab Dot Com Trial'
      WHEN {{ bizible_touchpoint_type }} IN ('Web Form', 'marketotouchpoin')
        AND {{ bizible_form_url_clean }} IN (
                                'about.gitlab.com/free-trial/index',
                                'about.gitlab.com/free-trial',
                                'about.gitlab.com/free-trial/self-managed',
                                'about.gitlab.com/free-trial/self-managed/index',
                                'about.gitlab.com/fr-fr/free-trial',
                                'about.gitlab.com/ja-jp/free-trial'
                                )
        THEN 'GitLab Self-Managed Trial'
      WHEN  {{ bizible_form_url_clean }} LIKE '%/sign_up%' 
        OR {{ bizible_form_url_clean }} LIKE '%/users%' 
        THEN 'Sign Up Form'
      WHEN {{ bizible_form_url_clean }} IN ('about.gitlab.com/sales', 
        'about.gitlab.com/ja-jp/sales',
        'about.gitlab.com/de-de/sales',
        'about.gitlab.com/fr-fr/sales',
        'about.gitlab.com/es/sales',
        'about.gitlab.com/dedicated',
        'about.gitlab.com/pt-br/sales'
        )
        OR {{ bizible_form_url_clean }} LIKE 'about.gitlab.com/pricing/smb%' 
        THEN 'Contact Sales Form' 
      WHEN {{ bizible_form_url_clean }} LIKE '%/resources/%' 
        THEN 'Resources'
      WHEN {{ bizible_touchpoint_type }} IN ('Web Form', 'marketotouchpoin') 
        AND 
          (
            ({{ bizible_form_url_clean }} LIKE '%page.gitlab.com%') 
            OR ({{ bizible_form_url_clean }} LIKE '%about.gitlab.com/gartner%%')
          )
        THEN 'Online Content' 
      WHEN {{ bizible_form_url_clean }} LIKE 'learn.gitlab.com%' 
        THEN 'Online Content'
      WHEN {{ bizible_marketing_channel }} = 'Event' 
        THEN {{ type }} 
      WHEN (LOWER({{ bizible_ad_campaign_name }}) LIKE '%lead%' 
        AND LOWER({{ bizible_ad_campaign_name }}) LIKE '%linkedin%')
        OR
        (
          {{ type }}  = 'Paid Social'
          -- they are all ABM LinkedIN Lead Gen but the name doesn't say that.
          AND ({{ bizible_ad_campaign_name }} LIKE '%2023_ABM%' 
            OR {{ bizible_ad_campaign_name }} LIKE '%2024_ABM%')
        ) 
        THEN 'Lead Gen Form'
      WHEN {{ bizible_marketing_channel }} = 'IQM' 
        THEN 'IQM'
      WHEN {{ bizible_form_url_clean }} LIKE '%/education/%' 
        OR LOWER({{ bizible_ad_campaign_name }}) LIKE '%education%' 
        THEN 'Education'
      WHEN {{ bizible_form_url_clean }} LIKE '%/company/%' 
        THEN 'Company Pages'
      WHEN {{ bizible_touchpoint_type }} = 'CRM' 
        AND (LOWER({{ bizible_ad_campaign_name }}) LIKE '%partner%' 
          OR LOWER(bizible_medium) LIKE '%partner%')
        THEN 'Partner Sourced'  
      WHEN {{ bizible_form_url_clean }} ='about.gitlab.com/company/contact/' 
        OR {{ bizible_form_url_clean }} LIKE '%/releases/%' 
        OR {{ bizible_form_url_clean }} LIKE '%/blog/%' 
        OR {{ bizible_form_url_clean }} LIKE '%/community/%' 
        THEN 'Newsletter/Release/Blog Sign-Up'
      WHEN {{ type }}  = 'Survey' 
        OR {{ bizible_form_url_clean }} LIKE '%survey%' 
          OR LOWER({{ bizible_ad_campaign_name }}) LIKE '%survey%' 
        THEN 'Survey'
      WHEN {{ bizible_form_url_clean }} LIKE '%/solutions/%' 
        OR {{ bizible_form_url_clean }} IN ('about.gitlab.com/enterprise/','about.gitlab.com/small-business/') 
        THEN 'Solutions' 
      WHEN {{ bizible_form_url_clean }} LIKE '%/blog%'
        THEN 'Blog'
      WHEN {{ bizible_form_url_clean }} LIKE '%/webcast/%' 
        THEN 'Webcast'
      WHEN {{ bizible_form_url_clean }} LIKE '%/services%'  
        THEN 'GitLab Professional Services' 
      WHEN {{ bizible_form_url_clean }} LIKE '%/fifteen%'  
        THEN  'Event Registration' 
      WHEN {{ bizible_ad_campaign_name }} LIKE '%PQL%' 
        THEN 'Product Qualified Lead'
      WHEN {{ bizible_marketing_channel }}_path LIKE '%Content Syndication%' 
        THEN 'Content Syndication'
      WHEN ({{ bizible_form_url_clean }} LIKE '%about.gitlab.com/events%')
        THEN 'Event Registration' 
      ELSE 'Other'
    END AS touchpoint_offer_type_wip,
    CASE 
      WHEN {{ pathfactory_content_type }} IS NOT NULL 
        THEN {{ pathfactory_content_type }}
      WHEN (touchpoint_offer_type_wip = 'Online Content' 
        AND {{ bizible_form_url_clean }} LIKE '%registration-page%'
        OR {{ bizible_form_url_clean }} LIKE '%registrationpage%'
        OR {{ bizible_form_url_clean }} LIKE '%inperson%'
        OR {{ bizible_form_url_clean }} LIKE '%in-person%'
        OR {{ bizible_form_url_clean }} LIKE '%landing-page%')
        OR touchpoint_offer_type_wip = 'Event Registration' 
        THEN 'Event Registration'
      WHEN touchpoint_offer_type_wip = 'Online Content' 
        AND {{ bizible_form_url_clean }} LIKE '%ebook%'
        THEN 'eBook'
      WHEN (touchpoint_offer_type_wip = 'Online Content'
        AND {{ bizible_form_url_clean }} LIKE '%webcast%')
        OR {{ bizible_form_url_clean }} IN ('about.gitlab.com/seventeen', 
                                      'about.gitlab.com/sixteen', 
                                      'about.gitlab.com/eighteen',
                                      'about.gitlab.com/nineteen',
                                      'about.gitlab.com/fr-fr/seventeen',
                                      'about.gitlab.com/de-de/seventeen',
                                      'about.gitlab.com/es/seventeen')
        THEN 'Webcast'
      WHEN touchpoint_offer_type_wip = 'Online Content' 
        AND {{ bizible_form_url_clean }} LIKE '%demo%' 
        THEN 'Demo'
      WHEN touchpoint_offer_type_wip = 'Online Content'
        THEN 'Other Online Content'
      ELSE touchpoint_offer_type_wip
    END AS touchpoint_offer_type,
    CASE
      WHEN {{ bizible_marketing_channel }} = 'Event' 
        OR touchpoint_offer_type = 'Event Registration' 
          OR touchpoint_offer_type = 'Webcast' 
          OR touchpoint_offer_type = 'Workshop' 
        THEN 'Events'
      WHEN touchpoint_offer_type_wip = 'Online Content' 
        THEN 'Online Content'
      WHEN touchpoint_offer_type IN ('Content Syndication','Newsletter/Release/Blog Sign-Up','Blog','Resources')
        THEN 'Online Content'
      WHEN touchpoint_offer_type = 'Sign Up Form' 
        THEN 'Sign Up Form'
      WHEN touchpoint_offer_type IN ('GitLab Dot Com Trial', 'GitLab Self-Managed Trial')
        THEN 'Trials'
      WHEN touchpoint_offer_type = 'Web Chat' 
        THEN 'Web Chat'
      WHEN touchpoint_offer_type = 'Contact Sales Form' 
        THEN 'Contact Sales Form'
      WHEN touchpoint_offer_type = 'Partner Sourced' 
        THEN 'Partner Sourced'
      WHEN touchpoint_offer_type = 'Lead Gen Form' 
        THEN 'Lead Gen Form'
      ELSE 'Other'
    END AS touchpoint_offer_type_grouped

{% endmacro %}