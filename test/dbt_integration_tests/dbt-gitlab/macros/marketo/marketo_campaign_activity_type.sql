{%- macro marketo_campaign_activity_type(campaign_type,campaign_member_status,campaign_name) -%}



    CASE
      WHEN {{ campaign_type }} = 'Gated Content'
        AND LOWER({{ campaign_member_status }}) IN ('downloaded')
        AND (
          {{ campaign_name }} LIKE '%Gartner%'
          OR {{ campaign_name }} LIKE '%Forrester%'
        )
        THEN 'Gated Content: Analyst Report'
      WHEN {{ campaign_type }} = 'Conference'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'attended on-demand', 'meeting attended')
        THEN 'Conference'
      WHEN LOWER({{ campaign_member_status }}) = 'visited booth'
        THEN 'Conference: Visited Booth'
      WHEN {{ campaign_type }} = 'Content Syndication'
        AND LOWER({{ campaign_member_status }}) = 'downloaded'
        THEN 'Content Syndication'
      WHEN {{ campaign_type }} = 'Executive Roundtables'
        AND LOWER({{ campaign_member_status }}) IN ('attended')
        THEN 'Exec Roundtable'
      WHEN LOWER({{ campaign_member_status }}) IN ('follow up requested')
        AND NOT (
          {{ campaign_type }} IN ('Direct Mail')
          OR {{ campaign_type }} IS NULL
        )
        THEN 'Event Follow Up Requested'
      WHEN ({{ campaign_type }} = 'Gated Content' OR {{ campaign_type }} IS NULL)
        AND LOWER({{ campaign_member_status }}) IN ('downloaded')
        AND NOT (
          {{ campaign_name }} LIKE '%Gartner%'
          OR {{ campaign_name }} LIKE '%Forrester%'
        )
        THEN 'Gated Content: Other'
      WHEN (
        {{ campaign_type }} = 'Inbound Request'
        AND LOWER({{ campaign_member_status }}) IN ('requested contact')
      )
      AND {{ campaign_name }} IN (
        'FY20_Contact_Request', 'Request_Matterhorn Contact Sales',
        'Request - GitLab Dedicated', 'Request_Usage_Limits', 'Request - Upgrade to Ultimate',
        'FY20 - Renewals', 'Request - VSD', 'Request - GitLab Duo Pro', 'FY20_Professional Services',
        '20230115_FY23Q4_EmailPromo - Landing Page', 'FY22_Integrated_PubSec_DI2E Campaign'
      )
        THEN 'Inbound Request: High'
      WHEN {{ campaign_type }} = 'Inbound - offline'
        AND LOWER({{ campaign_member_status }}) IN ('requested contact')
        THEN 'Inbound Request: Offline - PQL'
      WHEN (
        {{ campaign_type }} = 'Inbound Request'
        AND LOWER({{ campaign_member_status }}) IN ('requested contact')
      )
      AND {{ campaign_name }} IN (
        'FY22_Campaign_PartnerMarketplaceOfferings', '20231205_FY24Q4_EmailPromo - LP',
        'Web form - ROI calculator', 'Request - Google S3C and GitLab Security Solution', 'GitLab Subscription Portal',
        'FY22_Campaign_PartnerCloudCreditsPromo'
      )
      OR (
        {{ campaign_type }} = 'Brand'
        AND LOWER({{ campaign_member_status }}) IN ('engaged')
      )
        THEN 'Inbound Request: Medium'
      WHEN (
        {{ campaign_type }} = 'Inbound Request'
        AND LOWER({{ campaign_member_status }}) IN ('requested contact')
      )
      AND {{ campaign_name }} IN (
        'Request - JiHu', 'Request-VSA-WorldTour',
        'Application - Non-profit program', 'WIP_WF_LinkedIn Lead Ads_Test', 'FY20_Startup Application',
        'Request - Reference Program - Customer Reference Leads', 'FY20 - Heros Application',
        'FY20_CommitUserConf_PreLaunch', 'Impartner - Request - Contact', 'Request - Leading Orgs'
      )
        THEN 'Inbound Request: DNI'
      WHEN {{ campaign_type }} = 'Owned Event'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'attended on-demand', 'responded', 'meeting attended')
        THEN 'Owned Event'
      WHEN {{ campaign_type }} = 'Paid Social'
        AND LOWER({{ campaign_member_status }}) IN ('responded', 'downloaded')
        THEN 'Paid Social'
      WHEN LOWER({{ campaign_member_status }}) IN ('registered')
        AND NOT (
          {{ campaign_type }} IN ('Direct Mail', 'Partner - MDF', 'Partners', 'Brand')
          OR {{ campaign_type }} IS NULL
        )
        OR (
          {{ campaign_type }} = 'Conference'
          AND LOWER({{ campaign_member_status }}) IN ('meeting requested')
        )
        THEN 'Event: Registered'
      WHEN {{ campaign_type }} = 'Speaking Session'
        AND LOWER({{ campaign_member_status }}) IN ('attended')
        THEN 'Speaking Session'
      WHEN {{ campaign_type }} = 'Sponsored Webcast'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'attended on-demand')
        THEN 'Sponsored Webcast'
      WHEN {{ campaign_type }} = 'Survey'
        AND LOWER({{ campaign_member_status }}) IN ('filled-out survey')
        AND {{ campaign_name }} ILIKE ANY ('DONOTSCORE', '%google%', '%default%')
        AND NOT {{ campaign_name }} LIKE '%googlecloud%'
        THEN 'Survey: Low'
      WHEN {{ campaign_type }} = 'Survey'
        AND LOWER({{ campaign_member_status }}) IN ('filled-out survey')
        AND {{ campaign_name }} IS NULL
        THEN 'Survey: Medium'
      WHEN {{ campaign_type }} = 'Survey'
        AND LOWER({{ campaign_member_status }}) IN ('filled-out survey')
        THEN 'Survey: High'
      WHEN {{ campaign_type }} = 'Vendor Arranged Meetings'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'meeting attended')
        THEN 'Vendor Meeting'
      WHEN {{ campaign_type }} = 'Webcast'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'attended on-demand')
        AND NOT {{ campaign_name }} LIKE '%_techdemo_%'
        THEN 'Webcast'
      WHEN {{ campaign_type }} = 'Webcast'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'attended on-demand')
        AND {{ campaign_name }} LIKE '%_techdemo_%'
        THEN 'Webcast: Tech Demo'
      WHEN LOWER({{ campaign_member_status }}) = 'sent'
        AND {{ campaign_name }} LIKE '%Qualified%'
        THEN 'Web Chat (Qualified)'
      WHEN LOWER({{ campaign_member_status }}) = 'sent'
        AND {{ campaign_name }} LIKE '%_Drift'
        THEN 'Web Chat (Drift)'
      WHEN {{ campaign_type }} = 'Workshop'
        AND LOWER({{ campaign_member_status }}) IN ('attended', 'visited booth')
        THEN 'Workshop'
      WHEN {{ campaign_type }} = 'Cohort'
        AND LOWER({{ campaign_member_status }}) IN ('organic engaged')
        THEN 'Cohort: Organic Engaged'
      WHEN {{ campaign_type }} = 'Email Send'
        AND LOWER({{ campaign_member_status }}) IN ('clicked in-email link')
        THEN 'Email: Clicked Link'
      WHEN {{ campaign_type }} = 'PF Content'
        AND LOWER({{ campaign_member_status }}) IN ('content consumed')
        THEN 'PF: Content'
      WHEN {{ campaign_type }} = 'PF Content'
        AND LOWER({{ campaign_member_status }}) IN ('fast moving buyer')
        THEN 'PF: Fast Moving Buyer boost'
      WHEN {{ campaign_type }} = 'Prospecting'
        AND LOWER({{ campaign_member_status }}) IN ('filled-out form', 'member', 'responded')
        THEN 'Prospecting'
      WHEN {{ campaign_type }} = 'Self-Service Virtual Event'
        AND LOWER({{ campaign_member_status }}) IN ('attended')
        THEN 'Self Service Virtual Event'
      WHEN {{ campaign_type }} = 'Email Send'
        AND LOWER({{ campaign_member_status }}) IN ('email opened', 'opened', 'member')
        THEN 'Email: Opened'
      WHEN {{ campaign_type }} = 'Nurture'
        AND LOWER({{ campaign_member_status }}) IN ('influenced')
        THEN 'Nurture: Influenced'
      WHEN LOWER({{ campaign_name }})  LIKE '%saas%'
        AND LOWER({{ campaign_name }})  LIKE '%trial%'
        THEN 'Form - SaaS Trial'
    END AS activity_type,
    IFF(activity_type IN ('Conference','Conference: Visited Booth','Content Syndication','Event Follow Up Requested','Event: Registered','Exec Roundtable','Form - SaaS Trial','Gated Content: Analyst Report','Gated Content: Other','Inbound Request: High','Inbound Request: Medium','Inbound Request: Offline - PQL','Owned Event','PF: Content','Self Service Virtual Event','Speaking Session','Sponsored Webcast','Survey: High','Survey: Low','Vendor Meeting','Web Chat (Drift)','Web Chat (Qualified)','Webcast','Webcast: Tech Demo','Workshop'),
    TRUE,
    FALSE) AS in_current_scoring_model 
  

{%- endmacro -%}
