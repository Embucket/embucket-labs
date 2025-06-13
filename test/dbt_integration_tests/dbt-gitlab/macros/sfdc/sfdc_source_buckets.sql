{%- macro sfdc_source_buckets(lead_source) -%}

    CASE
      WHEN {{ lead_source }} in ('CORE Check-Up','Free Registration')
        THEN 'Core'
      WHEN {{ lead_source }} in ('GitLab Subscription Portal', 'Gitlab.com', 'GitLab.com', 'Trial - Gitlab.com', 'Trial - GitLab.com')
        THEN 'GitLab.com'
      WHEN {{ lead_source }} in ('Education', 'OSS')
        THEN 'Marketing/Community'
      WHEN {{ lead_source }} in ('CE Download', 'Demo', 'Drift', 'Email Request', 'Email Subscription', 'Gated Content - General', 'Gated Content - Report', 'Gated Content - Video'
                           , 'Gated Content - Whitepaper', 'Live Event', 'Newsletter', 'Request - Contact', 'Request - Professional Services', 'Request - Public Sector'
                           , 'Security Newsletter', 'Trial - Enterprise', 'Virtual Sponsorship', 'Web Chat', 'Web Direct', 'Web', 'Webcast')
        THEN 'Marketing/Inbound'
      WHEN {{ lead_source }} in ('Advertisement', 'Conference', 'Field Event', 'Gong', 'JiffleNow', 'Owned Event','UserGems Contact Tracking','UserGems - Meeting Assistant','UserGems - New Hires & Promotions')
        THEN 'Marketing/Outbound'
      WHEN {{ lead_source }} in ('Clearbit', 'Datanyze','GovWin IQ', 'Leadware', 'LinkedIn', 'Prospecting - LeadIQ', 'Prospecting - General', 'Prospecting', 'SDR Generated')
        THEN 'Prospecting'
      WHEN {{ lead_source }} in ('Employee Referral', 'External Referral', 'Partner', 'Word of mouth')
        THEN 'Referral'
      WHEN {{ lead_source }} in ('AE Generated')
        THEN 'Sales'
      WHEN {{ lead_source }} in ('DiscoverOrg')
        THEN 'DiscoverOrg'
      ELSE 'Other'
    END                               AS net_new_source_categories,
   CASE
      WHEN {{ lead_source }} like 'Gated Content%'
        THEN 'inbound'
      WHEN {{ lead_source }} in (
          'Contact Request','Demo','Drift','Education','Email Request','Email Subscription','Free Registration',
          'GitLab.com','Newsletter','Non-Profit Application','OSS','Request - Community','Request - Contact',
          'Request - Professional Services','Request - Public Sector','Security Newsletter','Startup Application',
          'Trust Center','Web','Web Chat', 'CE Download'
          )
        THEN 'inbound'
      WHEN {{ lead_source }} in (
          '6sense','AE Generated','Clearbit','Cognism','Datanyze','DiscoverOrg','Gainsight','GovWin IQ','JiffleNow','Leadware',
          'LinkedIn','Prospecting','Prospecting - General','Prospecting - LeadIQ','SDR Generated','seamless.ai',
          'UserGems - New Hires & Promotions','UserGems Contact Tracking','Zoominfo', 'Purchased List' 
          )
        THEN 'outbound'
      WHEN {{ lead_source }} in ('GitLab DataMart')
        THEN 'GitLab DataMart'
      WHEN {{ lead_source }} in (
        'Executive Roundtable', 'Field Event', 'Vendor Arranged Meetings', 'Speaking Session', 'Executive Roundtables',
         'Workshop', 'Owned Event', 'Conference'
      )
        THEN 'event'
      WHEN {{ lead_source }} in ('Advertisement', 'Paid Social', 'Content Syndication')
        THEN 'paid demand gen'
      WHEN {{ lead_source }} in (
        'Event partner', 'Impartner', 'Partner - MDF', 'Partner Qualified Lead', 'Partner',
        'Channel Marketplace'
        )
        THEN 'partner'
      WHEN {{ lead_source }} in ('Word of mouth', 'External Referral', 'Employee Referral', 'Existing Client')
        THEN 'referral'
      WHEN {{ lead_source }} in ('Trial - Enterprise','Trial - GitLab.com')
        THEN 'trial'
      WHEN {{ lead_source }} in ('Webinar', 'CSM Webinar', 'Virtual Sponsorship', 'Webcast', 'Self-Service Virtual Event', 'Sponsored Webcast')
        THEN 'virtual event'
      WHEN {{ lead_source }} in ('GitLab Subscription Portal','Web Direct')
        THEN 'web direct'
      ELSE 'Other'
    END                               AS source_buckets,

{%- endmacro -%}
