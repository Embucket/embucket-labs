{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "legacy"
  })
}}

with sfdc_opportunity as (

    SELECT distinct lead_source FROM {{ref('sfdc_opportunity')}}

),sfdc_lead as (

    SELECT distinct lead_source FROM {{ref('sfdc_lead')}}

), sfdc_contact as (

    SELECT distinct lead_source FROM {{ref('sfdc_contact')}}

), base as (

    SELECT * FROM sfdc_opportunity
    UNION ALL
    SELECT * FROM sfdc_lead
    UNION ALL 
    SELECT * FROM sfdc_contact

), lead_sources as (

    SELECT distinct lower(lead_source) AS lead_source
    FROM base
    WHERE lead_source IS NOT NULL

)

SELECT row_number() OVER (ORDER BY lead_source) AS lead_source_id,
        lead_source as initial_source,
        CASE WHEN lead_source IN('advertisement')
            THEN 'Advertising'
           WHEN lead_source LIKE '%email%' OR lead_source LIKE '%newsletter%'
            THEN 'Email'
           WHEN lead_source LIKE '%event%' 
            OR lead_source LIKE '%conference%'
            OR lead_source LIKE '%seminar%'
            THEN 'Events'
           WHEN lead_source IN ('Contact Request', 'Enterprise Trial', 'Development Request', 
                                'Prof Serv Request', 'Web', 'Webcast', 'Web Chat', 'Web Direct', 
                                'White Paper', 'Training Request', 'Consultancy Request', 
                                'Public Relations')
            THEN 'Marketing Site'
           WHEN lead_source IN ('SDR Generated', 'Linkedin', 'LeadWare', 'AE Generated', 
                                'Datanyze', 'DiscoverOrg', 'Clearbit','GovWin IQ')
            THEN 'Prospecting'
           WHEN lead_source IN ('Gitorious', 'GitLab Hosted', 'GitLab EE instance', 
                                'GitLab.com', 'CE Download', 'CE Usage Ping')
            THEN 'Product'
           WHEN lead_source IN ('Word of Mouth', 'External Referral', 'Employee Referral', 
                                'Partner', 'Existing Client')
            THEN 'Referral'
           ELSE 'Other'
        END as initial_source_type  
FROM lead_sources