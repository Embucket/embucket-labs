WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'campaign') }}



), renamed AS(

    SELECT
        id                                                                  AS campaign_id,
        name                                                                AS campaign_name,
        isactive                                                            AS is_active,
        startdate                                                           AS start_date,
        enddate                                                             AS end_date,
        status                                                              AS status,
        IFF(type LIKE 'Field Event%', 'Field Event', type)                  AS type,

        --keys
        campaignmemberrecordtypeid                                          AS campaign_member_record_type_id,
        ownerid                                                             AS campaign_owner_id,
        parentid                                                            AS campaign_parent_id,

        --info
        description                                                         AS description,
        region__c                                                           AS region,
        sub_region__c                                                       AS sub_region,
        budget_holder__c                                                    AS budget_holder,
        will_there_be_mdf_funding__c                                        AS will_there_be_mdf_funding,
        mdf_request__c                                                      AS mdf_request_id,
        vartopiadrs__partner_account__c                                     AS campaign_partner_crm_id,

        --projections
        budgetedcost                                                        AS budgeted_cost,
        expectedresponse                                                    AS expected_response,
        expectedrevenue                                                     AS expected_revenue,
        bizible2__bizible_attribution_synctype__c                           AS bizible_touchpoint_enabled_setting,
        allocadia_id__c                                                     AS allocadia_id,
        is_a_channel_partner_involved__c                                    AS is_a_channel_partner_involved,
        is_an_alliance_partner_involved__c                                  AS is_an_alliance_partner_involved,
        in_person_virtual__c                                                AS is_this_an_in_person_event,
        alliance_partner_name__c                                            AS alliance_partner_name,
        channel_partner_name__c                                             AS channel_partner_name,
        sales_play__c                                                       AS sales_play,
        gtm_motion__c                                                       AS gtm_motion,
        total_planned_mql__c                                                AS total_planned_mqls,
        registration_goal__c                                                AS registration_goal,
        attendance_goal__c                                                  AS attendance_goal,

        --planned results
        planned_inquiry__c                                                  AS planned_inquiry,
        planned_mql__c                                                      AS planned_mql,
        planned_pipeline__c                                                 AS planned_pipeline,
        planned_sao__c                                                      AS planned_sao,
        planned_won__c                                                      AS planned_won,
        total_planned_mql__c                                                AS total_planned_mql,
        pipeline_roi__c                                                     AS planned_roi,
        
        --results
        actualcost                                                          AS actual_cost,
        amountallopportunities                                              AS amount_all_opportunities,
        amountwonopportunities                                              AS amount_won_opportunities,
        numberofcontacts                                                    AS count_contacts,
        numberofconvertedleads                                              AS count_converted_leads,
        numberofleads                                                       AS count_leads,
        numberofopportunities                                               AS count_opportunities,
        numberofresponses                                                   AS count_responses,
        numberofwonopportunities                                            AS count_won_opportunities,
        numbersent                                                          AS count_sent,
        strat_contribution__c                                               AS strategic_marketing_contribution,
        large_bucket__c                                                     AS large_bucket,
        NULL                                                                AS reporting_type,

        --metadata
        createddate                                                         AS created_date,
        createdbyid                                                         AS created_by_id,
        lastmodifiedbyid                                                    AS last_modified_by_id,
        lastmodifieddate                                                    AS last_modified_date,
        lastactivitydate                                                    AS last_activity_date,
        systemmodstamp,

        isdeleted                                                           AS is_deleted

    FROM source
)

SELECT *
FROM renamed
