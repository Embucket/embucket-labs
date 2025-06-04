

WITH mart_user_request AS (
    
    SELECT *
    FROM {{ ref('mart_user_request') }}
    
), issue_account_summary AS ( -- First we summarise at the issue/epic - crm_account grain
    
    SELECT
        issue_id,
        epic_id,
        user_request_in,
        dim_crm_account_id,

        issue_epic_title,
        issue_epic_url,
        issue_epic_created_at,
        issue_epic_created_date,
        issue_epic_created_month,
        issue_epic_state_name,
        issue_epic_closed_at,
        issue_epic_closed_date,
        issue_epic_closed_month,
        milestone_title,
        milestone_due_date,
        issue_epic_labels,
        deliverable,
        product_group_extended,
        product_group,
        product_category,
        product_stage,
        issue_epic_type,
        issue_status,
        theme_labels,
        epic_status,
        parent_epic_path,
        parent_epic_title,
        upvote_count,
        issue_epic_weight,

        crm_account_name,
        crm_account_next_renewal_month,
        crm_account_health_score_color,
        parent_crm_account_sales_segment,
        technical_account_manager,
        strategic_account_leader,
        customer_reach,
        crm_account_arr,
        crm_account_open_opp_net_arr,
        crm_account_open_opp_net_arr_fo,
        crm_account_open_opp_net_arr_growth,
        opportunity_reach,
        crm_account_lost_opp_net_arr,
        crm_account_lost_customer_arr,
        lost_arr,
        
        SUM(request_priority)                                                   AS sum_request_priority,
        
        SUM(crm_opp_net_arr)                                                    AS sum_linked_crm_opp_net_arr,
        SUM(IFF(crm_opp_is_closed = FALSE, crm_opp_net_arr, 0))                 AS sum_linked_crm_opp_open_net_arr,
        SUM(crm_opp_seats)                                                      AS sum_linked_crm_opp_seats,
        SUM(IFF(crm_opp_is_closed = FALSE, crm_opp_seats, 0))                   AS sum_linked_crm_opp_open_seats,

        ARRAY_AGG(DISTINCT NULLIF(dim_crm_opportunity_id, MD5(-1)))
            WITHIN GROUP (ORDER BY NULLIF(dim_crm_opportunity_id, MD5(-1)))     AS opportunity_id_array,
        ARRAY_AGG(DISTINCT NULLIF(dim_ticket_id, -1))
            WITHIN GROUP (ORDER BY NULLIF(dim_ticket_id, -1))                   AS zendesk_ticket_id_array,
        
       SUM(link_retention_score)                                                AS account_retention_score,
       SUM(link_growth_score)                                                   AS account_growth_score,
       SUM(link_combined_score)                                                 AS account_combined_score,
       SUM(link_priority_score)                                                 AS account_priority_score

    FROM mart_user_request
    {{ dbt_utils.group_by(n=44) }}

), prep_issue_summary AS ( -- Then we summarise at the issue/epic grain

    SELECT
        issue_id,
        epic_id,
        user_request_in,

        issue_epic_title,
        issue_epic_url,
        issue_epic_created_at,
        issue_epic_created_date,
        issue_epic_created_month,
        issue_epic_state_name,
        issue_epic_closed_at,
        issue_epic_closed_date,
        issue_epic_closed_month,
        milestone_title,
        milestone_due_date,
        issue_epic_labels,
        deliverable,
        product_group_extended,
        product_group,
        product_category,
        product_stage,
        issue_epic_type,
        issue_status,
        theme_labels,
        epic_status,
        parent_epic_path,
        parent_epic_title,
        upvote_count,
        issue_epic_weight,

        SUM(sum_request_priority)                                                            AS sum_request_priority,

        -- Account additive fields
        COUNT(DISTINCT dim_crm_account_id)                                                   AS unique_accounts,
        ARRAY_AGG(DISTINCT crm_account_name) WITHIN GROUP (ORDER BY crm_account_name)        AS crm_account_name_array,
        ARRAY_AGG(DISTINCT crm_account_health_score_color) WITHIN GROUP (ORDER BY crm_account_health_score_color)
                                                                                             AS crm_account_health_score_color_array,
        ARRAY_AGG(DISTINCT parent_crm_account_sales_segment) WITHIN GROUP (ORDER BY parent_crm_account_sales_segment)
                                                                                             AS crm_account_parent_sales_segment_array,
        ARRAY_AGG(DISTINCT technical_account_manager) WITHIN GROUP (ORDER BY technical_account_manager)
                                                                                             AS crm_account_tam_array,
        ARRAY_AGG(DISTINCT strategic_account_leader) WITHIN GROUP (ORDER BY strategic_account_leader)
                                                                                             AS crm_account_strategic_account_leader_array,

        SUM(customer_reach)                                                                  AS sum_customer_reach,
        SUM(crm_account_arr)                                                                 AS sum_crm_account_arr,
        SUM(crm_account_open_opp_net_arr)                                                    AS sum_crm_account_open_opp_net_arr,
        SUM(crm_account_open_opp_net_arr_fo)                                                 AS sum_crm_account_open_opp_net_arr_fo,
        SUM(crm_account_open_opp_net_arr_growth)                                             AS sum_crm_account_open_opp_net_arr_growth,
        SUM(opportunity_reach)                                                               AS sum_opportunity_reach,
        SUM(crm_account_lost_opp_net_arr)                                                    AS sum_crm_account_lost_opp_net_arr,
        SUM(crm_account_lost_customer_arr)                                                   AS sum_crm_account_lost_customer_arr,
        SUM(lost_arr)                                                                        AS sum_lost_arr,
    
        -- Opportunity additive fields
        SUM(sum_linked_crm_opp_net_arr)                                                      AS sum_linked_crm_opp_net_arr,
        SUM(sum_linked_crm_opp_open_net_arr)                                                 AS sum_linked_crm_opp_open_net_arr,
        SUM(sum_linked_crm_opp_seats)                                                        AS sum_linked_crm_opp_seats,
        SUM(sum_linked_crm_opp_open_seats)                                                   AS sum_linked_crm_opp_open_seats,

        -- Priority score
        SUM(account_retention_score)                                                         AS retention_score,
        SUM(account_growth_score)                                                            AS growth_score,
        SUM(account_combined_score)                                                          AS combined_score,
        SUM(account_priority_score)                                                          AS priority_score,
        priority_score / NULLIFZERO(issue_epic_weight)                                       AS weighted_priority_score,
        IFF(weighted_priority_score IS NULL,
            '[Effort is Empty, Input Effort Here](' || issue_epic_url || ')',
            weighted_priority_score::TEXT)                                                   AS weighted_priority_score_input

    FROM issue_account_summary
{{ dbt_utils.group_by(n=28)}}

), prep_issue_opp_zendesk_links AS (

    SELECT
        issue_id,
        epic_id,
        COUNT(DISTINCT dim_crm_opportunity_id)                                              AS unique_opportunities,
        COUNT(DISTINCT IFF(crm_opp_is_closed = FALSE, dim_crm_opportunity_id, NULL))        AS unique_open_opportunities,
        ARRAY_AGG(DISTINCT NULLIF(dim_crm_opportunity_id, MD5(-1)))
            WITHIN GROUP (ORDER BY NULLIF(dim_crm_opportunity_id, MD5(-1)))                 AS opportunity_id_array,
        ARRAY_AGG(DISTINCT NULLIF(dim_ticket_id, -1))
            WITHIN GROUP (ORDER BY NULLIF(dim_ticket_id, -1))                               AS zendesk_ticket_id_array
        
    FROM mart_user_request
    GROUP BY 1,2
    
), issue_summary AS (
    
    SELECT
        {{ dbt_utils.generate_surrogate_key(['prep_issue_summary.issue_id', 'prep_issue_summary.epic_id']) }}
                                                                                            AS primary_key,
        prep_issue_summary.*,
        prep_issue_opp_zendesk_links.unique_opportunities,
        prep_issue_opp_zendesk_links.unique_open_opportunities,
        prep_issue_opp_zendesk_links.opportunity_id_array,
        prep_issue_opp_zendesk_links.zendesk_ticket_id_array
    FROM prep_issue_summary
    LEFT JOIN prep_issue_opp_zendesk_links
      ON prep_issue_opp_zendesk_links.issue_id = prep_issue_summary.issue_id
      AND prep_issue_opp_zendesk_links.epic_id = prep_issue_summary.epic_id
    --QUALIFY COUNT(*) OVER(PARTITION BY issue_id, epic_id, dim_crm_account_id) > 1

)


{{ dbt_audit(
    cte_ref="issue_summary",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-12-15",
    updated_date="2023-09-29",
  ) }}
