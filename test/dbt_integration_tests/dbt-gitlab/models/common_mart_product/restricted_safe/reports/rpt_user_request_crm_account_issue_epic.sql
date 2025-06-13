

WITH mart_user_request AS (
    
    SELECT *
    FROM {{ ref('mart_user_request') }}
    
), issue_account_summary AS (
    
    SELECT
        {{ dbt_utils.generate_surrogate_key(['issue_id', 'epic_id', 'dim_crm_account_id']) }} AS primary_key,
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
       SUM(link_priority_score)                                                 AS account_priority_score,
       account_priority_score / NULLIFZERO(issue_epic_weight)                   AS account_weighted_priority_score,
       IFF(account_weighted_priority_score IS NULL,
        '[Effort is Empty, Input Effort Here](' || issue_epic_url || ')',
        account_weighted_priority_score::TEXT)                                  AS account_weighted_priority_score_input

    FROM mart_user_request
    {{ dbt_utils.group_by(n=45) }}

)

{{ dbt_audit(
    cte_ref="issue_account_summary",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-12-15",
    updated_date="2023-09-29",
  ) }}
