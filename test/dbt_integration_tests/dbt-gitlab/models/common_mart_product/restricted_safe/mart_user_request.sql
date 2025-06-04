

{{ simple_cte([
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels'),
    ('bdg_epic_user_request', 'bdg_epic_user_request'),
    ('bdg_issue_user_request', 'bdg_issue_user_request'),
    ('dim_epic', 'dim_epic'),
    ('dim_issue', 'dim_issue'),
    ('fct_mrr', 'fct_mrr'),
    ('dim_date', 'dim_date'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_subscription', 'dim_subscription'),
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('dim_crm_user', 'dim_crm_user'),
    ('fct_quote_item', 'fct_quote_item'),
    ('dim_quote', 'dim_quote'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_order_type', 'dim_order_type'),
    ('dim_crm_user', 'dim_crm_user')
])}}

, opportunity_seats AS (

    SELECT
      dim_crm_opportunity.dim_crm_opportunity_id,
      dim_crm_opportunity.dim_crm_account_id,
      dim_crm_opportunity.stage_name,
      fct_crm_opportunity.is_closed,
      dim_crm_opportunity.order_type,
      SUM(fct_quote_item.quantity)              AS quantity
    FROM fct_quote_item
    INNER JOIN dim_crm_opportunity
      ON dim_crm_opportunity.dim_crm_opportunity_id = fct_quote_item.dim_crm_opportunity_id
    INNER JOIN fct_crm_opportunity
      ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    INNER JOIN dim_quote
      ON dim_quote.dim_quote_id = fct_quote_item.dim_quote_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_quote_item.dim_product_detail_id
    WHERE dim_quote.is_primary_quote = TRUE
      AND dim_product_detail.product_tier_name IN ('Plus', 'GitHost', 'Standard', 'Self-Managed - Starter', 'Self-Managed - Premium',
        'SaaS - Premium', 'SaaS - Bronze', 'Basic', 'Self-Managed - Ultimate', 'SaaS - Ultimate', 'Dedicated - Ultimate')
      AND fct_crm_opportunity.close_date >= '2019-02-01'
    {{ dbt_utils.group_by(5) }}

), account_open_fo_opp_seats AS (

    SELECT
      dim_crm_account_id,
      SUM(quantity) AS seats
    FROM opportunity_seats
    WHERE order_type = '1. New - First Order'
      AND is_closed = FALSE
    GROUP BY 1

), opportunity_net_arr AS (

    SELECT
      fct_crm_opportunity.dim_crm_opportunity_id,
      fct_crm_opportunity.dim_crm_account_id,
      dim_crm_opportunity.stage_name,
      fct_crm_opportunity.is_closed,
      dim_order_type.order_type_name,
      fct_crm_opportunity.net_arr,
      fct_crm_opportunity.arr_basis
    FROM fct_crm_opportunity
    INNER JOIN dim_order_type
      ON dim_order_type.dim_order_type_id = fct_crm_opportunity.dim_order_type_id
    INNER JOIN dim_crm_opportunity
      ON dim_crm_opportunity.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
    WHERE fct_crm_opportunity.close_date >= '2019-02-01' -- Net ARR is only good after 2019-02-01

), account_lost_opp_arr AS (

    SELECT
      dim_crm_account_id,
      SUM(net_arr) AS net_arr
    FROM opportunity_net_arr
    WHERE order_type_name IN ('1. New - First Order')
      AND stage_name IN ('8-Closed Lost')
    GROUP BY 1
  
), account_lost_customer_arr AS (

    SELECT
      dim_crm_account_id,
      SUM(arr_basis)  AS arr_basis
    FROM opportunity_net_arr
    WHERE order_type_name IN ('6. Churn - Final')
      AND stage_name IN ('8-Closed Lost')
    GROUP BY 1

), account_open_opp_net_arr AS (

    SELECT
      dim_crm_account_id,
      SUM(net_arr) AS net_arr
    FROM opportunity_net_arr
    WHERE is_closed = FALSE
    GROUP BY 1 

), account_open_opp_net_arr_fo AS (

    SELECT
      dim_crm_account_id,
      SUM(net_arr) AS net_arr
    FROM opportunity_net_arr
    WHERE is_closed = FALSE
      AND order_type_name IN ('1. New - First Order')
    GROUP BY 1 

), account_open_opp_net_arr_growth AS (

    SELECT
      dim_crm_account_id,
      SUM(net_arr) AS net_arr
    FROM opportunity_net_arr
    WHERE is_closed = FALSE
      AND order_type_name IN ('2. New - Connected', '3. Growth')
    GROUP BY 1 

), account_next_renewal_month AS (

    SELECT
      fct_mrr.dim_crm_account_id,
      MIN(subscription_end_month) AS next_renewal_month
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    WHERE dim_subscription.subscription_end_month >= DATE_TRUNC('month',CURRENT_DATE)
      AND fct_mrr.subscription_status IN ('Active', 'Cancelled')
    GROUP BY 1

), arr_metrics_current_month AS (

    SELECT
      fct_mrr.dim_crm_account_id,
      SUM(fct_mrr.mrr)                                                               AS mrr,
      SUM(fct_mrr.arr)                                                               AS arr,
      SUM(fct_mrr.quantity)                                                          AS quantity
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    WHERE subscription_status IN ('Active', 'Cancelled')
      AND dim_date.date_actual = DATE_TRUNC('month', CURRENT_DATE)
      AND dim_product_detail.product_tier_name IN ('Plus', 'GitHost', 'Standard', 'Self-Managed - Starter', 'Self-Managed - Premium',
        'SaaS - Premium', 'SaaS - Bronze', 'Basic', 'Self-Managed - Ultimate', 'SaaS - Ultimate', 'Dedicated - Ultimate')
    GROUP BY 1

), epic_weight AS (

    SELECT
      dim_epic_sk,
      SUM(weight)                                                               AS epic_weight,
      SUM(IFF(issue_state = 'closed', weight, 0)) / NULLIFZERO(epic_weight)     AS epic_completeness,
      SUM(IFF(issue_state = 'closed', 1, 0)) / COUNT(*)                         AS epic_completeness_alternative,
      COALESCE(epic_completeness, epic_completeness_alternative)                AS epic_status
    FROM dim_issue
    GROUP BY 1
    
), label_links_joined AS (

    SELECT
      dim_issue.dim_issue_sk,
      prep_label_links.*,
      prep_labels.label_title
    FROM prep_label_links
    LEFT JOIN prep_labels
      ON prep_label_links.dim_label_id = prep_labels.dim_label_id
    LEFT JOIN dim_issue
      ON prep_label_links.issue_id = dim_issue.issue_id

), issue_labels AS (

    SELECT 
      label_links_joined.dim_issue_sk,
      IFF(LOWER(label_links_joined.label_title) LIKE 'group::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)    AS group_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'devops::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)   AS devops_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'section::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)  AS section_label,
      COALESCE(group_label, devops_label, section_label)                                                                       AS product_group_extended,

      IFF(LOWER(label_links_joined.label_title) LIKE 'category:%', SPLIT_PART(label_links_joined.label_title, ':', 2), NULL)   AS category_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'type::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)     AS type_label,

      IFF(LOWER(label_links_joined.label_title) LIKE 'theme:%'
        AND NOT CONTAINS(label_links_joined.label_title, '::'), -- This second condition makes sure we are not matching the scoped label theme::*, only the non scoped theme:*
          SPLIT_PART(label_links_joined.label_title, ':', 2), NULL)                                                             AS theme_label,
      CASE
        WHEN group_label IS NOT NULL THEN 3
        WHEN devops_label IS NOT NULL THEN 2
        WHEN section_label IS NOT NULL THEN 1
        ELSE 0
      END product_group_level
    FROM label_links_joined

), epic_labels AS (

    SELECT 
      dim_epic.dim_epic_sk,
      IFF(LOWER(label_links_joined.label_title) LIKE 'group::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)    AS group_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'devops::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)   AS devops_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'section::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)  AS section_label,
      COALESCE(group_label, devops_label, section_label)                                                                       AS product_group_extended,

      IFF(LOWER(label_links_joined.label_title) LIKE 'category:%', SPLIT_PART(label_links_joined.label_title, ':', 2), NULL)   AS category_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'type::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)     AS type_label,

      IFF(LOWER(label_links_joined.label_title) LIKE 'theme:%'
        AND NOT CONTAINS(label_links_joined.label_title, '::'), -- This second condition makes sure we are not matching the scoped label theme::*, only the non scoped theme:*
          SPLIT_PART(label_links_joined.label_title, ':', 2), NULL)                                                             AS theme_label,

      CASE
        WHEN group_label IS NOT NULL THEN 3
        WHEN devops_label IS NOT NULL THEN 2
        WHEN section_label IS NOT NULL THEN 1
        ELSE 0
      END product_group_level
    FROM label_links_joined
    LEFT JOIN dim_epic
      ON label_links_joined.epic_id = dim_epic.epic_id

), issue_theme_labels AS (

    SELECT
      dim_issue_sk,
      ARRAY_AGG(theme_label) WITHIN GROUP (ORDER BY theme_label) AS theme_labels
    FROM issue_labels
    GROUP BY 1

), epic_theme_labels AS (

    SELECT
      dim_epic_sk,
      ARRAY_AGG(theme_label) WITHIN GROUP (ORDER BY theme_label) AS theme_labels
    FROM epic_labels
    GROUP BY 1

), issue_group_label AS ( -- There is a bug in the product where some scoped labels are used twice. This is a temporary fix for that for the group::* label

    SELECT
      dim_issue_sk,
      group_label
    FROM issue_labels
    WHERE group_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_issue_sk ORDER BY group_label) = 1

), issue_group_extended_label AS (

    SELECT
      dim_issue_sk,
      product_group_extended
    FROM issue_labels
    WHERE product_group_extended IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_issue_sk ORDER BY product_group_level DESC) = 1

), issue_category_dedup AS ( -- Since category: is not an scoped label, need to make sure I only pull one of them
  
    SELECT
      dim_issue_sk,
      category_label
    FROM issue_labels
    WHERE category_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_issue_sk ORDER BY category_label DESC) = 1
  
), issue_type_label AS ( -- There is a bug in the product where some scoped labels are used twice. This is a temporary fix for that for the type::* label

    SELECT
      dim_issue_sk,
      type_label
    FROM issue_labels
    WHERE type_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_issue_sk ORDER BY type_label) = 1

), issue_devops_label AS ( -- There is a bug in the product where some scoped labels are used twice. This is a temporary fix for that for the devops::* label

    SELECT
      dim_issue_sk,
      devops_label
    FROM issue_labels
    WHERE devops_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_issue_sk ORDER BY devops_label) = 1

), issue_status AS ( -- Some issues for some reason had two valid workflow labels, this dedup them

    SELECT
      label_links_joined.dim_issue_sk,
      IFF(LOWER(label_links_joined.label_title) LIKE 'workflow::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)   AS workflow_label
    FROM label_links_joined
    WHERE workflow_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_issue_sk ORDER BY workflow_label DESC) = 1

), epic_group_label AS ( -- There is a bug in the product where some scoped labels are used twice. This is a temporary fix for that for the group::* label

    SELECT
      dim_epic_sk,
      group_label
    FROM epic_labels
    WHERE group_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_sk ORDER BY group_label) = 1

), epic_group_extended_label AS (

    SELECT
      dim_epic_sk,
      product_group_extended
    FROM epic_labels
    WHERE product_group_extended IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_sk ORDER BY product_group_level DESC) = 1

), epic_category_dedup AS ( -- Since category: is not an scoped label, need to make sure I only pull one of them
  
    SELECT
      dim_epic_sk,
      category_label
    FROM epic_labels
    WHERE category_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_sk ORDER BY category_label DESC) = 1
  
), epic_type_label AS ( -- There is a bug in the product where some scoped labels are used twice. This is a temporary fix for that for the type::* label

    SELECT
      dim_epic_sk,
      type_label
    FROM epic_labels
    WHERE type_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_sk ORDER BY type_label) = 1

), epic_devops_label AS ( -- There is a bug in the product where some scoped labels are used twice. This is a temporary fix for that for the devops::* label

    SELECT
      dim_epic_sk,
      devops_label
    FROM epic_labels
    WHERE devops_label IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_sk ORDER BY devops_label) = 1

), epic_last_milestone AS ( -- Get issue milestone with the latest due dates for epics
    
    SELECT
      dim_epic_sk,
      milestone_title,
      milestone_due_date
    FROM dim_issue
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_sk ORDER BY milestone_due_date DESC NULLS LAST) = 1

), user_request AS (

    SELECT
      bdg_issue_user_request.issue_id                                             AS issue_id,
      IFNULL(dim_epic.epic_id, -1)                                                AS epic_id,
      'Issue'                                                                     AS user_request_in,

      bdg_issue_user_request.link_type                                            AS link_type,
      bdg_issue_user_request.dim_crm_opportunity_id                               AS dim_crm_opportunity_id,
      bdg_issue_user_request.dim_crm_account_id                                   AS dim_crm_account_id,
      bdg_issue_user_request.dim_ticket_id                                        AS dim_ticket_id,
      bdg_issue_user_request.request_priority                                     AS request_priority,
      bdg_issue_user_request.is_request_priority_empty                            AS is_request_priority_empty,
      bdg_issue_user_request.is_user_request_only_in_collaboration_project        AS is_user_request_only_in_collaboration_project,
      bdg_issue_user_request.link_last_updated_at                                 AS link_last_updated_at,
      bdg_issue_user_request.link_last_updated_at::DATE                           AS link_last_updated_date,
      DATE_TRUNC('month', bdg_issue_user_request.link_last_updated_at::DATE)      AS link_last_updated_month,

      IFF(link_type = 'Opportunity', 'https://gitlab.my.salesforce.com/' || bdg_issue_user_request.dim_crm_opportunity_id, 'No Link')
                                                                                  AS crm_opportunity_link,
      'https://gitlab.my.salesforce.com/' || bdg_issue_user_request.dim_crm_account_id
                                                                                  AS crm_account_link,
      IFF(link_type = 'Zendesk Ticket', 'https://gitlab.zendesk.com/agent/tickets/' || bdg_issue_user_request.dim_ticket_id, 'No Link')
                                                                                  AS ticket_link,

      -- Epic / Issue attributes
      dim_issue.issue_title                                                       AS issue_epic_title,
      dim_issue.issue_url                                                         AS issue_epic_url,
      dim_issue.created_at                                                        AS issue_epic_created_at,
      dim_issue.created_at::DATE                                                  AS issue_epic_created_date,
      DATE_TRUNC('month', dim_issue.created_at::DATE)                             AS issue_epic_created_month,
      dim_issue.issue_state                                                       AS issue_epic_state_name,
      dim_issue.issue_closed_at                                                   AS issue_epic_closed_at,
      dim_issue.issue_closed_at::DATE                                             AS issue_epic_closed_date,
      DATE_TRUNC('month', dim_issue.issue_closed_at::DATE)                        AS issue_epic_closed_month,
      dim_issue.milestone_title                                                   AS milestone_title,
      dim_issue.milestone_due_date                                                AS milestone_due_date,
      dim_issue.labels                                                            AS issue_epic_labels,
      CASE
        WHEN ARRAY_CONTAINS('deliverable'::VARIANT, dim_issue.labels) THEN 'Yes'
        WHEN ARRAY_CONTAINS('stretch'::VARIANT, dim_issue.labels) THEN 'Stretch'
        ELSE 'No'
      END                                                                         AS deliverable,
      IFNULL(issue_group_extended_label.product_group_extended, 'Unknown')        AS product_group_extended,
      group_label.group_label                                                     AS product_group,
      category_label.category_label                                               AS product_category,
      devops_label.devops_label                                                   AS product_stage,
      CASE type_label.type_label
        WHEN 'bug' THEN 'bug fix'
        WHEN 'feature' THEN 'feature request'
      END                                                                         AS issue_epic_type,
      IFNULL(issue_status.workflow_label, 'Not Started')                          AS issue_status,
      issue_theme_labels.theme_labels                                             AS theme_labels,
      -1                                                                          AS epic_status,
      dim_epic.epic_url                                                           AS parent_epic_path,
      dim_epic.epic_title                                                         AS parent_epic_title,
      dim_issue.upvote_count                                                      AS upvote_count,
      IFNULL(dim_issue.weight, 0)                                                 AS issue_epic_weight

    FROM bdg_issue_user_request
    LEFT JOIN dim_issue
      ON dim_issue.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN issue_group_extended_label
      ON issue_group_extended_label.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN issue_status
      ON issue_status.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN dim_epic
      ON dim_epic.dim_epic_sk = dim_issue.dim_epic_sk
    LEFT JOIN issue_category_dedup AS category_label
      ON category_label.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN issue_group_label AS group_label
      ON group_label.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN issue_devops_label AS devops_label
      ON devops_label.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN issue_type_label AS type_label
      ON type_label.dim_issue_sk = bdg_issue_user_request.dim_issue_sk
    LEFT JOIN issue_theme_labels
      ON issue_theme_labels.dim_issue_sk = bdg_issue_user_request.dim_issue_sk

    UNION

    SELECT
      -1                                                                          AS issue_id,
      dim_epic.epic_id                                                            AS epic_id,
      'Epic'                                                                      AS user_request_in,
      
      bdg_epic_user_request.link_type                                             AS link_type,
      bdg_epic_user_request.dim_crm_opportunity_id                                AS dim_crm_opportunity_id,
      bdg_epic_user_request.dim_crm_account_id                                    AS dim_crm_account_id,
      bdg_epic_user_request.dim_ticket_id                                         AS dim_ticket_id,
      bdg_epic_user_request.request_priority                                      AS request_priority,
      bdg_epic_user_request.is_request_priority_empty                             AS is_request_priority_empty,
      bdg_epic_user_request.is_user_request_only_in_collaboration_project         AS is_user_request_only_in_collaboration_project,
      bdg_epic_user_request.link_last_updated_at                                  AS link_last_updated_at,
      bdg_epic_user_request.link_last_updated_at::DATE                            AS link_last_updated_date,
      DATE_TRUNC('month', bdg_epic_user_request.link_last_updated_at::DATE)       AS link_last_updated_month,

      IFF(link_type = 'Opportunity', 'https://gitlab.my.salesforce.com/' || bdg_epic_user_request.dim_crm_opportunity_id, 'No Link')
                                                                                  AS crm_opportunity_link,
      'https://gitlab.my.salesforce.com/' || bdg_epic_user_request.dim_crm_account_id
                                                                                  AS crm_account_link,
      IFF(link_type = 'Zendesk Ticket', 'https://gitlab.zendesk.com/agent/tickets/' || bdg_epic_user_request.dim_ticket_id, 'No Link')
                                                                                  AS ticket_link,

      -- Epic / Issue attributes
      dim_epic.epic_title                                                         AS epic_title,
      dim_epic.epic_url                                                           AS epic_url,
      dim_epic.created_at                                                         AS issue_epic_created_at,
      dim_epic.created_at::DATE                                                   AS issue_epic_created_date,
      DATE_TRUNC('month', dim_epic.created_at::DATE)                              AS issue_epic_created_month,
      dim_epic.epic_state                                                         AS issue_epic_state_name,
      dim_epic.closed_at                                                          AS issue_epic_closed_at,
      dim_epic.closed_at::DATE                                                    AS issue_epic_closed_date,
      DATE_TRUNC('month', dim_epic.closed_at::DATE)                               AS issue_epic_closed_month,
      epic_last_milestone.milestone_title                                         AS milestone_title,
      epic_last_milestone.milestone_due_date                                      AS milestone_due_date,
      dim_epic.labels                                                             AS issue_epic_labels,
      CASE
        WHEN ARRAY_CONTAINS('deliverable'::VARIANT, dim_epic.labels) THEN 'Yes'
        WHEN ARRAY_CONTAINS('stretch'::VARIANT, dim_epic.labels) THEN 'Stretch'
        ELSE 'No'
      END                                                                         AS deliverable,
      IFNULL(epic_group_extended_label.product_group_extended, 'Unknown')         AS product_group_extended,
      group_label.group_label                                                     AS product_group,
      category_label.category_label                                               AS product_category,
      devops_label.devops_label                                                   AS product_stage,
      CASE type_label.type_label
        WHEN 'bug' THEN 'bug fix'
        WHEN 'feature' THEN 'feature request'
      END                                                                         AS issue_epic_type,
      'Not Applicable'                                                            AS issue_status,
      epic_theme_labels.theme_labels                                              AS theme_labels,
      IFNULL(epic_weight.epic_status, 0)                                          AS epic_status,
      parent_epic.epic_url                                                        AS parent_epic_path,
      parent_epic.epic_title                                                      AS parent_epic_title,
      dim_epic.upvote_count                                                       AS upvote_count,
      IFNULL(epic_weight.epic_weight, 0)                                          AS issue_epic_weight

    FROM bdg_epic_user_request
    LEFT JOIN dim_issue
      ON dim_issue.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN dim_epic
      ON dim_epic.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_last_milestone
      ON epic_last_milestone.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_group_extended_label
      ON epic_group_extended_label.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_weight
      ON epic_weight.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN dim_epic AS parent_epic
      ON parent_epic.epic_id = dim_epic.parent_id
    LEFT JOIN epic_category_dedup AS category_label
      ON category_label.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_group_label AS group_label
      ON group_label.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_devops_label AS devops_label
      ON devops_label.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_type_label AS type_label
      ON type_label.dim_epic_sk = bdg_epic_user_request.dim_epic_sk
    LEFT JOIN epic_theme_labels
      ON epic_theme_labels.dim_epic_sk = bdg_epic_user_request.dim_epic_sk

), user_request_with_account_opp_attributes AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['user_request.issue_id',
                                  'user_request.epic_id',
                                  'user_request.dim_crm_account_id',
                                  'user_request.dim_crm_opportunity_id',
                                  'user_request.dim_ticket_id']
                                ) }}                                              AS primary_key,
      user_request.*,

      -- CRM Account attributes
      dim_crm_account.crm_account_name                                            AS crm_account_name,
      account_next_renewal_month.next_renewal_month                               AS crm_account_next_renewal_month,
      dim_crm_account.health_score_color                                          AS crm_account_health_score_color,
      dim_crm_account.parent_crm_account_sales_segment                            AS parent_crm_account_sales_segment,
      dim_crm_account.technical_account_manager                                   AS technical_account_manager,
      dim_crm_account.account_owner                                               AS strategic_account_leader,
      IFNULL(arr_metrics_current_month.quantity, 0)                               AS customer_reach,
      IFNULL(arr_metrics_current_month.arr, 0)                                    AS crm_account_arr,
      IFNULL(account_open_opp_net_arr.net_arr, 0)                                 AS crm_account_open_opp_net_arr,
      IFNULL(account_open_opp_net_arr_fo.net_arr, 0)                              AS crm_account_open_opp_net_arr_fo,
      IFNULL(account_open_opp_net_arr_growth.net_arr, 0)                          AS crm_account_open_opp_net_arr_growth,
      IFNULL(account_open_fo_opp_seats.seats, 0)                                  AS opportunity_reach,
      IFNULL(account_lost_opp_arr.net_arr, 0)                                     AS crm_account_lost_opp_net_arr,
      IFNULL(account_lost_customer_arr.arr_basis, 0)                              AS crm_account_lost_customer_arr,
      crm_account_lost_opp_net_arr + crm_account_lost_customer_arr                AS lost_arr,

      -- CRM Opportunity attributes
      dim_crm_opportunity.stage_name                                              AS crm_opp_stage_name,
      fct_crm_opportunity.is_closed                                               AS crm_opp_is_closed,
      fct_crm_opportunity.close_date                                              AS crm_opp_close_date,
      dim_order_type.order_type_name                                              AS crm_opp_order_type,
      dim_order_type.order_type_grouped                                           AS crm_opp_order_type_grouped,
      primary_solution_architect.dim_crm_user_id                                  AS primary_solution_architect_id,
      primary_solution_architect.user_name                                        AS primary_solution_architect_user_name,
      IFF(DATE_TRUNC('month', dim_crm_opportunity.subscription_end_date) >= DATE_TRUNC('month',CURRENT_DATE),
        DATE_TRUNC('month', dim_crm_opportunity.subscription_end_date),
        NULL
      )                                                                           AS crm_opp_next_renewal_month,
      fct_crm_opportunity.net_arr                                                 AS crm_opp_net_arr,
      fct_crm_opportunity.arr_basis                                               AS crm_opp_arr_basis,
      opportunity_seats.quantity                                                  AS crm_opp_seats,
      fct_crm_opportunity.probability                                             AS crm_opp_probability

    FROM user_request

    -- Account Joins
    LEFT JOIN arr_metrics_current_month
      ON arr_metrics_current_month.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_next_renewal_month
      ON account_next_renewal_month.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_open_fo_opp_seats
      ON account_open_fo_opp_seats.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_lost_opp_arr
      ON account_lost_opp_arr.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_lost_customer_arr
      ON account_lost_customer_arr.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_open_opp_net_arr
      ON account_open_opp_net_arr.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_open_opp_net_arr_fo
      ON account_open_opp_net_arr_fo.dim_crm_account_id = user_request.dim_crm_account_id
    LEFT JOIN account_open_opp_net_arr_growth
      ON account_open_opp_net_arr_growth.dim_crm_account_id = user_request.dim_crm_account_id

    -- Opportunity Joins
    LEFT JOIN fct_crm_opportunity
      ON fct_crm_opportunity.dim_crm_opportunity_id = user_request.dim_crm_opportunity_id
    LEFT JOIN dim_order_type
      ON dim_order_type.dim_order_type_id = fct_crm_opportunity.dim_order_type_id
    LEFT JOIN dim_crm_opportunity
      ON dim_crm_opportunity.dim_crm_opportunity_id = user_request.dim_crm_opportunity_id
    LEFT JOIN opportunity_seats
      ON opportunity_seats.dim_crm_opportunity_id = user_request.dim_crm_opportunity_id
    LEFT JOIN dim_crm_user AS primary_solution_architect
      ON primary_solution_architect.dim_crm_user_id = dim_crm_opportunity.primary_solution_architect


), customer_value_scores AS (

    SELECT
      primary_key,
      CASE
        WHEN crm_account_health_score_color = 'Green'
          THEN 1
        WHEN crm_account_health_score_color = 'Yellow'
          THEN
          CASE
            WHEN DATEDIFF('months', CURRENT_DATE, crm_account_next_renewal_month) > 18
              THEN 1.5
            WHEN DATEDIFF('months', CURRENT_DATE, crm_account_next_renewal_month) > 12
              THEN 2
            WHEN DATEDIFF('months', CURRENT_DATE, crm_account_next_renewal_month) <= 12
              THEN 2.5
          END
        WHEN crm_account_health_score_color = 'Red'
          THEN
            CASE
            WHEN DATEDIFF('months', CURRENT_DATE, crm_account_next_renewal_month) > 18
              THEN 2
            WHEN DATEDIFF('months', CURRENT_DATE, crm_account_next_renewal_month) > 12
              THEN 3
            WHEN DATEDIFF('months', CURRENT_DATE, crm_account_next_renewal_month) <= 12
              THEN 4
          END
        ELSE 1
      END                                                                                                     AS retention_urgency_score,
      CASE
        WHEN crm_opp_probability > 60
          THEN 1
        WHEN crm_opp_probability > 39
          THEN
            CASE
              WHEN DATEDIFF('months', CURRENT_DATE, crm_opp_close_date) > 6
                THEN 1.25
              WHEN DATEDIFF('months', CURRENT_DATE, crm_opp_close_date) > 3
                THEN 1.5
              WHEN DATEDIFF('months', CURRENT_DATE, crm_opp_close_date) <= 3
                THEN 2
            END
        WHEN crm_opp_probability < 40
          THEN 
          CASE
            WHEN DATEDIFF('months', CURRENT_DATE, crm_opp_close_date) > 6
              THEN 1.5
            WHEN DATEDIFF('months', CURRENT_DATE, crm_opp_close_date) > 3
              THEN 2
            WHEN DATEDIFF('months', CURRENT_DATE, crm_opp_close_date) <= 3
              THEN 2.5
          END
        ELSE 1
      END                                                                                                     AS opportunity_urgency_score,
      IFF(link_type = 'Opportunity', crm_opp_arr_basis, crm_account_arr)                                      AS arr_to_use,
      ZEROIFNULL(crm_opp_net_arr / NULLIF(ZEROIFNULL(crm_opp_net_arr) + ZEROIFNULL(arr_to_use), 0))           AS growth_percentage,
      ZEROIFNULL(arr_to_use / NULLIF(ZEROIFNULL(crm_opp_net_arr) + ZEROIFNULL(arr_to_use), 0))                AS retention_percentage,
      request_priority * growth_percentage                                                                    AS growth_priority,
      request_priority * retention_percentage                                                                 AS retention_priority,
      -- for that account's links in that opportunity - use multiple partitions
      ZEROIFNULL(growth_priority / NULLIF(SUM(growth_priority) OVER(PARTITION BY dim_crm_account_id, dim_crm_opportunity_id), 0))
                                                                                                              AS growth_priority_weighting,
      ZEROIFNULL(retention_priority / NULLIF(SUM(retention_priority) OVER(PARTITION BY dim_crm_account_id), 0))
                                                                                                              AS retention_priority_weighting,
      -- a utility column to allow sum of all epics for customer reach
      customer_reach / NULLIF(COUNT(*) OVER(PARTITION BY epic_id, dim_crm_account_id), 0)                 AS customer_epic_reach,
      CASE
        WHEN link_type = 'Opportunity'
          THEN crm_opp_net_arr * growth_priority_weighting
        ELSE 0
      END                                                                                                     AS growth_score,
      retention_priority_weighting * crm_account_arr                                                          AS retention_score,
      growth_score + retention_score                                                                          AS combined_score,
      combined_score * CASE
        WHEN link_type = 'Opportunity'
          THEN opportunity_urgency_score
        ELSE retention_urgency_score
      END                                                                                                     AS priority_score
    FROM user_request_with_account_opp_attributes
    WHERE issue_epic_state_name = 'opened'
      AND (
        CASE
          WHEN link_type = 'Opportunity'
            THEN crm_opp_is_closed = FALSE
          ELSE TRUE
        END
      )

), final AS (

    SELECT
      user_request_with_account_opp_attributes.*,
      CASE
        WHEN user_request_with_account_opp_attributes.is_request_priority_empty
          THEN '[Input (Using 1 as Default)](' || user_request_with_account_opp_attributes.issue_epic_url || ')'
        ELSE request_priority::TEXT
      END                                                                            AS priority_input_url,
      CASE
        WHEN user_request_with_account_opp_attributes.link_type = 'Zendesk Ticket'
          THEN '[' || user_request_with_account_opp_attributes.link_type || '](' || user_request_with_account_opp_attributes.ticket_link || ')'
        WHEN user_request_with_account_opp_attributes.link_type = 'Opportunity'
          THEN '[' || user_request_with_account_opp_attributes.link_type || '](' || user_request_with_account_opp_attributes.crm_opportunity_link || ')'
        WHEN user_request_with_account_opp_attributes.link_type = 'Account'
          THEN '[' || user_request_with_account_opp_attributes.link_type || '](' || user_request_with_account_opp_attributes.crm_account_link || ')'
      END                                                                            AS user_request_link,
      customer_value_scores.retention_percentage                                     AS link_retention_percentage,
      customer_value_scores.growth_percentage                                        AS link_growth_percentage,
      customer_value_scores.retention_priority                                       AS link_retention_priority,
      customer_value_scores.growth_priority                                          AS link_growth_priority,
      customer_value_scores.retention_priority_weighting                             AS link_retention_priority_weighting,
      customer_value_scores.growth_priority_weighting                                AS link_growth_priority_weighting,
      customer_value_scores.retention_score                                          AS link_retention_score,
      customer_value_scores.growth_score                                             AS link_growth_score,
      customer_value_scores.combined_score                                           AS link_combined_score,
      customer_value_scores.priority_score                                           AS link_priority_score,
      link_priority_score / NULLIFZERO(issue_epic_weight)                            AS link_weighted_priority_score,
      IFF(link_weighted_priority_score IS NULL,
        '[Effort is Empty, Input Effort Here](' || user_request_with_account_opp_attributes.issue_epic_url || ')',
        link_weighted_priority_score::TEXT)                                          AS link_weighted_priority_score_input
    FROM user_request_with_account_opp_attributes
    LEFT JOIN customer_value_scores
      ON user_request_with_account_opp_attributes.primary_key = customer_value_scores.primary_key

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-10-22",
    updated_date="2023-09-29",
  ) }}
