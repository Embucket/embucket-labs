{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

{{ simple_cte([
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily_with_namespace_and_installation'),
    ('map_namespace_subscription_product', 'map_namespace_subscription_product'),
    ('map_installation_subscription_product','map_installation_subscription_product'),
    ('prep_product_detail', 'prep_product_detail'),
    ('prep_seat_link_installation_daily', 'prep_seat_link_installation_daily'),
    ('fct_ping_instance', 'fct_ping_instance'),
    ('gitlab_dotcom_subscription_add_on_purchases_snapshots_base', 'gitlab_dotcom_subscription_add_on_purchases_snapshots_base'),
    ('gitlab_dotcom_subscription_user_add_on_assignment_versions', 'gitlab_dotcom_subscription_user_add_on_assignment_versions_source'),
    ('gitlab_dotcom_subscription_user_add_on_assignments', 'gitlab_dotcom_subscription_user_add_on_assignments_source'),
    ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
    ('dim_date', 'dim_date'),
    ('prep_subscription', 'prep_subscription'),
    ('customers_db_trials', 'customers_db_trials'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_namespace_order_trial', 'prep_namespace_order_trial'),
    ('prep_trial', 'prep_trial')

])}},

zuora_base AS (

-- Only licence users are avaiable from zuora

  SELECT
    prep_charge_mrr_daily.date_actual::DATE                                                                                                            AS reporting_date,

    prep_charge_mrr_daily.dim_namespace_id::INT                                                                                                        AS dim_namespace_id,
    IFF(prep_namespace.namespace_is_internal = TRUE, TRUE, FALSE)::BOOLEAN                                                                             AS namespace_is_internal,
    prep_charge_mrr_daily.dim_installation_id::VARCHAR                                                                                                 AS dim_installation_id,

    SUM(IFF(SPLIT_PART(prep_product_detail.product_rate_plan_category, ' - ', 2) = 'GitLab Duo Pro', prep_charge_mrr_daily.quantity, NULL))            AS duo_pro_license_users_zuora,
    SUM(IFF(SPLIT_PART(prep_product_detail.product_rate_plan_category, ' - ', 2) = 'GitLab Duo Enterprise', prep_charge_mrr_daily.quantity, NULL))     AS duo_enterprise_license_users_zuora,
    SUM(IFF(SPLIT_PART(prep_product_detail.product_rate_plan_category, ' - ', 2) = 'Enterprise Agile Planning', prep_charge_mrr_daily.quantity, NULL)) AS enterprise_agile_planning_license_users_zuora

  FROM prep_charge_mrr_daily
  LEFT JOIN prep_namespace
    ON prep_charge_mrr_daily.dim_namespace_id = prep_namespace.dim_namespace_id
  INNER JOIN prep_product_detail
    ON prep_charge_mrr_daily.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  WHERE prep_product_detail.is_licensed_user = TRUE
    AND prep_product_detail.product_category = 'Add On Services'
    AND SPLIT_PART(prep_product_detail.product_rate_plan_category, ' - ', 2) IN ('GitLab Duo Pro', 'GitLab Duo Enterprise', 'Enterprise Agile Planning')
    AND reporting_date BETWEEN '2024-02-01' AND CURRENT_DATE -- first duo pro arr
    AND (prep_charge_mrr_daily.dim_namespace_id IS NOT NULL OR prep_charge_mrr_daily.dim_installation_id IS NOT NULL) -- Exclude records where either of the dim_namespace_id or dim_installation_id are null
  GROUP BY ALL

),

seat_link_base AS (

  SELECT
    report_date,
    dim_installation_id,
    MAX(IFF(add_on_type = 'GitLab Duo Pro', addon_purchased_seats, NULL))        AS duo_pro_license_users_seat_link,
    MAX(IFF(add_on_type = 'GitLab Duo Pro', addon_assigned_seats, NULL))         AS duo_pro_billable_users_seat_link,
    MAX(IFF(add_on_type = 'GitLab Duo Enterprise', addon_purchased_seats, NULL)) AS duo_enterprise_license_users_seat_link,
    MAX(IFF(add_on_type = 'GitLab Duo Enterprise', addon_assigned_seats, NULL))  AS duo_enterprise_billable_users_seat_link
  FROM prep_seat_link_installation_daily
  WHERE dim_installation_id IS NOT NULL
  GROUP BY report_date, dim_installation_id

),

service_ping_base AS (

  SELECT
    ping_created_at::DATE                    AS reporting_date,
    dim_installation_id,
    duo_pro_purchased_seats                  AS duo_pro_license_users_service_ping,
    duo_pro_assigned_seats                   AS duo_pro_billable_users_service_ping,
    duo_enterprise_purchased_seats           AS duo_enterprise_license_users_service_ping,
    duo_enterprise_assigned_seats            AS duo_enterprise_billable_users_service_ping
  FROM fct_ping_instance
  WHERE dim_installation_id IS NOT NULL
    AND (
      duo_pro_purchased_seats IS NOT NULL
      OR duo_pro_assigned_seats IS NOT NULL
      OR duo_enterprise_purchased_seats IS NOT NULL
      OR duo_enterprise_assigned_seats IS NOT NULL
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      ping_created_at::DATE,
      dim_installation_id
    ORDER BY ping_created_at DESC
  ) = 1

),

dotcom_add_on_purchases AS (

  SELECT 
    purchases_snapshot.id,
    purchases_snapshot.namespace_id,
    IFF(prep_namespace.namespace_is_internal = TRUE, TRUE, FALSE) AS namespace_is_internal,
    purchases_snapshot.subscription_add_on_id,
    purchases_snapshot.quantity,
    purchases_snapshot.corrected_purchase_date,
    purchases_snapshot.corrected_expires_on_date
  FROM gitlab_dotcom_subscription_add_on_purchases_snapshots_base AS purchases_snapshot
  LEFT JOIN prep_namespace
    ON purchases_snapshot.namespace_id = prep_namespace.dim_namespace_id
  -- get the most recent record for each namespace/subscription combination.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      purchases_snapshot.namespace_id,
      purchases_snapshot.subscription_add_on_id
    ORDER BY purchases_snapshot.valid_from DESC
  ) = 1

),

dotcom_add_on_deleted_assignments AS (

  SELECT
    item_id,
    user_id,
    created_at AS deleted_at
  FROM gitlab_dotcom_subscription_user_add_on_assignment_versions
  WHERE event = 'destroy'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
      item_id
    ORDER BY uploaded_at DESC
  ) = 1
),

dotcom_add_on_assignments AS (

  SELECT
    DISTINCT -- Unique assignments per namespace_id
    memebership.namespace_id,
    assignments.add_on_purchase_id,
    assignments.user_id,
    assignments.created_at,
    CASE
        WHEN deleted_assignments.deleted_at IS NOT NULL THEN deleted_assignments.deleted_at::DATE
        WHEN assignments.pgp_is_deleted = TRUE THEN NULL
        ELSE CURRENT_DATE()
    END as effective_end_date
  FROM gitlab_dotcom_subscription_user_add_on_assignments AS assignments
  INNER JOIN dotcom_add_on_purchases AS purchases
    ON assignments.add_on_purchase_id = purchases.id
  INNER JOIN gitlab_dotcom_memberships AS memebership
    ON memebership.user_id = assignments.user_id
    AND memebership.namespace_id = purchases.namespace_id
  -- Track deleted assignments, we can only track the exact deleted_at date for the records after 2024-11-08, before that date the deleted assignments are simply exluded from the calculation
  -- Related issue: https://gitlab.com/gitlab-org/gitlab/-/issues/508335
  LEFT JOIN dotcom_add_on_deleted_assignments AS deleted_assignments
    ON assignments.id = deleted_assignments.item_id
    AND assignments.user_id = deleted_assignments.user_id
),

dotcom_add_on_daily_purchases_assignments AS ( -- CTE to calculate daily assigned seats per namespace ID
                                               -- Seat count updates daily to reflect new assignments and unassignments
  SELECT
    -- Use COALESCE to handle cases where assignments might be NULL
    COALESCE(dotcom_add_on_assignments.namespace_id, purchases_snapshot.namespace_id)   AS namespace_id,
    purchases_snapshot.namespace_is_internal,
    dim_date.date_day,
    purchases_snapshot.subscription_add_on_id,
    -- Use MAX as a selector to pick the single quantity value for each unique combination of namespace, date, and subscription add-on
    MAX(purchases_snapshot.quantity)                                                    AS purchased_seats,
    -- If effective_end_date IS NULL (PaperTrial has no deletion record but the pgp_is_deleted is TRUE), it doesn't count the assignments but still include those purchases
    -- For namespaces that have no assignments, the assigned seats should be 0 rather than NULL.
    -- For namespaces with assignments: Include dates within the assignment period
    COUNT(DISTINCT IFF(dotcom_add_on_assignments.effective_end_date IS NOT NULL
                        -- Only count from when the assignment was created till the end date
                        AND dim_date.date_actual >= DATE(dotcom_add_on_assignments.created_at)
                        AND dim_date.date_actual <= dotcom_add_on_assignments.effective_end_date,
                        dotcom_add_on_assignments.user_id,
                        NULL))                                                          AS assigned_seats
  FROM dotcom_add_on_purchases AS purchases_snapshot
  -- LEFT OUTER JOIN to include namespaces that have no assignments
  LEFT OUTER JOIN dotcom_add_on_assignments
    ON dotcom_add_on_assignments.namespace_id = purchases_snapshot.namespace_id
    AND dotcom_add_on_assignments.add_on_purchase_id = purchases_snapshot.id
  CROSS JOIN dim_date
  WHERE 
    -- Main date range: from purchase date to expiration date (or current date if no expiration)
    dim_date.date_actual BETWEEN DATE(purchases_snapshot.corrected_purchase_date) 
                             AND COALESCE(purchases_snapshot.corrected_expires_on_date, CURRENT_DATE())
  GROUP BY ALL

),

dotcom_assignment_base AS (

  SELECT
    -- Compared to the FulfillmentProvisionGitLab_comDuoProSeatAdoption Dashboard this logic contains upgrades/downgrades from Duo Pro and has all the inactive susbcriptions (records which have an expires_on > CURRENT_DATE())
    date_day::DATE                                                    AS reporting_date,
    namespace_id::INT                                                 AS dim_namespace_id,
    namespace_is_internal::BOOLEAN                                    AS namespace_is_internal,
    -- MAX is used here to select the non-NULL value for each product type (Duo Pro or Duo Enterprise) within each namespace and date group.
    MAX(IFF(subscription_add_on_id = 1000002, purchased_seats, NULL)) AS duo_pro_license_users_assignment_table,
    MAX(IFF(subscription_add_on_id = 1000035, purchased_seats, NULL)) AS duo_enterprise_license_users_assignment_table,
    MAX(IFF(subscription_add_on_id = 1000002, assigned_seats, NULL))  AS duo_pro_billable_users_assignment_table,
    MAX(IFF(subscription_add_on_id = 1000035, assigned_seats, NULL))  AS duo_enterprise_billable_users_assignment_table
  FROM dotcom_add_on_daily_purchases_assignments
  -- Exclude assignment records where subscription_add_on_id is null because either the associated purchase got filtered in the dotcom_add_on_purchases CTE due to the purchase_xid criteria but dotcom_add_on_assignments has data because the ID for the other purchase was same 
  -- OR the subscription was upgraded, but no new assignment record was created after the upgrade date. This prevents attributing old assignments to a new subscription type.
  WHERE subscription_add_on_id IS NOT NULL
  GROUP BY ALL

),

dotcom_duo_trials AS (

  SELECT DISTINCT
    prep_namespace.dim_namespace_id,
    CASE
      WHEN prep_namespace_order_trial.trial_type = 2
        THEN 'GitLab Duo Pro'
      WHEN prep_namespace_order_trial.trial_type IN (3,5,6)
        THEN 'GitLab Duo Enterprise'
    END                                                               AS product_category,
    prep_namespace_order_trial.order_start_date                       AS trial_start_date,
    MAX(prep_trial.trial_end_date)                                    AS trial_end_date
  FROM prep_namespace_order_trial
  LEFT JOIN prep_trial
    ON prep_namespace_order_trial.dim_namespace_id = prep_trial.dim_namespace_id
    AND prep_namespace_order_trial.order_start_date = prep_trial.trial_start_date
    AND prep_trial.product_rate_plan_id LIKE '%duo%'
  INNER JOIN prep_namespace
    ON prep_namespace_order_trial.dim_namespace_id = prep_namespace.dim_namespace_id
  WHERE prep_namespace_order_trial.trial_type IN (2,3,5,6)
  AND prep_namespace.namespace_creator_is_blocked = FALSE 
  AND prep_namespace.namespace_is_ultimate_parent = TRUE
  GROUP BY ALL

),

sm_dedicated_duo_trials AS (

  SELECT DISTINCT
    map_installation_subscription_product.dim_installation_id::VARCHAR                            AS dim_installation_id,
    TRIM(SPLIT_PART(SPLIT_PART(prep_product_detail.product_rate_plan_category, '(', 1), '- ', 2)) AS product_category,
    customers_db_trials.start_date::DATE                                                          AS trial_start_date,
    customers_db_trials.end_date::DATE                                                            AS trial_end_date,    
  FROM customers_db_trials
  LEFT JOIN prep_product_detail
    ON customers_db_trials.product_rate_plan_id = prep_product_detail.product_rate_plan_id
  LEFT JOIN prep_subscription
    ON customers_db_trials.subscription_name = prep_subscription.subscription_name
     AND prep_subscription.subscription_version = 1
  INNER JOIN map_installation_subscription_product
    ON prep_subscription.dim_subscription_id_original = map_installation_subscription_product.dim_subscription_id_original
      AND map_installation_subscription_product.date_actual BETWEEN customers_db_trials.start_date AND customers_db_trials.end_date
  WHERE prep_product_detail.product_rate_plan_name ILIKE '%duo%'

),

joined AS (

  SELECT
    COALESCE(
      zuora_base.reporting_date,
      dotcom_assignment_base.reporting_date,
      seat_link_base.report_date,
      service_ping_base.reporting_date
    )::DATE                                                                    AS reporting_date,

    COALESCE(
      zuora_base.dim_installation_id,
      seat_link_base.dim_installation_id,
      service_ping_base.dim_installation_id
    )::VARCHAR                                                                 AS dim_installation_id,

    COALESCE(
      zuora_base.dim_namespace_id,
      dotcom_assignment_base.dim_namespace_id
    )::INT                                                                     AS dim_namespace_id,

    COALESCE(
      zuora_base.namespace_is_internal,
      dotcom_assignment_base.namespace_is_internal
    )::BOOLEAN                                                                 AS namespace_is_internal,

    -- Duo Pro License Users
    COALESCE(
      zuora_base.duo_pro_license_users_zuora,
      dotcom_assignment_base.duo_pro_license_users_assignment_table,
      service_ping_base.duo_pro_license_users_service_ping,
      seat_link_base.duo_pro_license_users_seat_link
    )::INT                                                                     AS duo_pro_license_users_coalesced,

    zuora_base.duo_pro_license_users_zuora::INT                                AS duo_pro_license_users_zuora,
    dotcom_assignment_base.duo_pro_license_users_assignment_table::INT         AS duo_pro_license_users_assignment_table,
    seat_link_base.duo_pro_license_users_seat_link::INT                        AS duo_pro_license_users_seat_link,
    service_ping_base.duo_pro_license_users_service_ping::INT                  AS duo_pro_license_users_service_ping,

    -- Duo Enterprise License Users
    COALESCE(
      zuora_base.duo_enterprise_license_users_zuora,
      dotcom_assignment_base.duo_enterprise_license_users_assignment_table,
      service_ping_base.duo_enterprise_license_users_service_ping,
      seat_link_base.duo_enterprise_license_users_seat_link
    )::INT                                                                     AS duo_enterprise_license_users_coalesced,

    dotcom_assignment_base.duo_enterprise_license_users_assignment_table::INT  AS duo_enterprise_license_users_assignment_table,
    zuora_base.duo_enterprise_license_users_zuora::INT                         AS duo_enterprise_license_users_zuora,
    seat_link_base.duo_enterprise_license_users_seat_link::INT                 AS duo_enterprise_license_users_seat_link,
    service_ping_base.duo_enterprise_license_users_service_ping::INT           AS duo_enterprise_license_users_service_ping,

    -- Enterprise Agile Planning License Users
    zuora_base.enterprise_agile_planning_license_users_zuora::INT              AS enterprise_agile_planning_license_users_zuora,

    -- Duo Pro Billable Users
    COALESCE(
      dotcom_assignment_base.duo_pro_billable_users_assignment_table,
      service_ping_base.duo_pro_billable_users_service_ping,
      seat_link_base.duo_pro_billable_users_seat_link
    )::INT                                                                     AS duo_pro_billable_users_coalesced,

    dotcom_assignment_base.duo_pro_billable_users_assignment_table::INT        AS duo_pro_billable_users_assignment_table,
    seat_link_base.duo_pro_billable_users_seat_link::INT                       AS duo_pro_billable_users_seat_link,
    service_ping_base.duo_pro_billable_users_service_ping::INT                 AS duo_pro_billable_users_service_ping,

    -- Duo Enterprise Billable Users

    COALESCE(
      dotcom_assignment_base.duo_enterprise_billable_users_assignment_table,
      service_ping_base.duo_enterprise_billable_users_service_ping,
      seat_link_base.duo_enterprise_billable_users_seat_link
    )::INT                                                                     AS duo_enterprise_billable_users_coalesced,

    dotcom_assignment_base.duo_enterprise_billable_users_assignment_table::INT AS duo_enterprise_billable_users_assignment_table,
    seat_link_base.duo_enterprise_billable_users_seat_link::INT                AS duo_enterprise_billable_users_seat_link,
    service_ping_base.duo_enterprise_billable_users_service_ping::INT          AS duo_enterprise_billable_users_service_ping

  FROM zuora_base
  FULL OUTER JOIN seat_link_base
    ON zuora_base.reporting_date = seat_link_base.report_date
      AND zuora_base.dim_installation_id = seat_link_base.dim_installation_id
  FULL OUTER JOIN service_ping_base
    ON COALESCE(zuora_base.reporting_date, seat_link_base.report_date) = service_ping_base.reporting_date
      AND COALESCE(zuora_base.dim_installation_id, seat_link_base.dim_installation_id) = service_ping_base.dim_installation_id
  FULL OUTER JOIN dotcom_assignment_base
    ON zuora_base.reporting_date = dotcom_assignment_base.reporting_date
      AND zuora_base.dim_namespace_id = dotcom_assignment_base.dim_namespace_id

)

SELECT 
  joined.reporting_date,
  joined.dim_installation_id,
  joined.dim_namespace_id,

  joined.namespace_is_internal,

  CASE
    WHEN EXISTS (
      SELECT 1
      FROM sm_dedicated_duo_trials
      WHERE sm_dedicated_duo_trials.dim_installation_id = joined.dim_installation_id
        AND joined.reporting_date BETWEEN sm_dedicated_duo_trials.trial_start_date AND sm_dedicated_duo_trials.trial_end_date
        AND joined.duo_pro_license_users_coalesced > 0 
        AND sm_dedicated_duo_trials.product_category = 'GitLab Duo Pro'
    ) OR EXISTS (
      SELECT 1
      FROM dotcom_duo_trials
      WHERE dotcom_duo_trials.dim_namespace_id = joined.dim_namespace_id
        AND joined.reporting_date BETWEEN dotcom_duo_trials.trial_start_date AND dotcom_duo_trials.trial_end_date
        AND joined.duo_pro_license_users_coalesced > 0 
        AND dotcom_duo_trials.product_category = 'GitLab Duo Pro'
    ) THEN TRUE
    ELSE FALSE
  END AS is_duo_pro_trial,

  CASE
    WHEN EXISTS (
      SELECT 1
      FROM sm_dedicated_duo_trials
      WHERE sm_dedicated_duo_trials.dim_installation_id = joined.dim_installation_id
        AND joined.reporting_date BETWEEN sm_dedicated_duo_trials.trial_start_date AND sm_dedicated_duo_trials.trial_end_date
        AND joined.duo_enterprise_license_users_coalesced > 0 
        AND sm_dedicated_duo_trials.product_category = 'GitLab Duo Enterprise'
    ) OR EXISTS (
      SELECT 1
      FROM dotcom_duo_trials
      WHERE dotcom_duo_trials.dim_namespace_id = joined.dim_namespace_id
        AND joined.reporting_date BETWEEN dotcom_duo_trials.trial_start_date AND dotcom_duo_trials.trial_end_date
        AND joined.duo_enterprise_license_users_coalesced > 0 
        AND dotcom_duo_trials.product_category = 'GitLab Duo Enterprise'
    ) THEN TRUE
    ELSE FALSE
  END AS is_duo_enterprise_trial,

  joined.duo_pro_license_users_coalesced,
  joined.duo_pro_license_users_zuora,
  joined.duo_pro_license_users_assignment_table,
  joined.duo_pro_license_users_seat_link,
  joined.duo_pro_license_users_service_ping,

  joined.duo_enterprise_license_users_coalesced,
  joined.duo_enterprise_license_users_assignment_table,
  joined.duo_enterprise_license_users_zuora,
  joined.duo_enterprise_license_users_seat_link,
  joined.duo_enterprise_license_users_service_ping,

  joined.enterprise_agile_planning_license_users_zuora,

  joined.duo_pro_billable_users_coalesced,
  joined.duo_pro_billable_users_assignment_table,
  joined.duo_pro_billable_users_seat_link,
  joined.duo_pro_billable_users_service_ping,

  joined.duo_enterprise_billable_users_coalesced,
  joined.duo_enterprise_billable_users_assignment_table,
  joined.duo_enterprise_billable_users_seat_link,
  joined.duo_enterprise_billable_users_service_ping

FROM joined
