{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

{{ simple_cte([
    ('prep_crm_account', 'prep_crm_account'),
    ('dim_crm_user', 'dim_crm_user'),
    ('ent_sfdc_geo_pubsec_segment', 'ent_sfdc_geo_pubsec_segment')
]) }},

manager_chain AS (
    -- Get all management relationships including direct ownership
    SELECT 
        prep_crm_account.dim_crm_account_id,
        -- Direct owners
        user.user_email AS direct_owner_email,
        -- Manager chain
        manager1.user_email AS manager_l1_email,
        manager2.user_email AS manager_l2_email,
        manager3.user_email AS manager_l3_email,
        prep_crm_account.parent_crm_account_geo_pubsec_segment
    FROM prep_crm_account
    -- Join for direct owners (AE, BDR, TAM)
    LEFT JOIN dim_crm_user AS user
        ON user.dim_crm_user_id IN (
            prep_crm_account.crm_account_owner_id, 
            prep_crm_account.crm_sales_dev_rep_id, 
            prep_crm_account.technical_account_manager_id
        )
        AND user.is_active = TRUE
    -- Management chain
    LEFT JOIN dim_crm_user AS manager1 
        ON user.manager_id = manager1.dim_crm_user_id 
        AND manager1.is_active = TRUE
    LEFT JOIN dim_crm_user AS manager2 
        ON manager1.manager_id = manager2.dim_crm_user_id 
        AND manager2.is_active = TRUE
    LEFT JOIN dim_crm_user AS manager3 
        ON manager2.manager_id = manager3.dim_crm_user_id 
        AND manager3.is_active = TRUE
    WHERE prep_crm_account.is_deleted = FALSE
),

all_owners AS (
    -- Get all ownership relationships
    SELECT DISTINCT
        dim_crm_account_id,
        parent_crm_account_geo_pubsec_segment,
        email,
        'account_ownership' AS entitlement_basis
    FROM manager_chain
    UNPIVOT(
        email FOR owner_type IN (
            direct_owner_email,
            manager_l1_email,
            manager_l2_email,
            manager_l3_email
        )
    )
    WHERE email IS NOT NULL
),

owner_list AS (
    -- Get list of all owners to exclude from geo access
    SELECT DISTINCT 
        email AS user_email
    FROM all_owners
),

geo_access AS (
    -- Get geo access ONLY for users who are never owners
    SELECT DISTINCT
        prep_crm_account.dim_crm_account_id,
        ent_sfdc_geo_pubsec_segment.user_email,
        ent_sfdc_geo_pubsec_segment.geo_pubsec_segment,
        CONCAT('geo_', ent_sfdc_geo_pubsec_segment.entitlement_basis) AS entitlement_basis
    FROM prep_crm_account
    JOIN ent_sfdc_geo_pubsec_segment
        ON ent_sfdc_geo_pubsec_segment.geo_pubsec_segment = prep_crm_account.parent_crm_account_geo_pubsec_segment
    LEFT JOIN owner_list
        ON owner_list.user_email = ent_sfdc_geo_pubsec_segment.user_email
    WHERE owner_list.user_email IS NULL  -- Only include users who are never owners
        AND prep_crm_account.is_deleted = FALSE
),

final AS (
    -- Combine ownership and geo access
    SELECT 
        dim_crm_account_id,
        email AS user_email,
        parent_crm_account_geo_pubsec_segment AS geo_pubsec_segment,
        entitlement_basis
    FROM all_owners

    UNION ALL

    SELECT 
        dim_crm_account_id,
        user_email,
        geo_pubsec_segment,
        entitlement_basis
    FROM geo_access
)

SELECT *
FROM final